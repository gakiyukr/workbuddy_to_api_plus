package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentConfigFallbacks(t *testing.T) {
	isolatedGatewayConfig(t)
	for _, raw := range []string{
		`{}`, `{"concurrent":null}`, `{"concurrent":{}}`, `{"concurrent":""}`,
		`{"concurrent":false}`, `{"concurrent":[]}`,
		`{"concurrent":{"MaxAcc":null,"MaxGlobal":null}}`,
		`{"concurrent":{"MaxAcc":"","MaxGlobal":""}}`,
		`{"concurrent":{"MaxAcc":0,"MaxGlobal":0}}`,
		`{"concurrent":{"MaxAcc":-1,"MaxGlobal":-1}}`,
		`{"concurrent":{"MaxAcc":true,"MaxGlobal":[]}}`,
		`{"concurrent":{"MaxAcc":1.5,"MaxGlobal":2.5}}`,
		`{"concurrent":{"MaxAcc":9999999999999999999999999,"MaxGlobal":"invalid"}}`,
	} {
		c, err := parseRuntimeConfig([]byte(raw))
		if err != nil {
			t.Fatalf("fallback rejected %s: %v", raw, err)
		}
		if got := normalizeConcurrentConfig(c.Concurrent); got != (concurrentFileConfig{defaultMaxAcc, 0}) {
			t.Fatalf("%s => %#v", raw, got)
		}
	}
	for raw, want := range map[string]concurrentFileConfig{
		`{"concurrent":{"MaxAcc":2,"MaxGlobal":3}}`:  {2, 3},
		`{"concurrent":{"MaxAcc":7}}`:                {7, 0},
		`{"concurrent":{"MaxGlobal":2}}`:             {defaultMaxAcc, 2},
		`{"concurrent":{"MaxAcc":"","MaxGlobal":5}}`: {defaultMaxAcc, 5},
		`{"concurrent":{"maxacc":8,"maxglobal":0}}`:  {8, 0},
	} {
		c, err := parseRuntimeConfig([]byte(raw))
		if err != nil || normalizeConcurrentConfig(c.Concurrent) != want {
			t.Fatalf("%s => %#v, %v", raw, c.Concurrent, err)
		}
	}
	if _, err := parseRuntimeConfig([]byte(`{"concurrent":{"MaxAc":2}}`)); err == nil {
		t.Fatal("typo must not silently disable an intended limit")
	}
}

func TestConcurrentConfigCreationMigrationAndIdempotence(t *testing.T) {
	for _, initial := range []string{
		"", `{}`, `{"gateway":{"apiPort":18317},"identity":{"target":"Keep"}}`,
		`{"identity":{"target":"Keep"},"concurrent":{"MaxAcc":2}}`,
		`{"concurrent":{"MaxGlobal":3}}`, `{"concurrent":{"MaxAcc":6,"MaxGlobal":2}}`,
		`{"Concurrent":{"maxacc":7}}`, `{"concurrent":null}`,
	} {
		t.Run(initial, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if initial != "" {
				if err := os.WriteFile(path, []byte(initial), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := ensureRuntimeConfigFile(path); err != nil {
				t.Fatal(err)
			}
			raw, _ := os.ReadFile(path)
			var c runtimeFileConfig
			if err := json.Unmarshal(raw, &c); err != nil {
				t.Fatal(err)
			}
			var old runtimeFileConfig
			if initial != "" {
				_ = json.Unmarshal([]byte(initial), &old)
			}
			want := normalizeConcurrentConfig(old.Concurrent)
			if got := normalizeConcurrentConfig(c.Concurrent); got != want {
				t.Fatalf("%s => %#v want %#v", raw, got, want)
			}
			if old.Identity.Target != "" && old.Identity.Target != c.Identity.Target {
				t.Fatal("identity overwritten")
			}
			if old.Gateway.APIPort != 0 && old.Gateway.APIPort != c.Gateway.APIPort {
				t.Fatal("gateway overwritten")
			}
			if err := ensureRuntimeConfigFile(path); err != nil {
				t.Fatal(err)
			}
			again, _ := os.ReadFile(path)
			if string(raw) != string(again) {
				t.Fatal("second startup rewrote config")
			}
			if initial == "" && c.Concurrent != (concurrentFileConfig{defaultMaxAcc, 0}) {
				t.Fatalf("embedded defaults missing: %s", raw)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "config.json")
	for _, invalid := range []string{"null", "{", "[]"} {
		_ = os.WriteFile(path, []byte(invalid), 0600)
		if err := ensureRuntimeConfigFile(path); err != nil {
			t.Fatal(err)
		}
		raw, _ := os.ReadFile(path)
		if string(raw) != invalid {
			t.Fatal("invalid file overwritten")
		}
	}
}

func TestConcurrentWatcherHotReload(t *testing.T) {
	isolatePolicies(t)
	watcher := runtimeConfigWatcher{}
	for _, tc := range []struct {
		raw  string
		want concurrentFileConfig
	}{
		{`{"concurrent":{"MaxAcc":2,"MaxGlobal":3}}`, concurrentFileConfig{2, 3}},
		{`{"concurrent":{"MaxAcc":6,"MaxGlobal":0}}`, concurrentFileConfig{6, 0}},
		{`{}`, concurrentFileConfig{defaultMaxAcc, 0}},
	} {
		writeRuntimeFixture(t, tc.raw)
		if !watcher.reload() {
			t.Fatalf("reload failed: %v", watcher.lastError)
		}
		got, _, _ := modelConcurrency.snapshot("")
		if got != tc.want {
			t.Fatalf("limits=%v want %v", got, tc.want)
		}
	}
	writeRuntimeFixture(t, `{"concurrent":{"MaxAcc":1},"logs":{"retentionDays":-1}}`)
	if watcher.reload() {
		t.Fatal("invalid full document applied")
	}
	got, _, _ := modelConcurrency.snapshot("")
	if got != (concurrentFileConfig{defaultMaxAcc, 0}) {
		t.Fatal("partial invalid update applied")
	}
}

func mustConcurrentAcquire(t *testing.T, l *modelConcurrencyLimiter, key string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err := l.acquire(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	return release
}

func TestConcurrentAccountGlobalAndCancellation(t *testing.T) {
	l := newModelConcurrencyLimiter()
	l.configure(concurrentFileConfig{2, 3})
	a1 := mustConcurrentAcquire(t, l, "a")
	mustConcurrentAcquire(t, l, "a")
	// Waiting on busy account a must not consume the remaining global slot.
	ctx, cancel := context.WithCancel(context.Background())
	waiting := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		release, err := l.acquire(ctx, "a", func() { close(waiting) })
		if release != nil {
			release()
		}
		result <- err
	}()
	<-waiting
	b := mustConcurrentAcquire(t, l, "b")
	blocked, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if release, err := l.acquire(blocked, "c", nil); !errors.Is(err, context.DeadlineExceeded) {
		if release != nil {
			release()
		}
		t.Fatal("global cap not enforced")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	b()
	b()
	a1()
	a1()
	_, total, ac := l.snapshot("a")
	if total != 1 || ac != 1 {
		t.Fatalf("leak or double release: total=%d a=%d", total, ac)
	}
}

func TestConcurrentResizePreservesInflightAndWakesWaiters(t *testing.T) {
	l := newModelConcurrencyLimiter()
	l.configure(concurrentFileConfig{1, 1})
	first := mustConcurrentAcquire(t, l, "a")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	queued := make(chan struct{})
	acquired := make(chan func(), 1)
	go func() {
		release, err := l.acquire(ctx, "a", func() { close(queued) })
		if err == nil {
			acquired <- release
		}
	}()
	<-queued
	l.configure(concurrentFileConfig{2, 2})
	var second func()
	select {
	case second = <-acquired:
	case <-ctx.Done():
		t.Fatal("raising limits did not wake queue")
	}
	defer second()
	l.configure(concurrentFileConfig{1, 1})
	_, count, _ := l.snapshot("a")
	if count != 2 {
		t.Fatal("shrinking reset active count")
	}
	first()
	blocked, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if release, err := l.acquire(blocked, "a", nil); err == nil {
		release()
		t.Fatal("admitted while active count equals reduced limit")
	}
	second()
	mustConcurrentAcquire(t, l, "a")
	l.configure(concurrentFileConfig{1, 0})
	for _, key := range []string{"b", "c", "d", "e", "f"} {
		mustConcurrentAcquire(t, l, key)
	}
}

func TestConcurrentStreamLifecycleRelease(t *testing.T) {
	for _, mode := range []string{"eof", "close", "cancel", "client_cancel"} {
		t.Run(mode, func(t *testing.T) {
			l := newModelConcurrencyLimiter()
			release := mustConcurrentAcquire(t, l, "a")
			ctx, clientCancel := context.WithCancel(context.Background())
			defer clientCancel()
			req := httptest.NewRequest("POST", "/", nil).WithContext(ctx)
			resp := &http.Response{Body: io.NopCloser(strings.NewReader("ok"))}
			cancel := bindModelConcurrency(req, resp, func() {}, release)
			_, active, _ := l.snapshot("a")
			if active != 1 {
				t.Fatal("released at headers rather than end of stream")
			}
			switch mode {
			case "eof":
				_, _ = io.ReadAll(resp.Body)
			case "close":
				_ = resp.Body.Close()
			case "cancel":
				cancel()
			case "client_cancel":
				clientCancel()
			}
			deadline := time.Now().Add(time.Second)
			for {
				_, active, _ = l.snapshot("a")
				if active == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("slot leaked")
				}
				time.Sleep(time.Millisecond)
			}
			resp.Body.Close()
			cancel()
			_, active, _ = l.snapshot("a")
			if active != 0 {
				t.Fatal("double release")
			}
		})
	}
}

func TestConcurrentActualUpstreamFourRequestsShareAccount(t *testing.T) {
	isolatePolicies(t)
	modelConcurrency.configure(concurrentFileConfig{4, 0})
	accounts = []*Account{{Path: "fixture.json", Auth: &StoredAuth{Auth: StoredTokens{
		AccessToken: "synthetic-only", ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}}}}
	entered := make(chan struct{}, 4)
	unblock := make(chan struct{})
	cfg.HttpClient = &http.Client{Transport: retryTransportFunc(func(r *http.Request) (*http.Response, error) {
		entered <- struct{}{}
		select {
		case <-unblock:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			r := httptest.NewRequest("POST", "/v1/chat/completions", nil).WithContext(ctx)
			resp, _, _, ok := upstreamChat(httptest.NewRecorder(), r, id, "fixture-model", []byte(`{"model":"fixture-model"}`), time.Now(), "", false)
			if !ok {
				t.Error("upstream request rejected")
				return
			}
			resp.Body.Close()
		}(uint64(i + 1))
	}
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-ctx.Done():
			close(unblock)
			wg.Wait()
			t.Fatal("same-account requests were serialized")
		}
	}
	close(unblock)
	wg.Wait()
	_, active, _ := modelConcurrency.snapshot("")
	if active != 0 {
		t.Fatalf("actual upstream leaked %d slots", active)
	}
}

func TestConcurrentActualUpstreamRetainsSlotsAfterHeadersAndCancelsQueue(t *testing.T) {
	isolatePolicies(t)
	modelConcurrency.configure(concurrentFileConfig{1, 1})
	accounts = []*Account{{Path: "fixture.json", Auth: &StoredAuth{Auth: StoredTokens{
		AccessToken: "synthetic-only", ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}}}}
	var mu sync.Mutex
	calls := 0
	cfg.HttpClient = &http.Client{Transport: retryTransportFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return &http.Response{StatusCode: 200, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader("still-streaming"))}, nil
	})}
	request := func(ctx context.Context) (*http.Response, bool) {
		r := httptest.NewRequest("POST", "/v1/responses", nil).WithContext(ctx)
		resp, _, _, ok := upstreamChat(httptest.NewRecorder(), r, 1, "fixture-model",
			[]byte(`{"model":"fixture-model"}`), time.Now(), "", false)
		return resp, ok
	}
	first, ok := request(context.Background())
	if !ok {
		t.Fatal("first request rejected")
	}
	defer first.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if resp, ok := request(ctx); ok {
		resp.Body.Close()
		t.Fatal("second request bypassed active stream cap")
	}
	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("canceled queue sent %d upstream requests", got)
	}
	first.Body.Close()
	// Read lock must also be released after headers, so maintenance is possible.
	if !accounts[0].lock.TryLock() {
		t.Fatal("stream retained credential maintenance lock")
	}
	accounts[0].lock.Unlock()
	next, ok := request(context.Background())
	if !ok {
		t.Fatal("slot was not released by closing first stream")
	}
	next.Body.Close()
	_, active, _ := modelConcurrency.snapshot("")
	if active != 0 {
		t.Fatalf("remaining slots %d", active)
	}
}

func TestConcurrentActualNetworkErrorDoesNotLeak(t *testing.T) {
	isolatePolicies(t)
	modelConcurrency.configure(concurrentFileConfig{1, 1})
	accounts = []*Account{{Path: "fixture.json", Auth: &StoredAuth{Auth: StoredTokens{
		AccessToken: "synthetic-only", ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}}}}
	cfg.HttpClient = &http.Client{Transport: retryTransportFunc(func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("synthetic permanent transport failure")
	})}
	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	if resp, _, _, ok := upstreamChat(httptest.NewRecorder(), req, 1, "fixture-model",
		[]byte(`{"model":"fixture-model"}`), time.Now(), "", false); ok {
		resp.Body.Close()
		t.Fatal("network failure returned success")
	}
	_, active, _ := modelConcurrency.snapshot("")
	if active != 0 {
		t.Fatalf("network error leaked %d slots", active)
	}
}

func TestConcurrentHugeLimitsDoNotAllocateSlots(t *testing.T) {
	l := newModelConcurrencyLimiter()
	l.configure(concurrentFileConfig{int(^uint(0) >> 1), int(^uint(0) >> 1)})
	release := mustConcurrentAcquire(t, l, "fixture")
	release()
}

func TestConcurrentLimitsSharedAcrossProtocols(t *testing.T) {
	isolatePolicies(t)
	resetStatsState(t)
	modelConcurrency.configure(concurrentFileConfig{4, 1})
	accounts = []*Account{{Path: "fixture.json", Auth: &StoredAuth{Auth: StoredTokens{
		AccessToken: "synthetic-only", ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}}}}
	entered, allow := make(chan struct{}, 3), make(chan struct{}, 3)
	cfg.HttpClient = &http.Client{Transport: retryTransportFunc(func(r *http.Request) (*http.Response, error) {
		entered <- struct{}{}
		select {
		case <-allow:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(conversationFixtureSSE(false)))}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	handler := newAPIHandler()
	results := make(chan int, 3)
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			payload := `{"model":"fixture-model","stream":false,"max_tokens":32,"input":"test","messages":[{"role":"user","content":"test"}]}`
			req := httptest.NewRequest("POST", path, strings.NewReader(payload)).WithContext(ctx)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			results <- w.Code
		}(path)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("protocol request never reached upstream")
		}
		select {
		case <-entered:
			t.Fatal("another protocol bypassed global limit")
		case <-time.After(15 * time.Millisecond):
		}
		allow <- struct{}{}
	}
	for i := 0; i < 2; i++ {
		select {
		case code := <-results:
			if code != 200 {
				t.Errorf("protocol status=%d", code)
			}
		case <-ctx.Done():
			t.Fatal("handler did not finish")
		}
	}
	_, active, _ := modelConcurrency.snapshot("")
	if active != 0 {
		t.Fatalf("protocols leaked %d slots", active)
	}
	files, _ := filepath.Glob(filepath.Join(logDir, "gateway-*.log"))
	var audit string
	for _, path := range files {
		raw, _ := os.ReadFile(path)
		audit += string(raw)
	}
	for _, stage := range []string{"阶段=排队", "阶段=准入", "阶段=释放"} {
		if !strings.Contains(audit, "[模型并发] traceId=") || !strings.Contains(audit, stage) {
			t.Fatalf("missing persisted concurrency audit: %s", stage)
		}
	}
	if strings.Contains(audit, "synthetic-only") {
		t.Fatal("concurrency audit leaked token")
	}
}

func TestConcurrentAccountFallbackReleasesOldSlot(t *testing.T) {
	isolatePolicies(t)
	modelConcurrency.configure(concurrentFileConfig{1, 1})
	oldRR := rrIndex
	rrIndex = 0
	t.Cleanup(func() { rrIndex = oldRR })
	accounts = nil
	for _, name := range []string{"first.json", "second.json"} {
		accounts = append(accounts, &Account{Path: name, Auth: &StoredAuth{Auth: StoredTokens{
			AccessToken: "synthetic-only", ExpiresAt: time.Now().Add(time.Hour).Unix(),
		}}})
	}
	calls := 0
	cfg.HttpClient = &http.Client{Transport: retryTransportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{StatusCode: 429, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader(`{"code":6004,"msg":"rate limit"}`))}, nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, _, _, ok := upstreamChat(httptest.NewRecorder(),
		httptest.NewRequest("POST", "/v1/responses", nil).WithContext(ctx), 1, "fixture-model",
		[]byte(`{"model":"fixture-model"}`), time.Now(), "", false)
	if !ok || calls != 2 {
		t.Fatalf("fallback failed: ok=%t calls=%d", ok, calls)
	}
	resp.Body.Close()
	_, active, _ := modelConcurrency.snapshot("")
	if active != 0 {
		t.Fatalf("fallback leaked %d slots", active)
	}
}
