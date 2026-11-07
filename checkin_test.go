package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func checkinAt(hour, minute int) time.Time {
	return time.Date(2026, 10, 25, hour, minute, 0, 0, checkinZone)
}

func checkinFixture(t *testing.T) (*dailyCheckinScheduler, *time.Time, checkinTarget) {
	t.Helper()
	chdirTemp(t)
	now := checkinAt(7, 0)
	s := newDailyCheckinScheduler(checkinStateFile)
	s.now = func() time.Time { return now }
	s.random = func(window time.Duration) (time.Duration, error) { return window / 2, nil }
	acc := &Account{Path: "synthetic-checkin.json", Auth: &StoredAuth{
		Edition: "cn", Auth: StoredTokens{AccessToken: "synthetic-checkin-token"},
		Account: StoredAccount{UID: "synthetic-checkin-user"},
	}}
	accountMu.Lock()
	target := checkinTargetLocked(acc)
	oldAccounts := accounts
	accounts = []*Account{acc}
	accountMu.Unlock()
	oldStore := dailyCheckins
	dailyCheckins = s
	t.Cleanup(func() {
		dailyCheckins = oldStore
		accountMu.Lock()
		accounts = oldAccounts
		accountMu.Unlock()
	})
	return s, &now, target
}

func TestCheckinPlanPersistsWithoutStartupOrCredentialEventRequests(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	if jobs := s.planAndClaim(*now, targets, true); len(jobs) != 0 {
		t.Fatal("startup must never dispatch")
	}
	p := *s.doc.Plans[target.key]
	if p.FirstAt != checkinAt(8, 45).UnixMilli() || p.RetryAt != checkinAt(9, 45).UnixMilli() {
		t.Fatalf("wrong windows: %+v", p)
	}
	for _, at := range []time.Time{checkinAt(7, 30), checkinAt(8, 0), checkinAt(8, 44)} {
		if jobs := s.planAndClaim(at, targets, false); len(jobs) != 0 {
			t.Fatal("polling an account change triggered early checkin")
		}
	}
	restarted := newDailyCheckinScheduler(s.file)
	if jobs := restarted.planAndClaim(checkinAt(7, 45), targets, true); len(jobs) != 0 {
		t.Fatal("restart before 08:00 sent a request")
	}
	if got := restarted.doc.Plans[target.key]; got.FirstAt != p.FirstAt || got.RetryAt != p.RetryAt {
		t.Fatal("restart rerolled schedule")
	}
	raw, _ := os.ReadFile(s.file)
	for _, private := range []string{"synthetic-checkin-token", "synthetic-checkin-user", target.account.Path} {
		if strings.Contains(string(raw), private) {
			t.Fatalf("state contains private identity: %s", private)
		}
	}
}

func TestCheckinSuccessAndAlreadyDonePreventAllFurtherAttempts(t *testing.T) {
	for _, outcome := range []string{"ok", "already"} {
		t.Run(outcome, func(t *testing.T) {
			s, now, target := checkinFixture(t)
			targets := []checkinTarget{target}
			calls := 0
			s.perform = func(context.Context, *Account, string) (string, error) { calls++; return outcome, nil }
			s.planAndClaim(*now, targets, true)
			*now = checkinAt(8, 45)
			jobs := s.planAndClaim(*now, targets, false)
			if len(jobs) != 1 || jobs[0].attempt != 1 {
				t.Fatal("first scheduled attempt missing")
			}
			var saved checkinDocument
			raw, _ := os.ReadFile(s.file)
			json.Unmarshal(raw, &saved)
			if saved.Plans[target.key].Attempts != 1 || saved.Plans[target.key].Status != "running" {
				t.Fatal("request was dispatched before attempt reservation persisted")
			}
			if len(s.planAndClaim(*now, targets, false)) != 0 {
				t.Fatal("inflight attempt duplicated")
			}
			s.runJob(context.Background(), jobs[0])
			*now = checkinAt(9, 45)
			if len(s.planAndClaim(*now, targets, false)) != 0 || calls != 1 {
				t.Fatal("success retried")
			}
			if !s.nextLocked(*now).IsZero() {
				t.Fatal("finished day must not advertise a fabricated fixed next-day sign-in time")
			}
			if view := s.view(target, *now); view.Status != "success" || checkinDisplay(view, "cn", *now) != "已签" {
				t.Fatalf("success display: %+v", view)
			}
			restarted := newDailyCheckinScheduler(s.file)
			restarted.planAndClaim(*now, targets, true)
			if restarted.view(target, *now).Status != "success" || len(restarted.planAndClaim(*now, targets, false)) != 0 {
				t.Fatal("success not retained across restart")
			}
		})
	}
}

func TestCheckinFailureRetriesExactlyOnceInSecondWindow(t *testing.T) {
	for _, retryOK := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry-fails", true: "retry-succeeds"}[retryOK], func(t *testing.T) {
			s, now, target := checkinFixture(t)
			targets := []checkinTarget{target}
			calls := 0
			s.perform = func(context.Context, *Account, string) (string, error) {
				calls++
				if retryOK && calls == 2 {
					return "ok", nil
				}
				return "failed", errors.New("synthetic-private-token-must-not-be-persisted")
			}
			s.planAndClaim(*now, targets, true)
			*now = checkinAt(8, 45)
			s.runJob(context.Background(), s.planAndClaim(*now, targets, false)[0])
			if view := s.view(target, *now); view.Status != "failed" || view.Attempts != 1 || view.NextAt != checkinAt(9, 45).Unix() {
				t.Fatalf("failure/retry state wrong: %+v", view)
			}
			for _, at := range []time.Time{checkinAt(9, 0), checkinAt(9, 30), checkinAt(9, 44)} {
				if len(s.planAndClaim(at, targets, false)) != 0 {
					t.Fatal("retry before random retry time")
				}
			}
			*now = checkinAt(9, 45)
			jobs := s.planAndClaim(*now, targets, false)
			if len(jobs) != 1 || jobs[0].attempt != 2 {
				t.Fatal("exactly one retry required")
			}
			s.runJob(context.Background(), jobs[0])
			for _, at := range []time.Time{checkinAt(9, 59), checkinAt(10, 0), checkinAt(23, 59)} {
				if len(s.planAndClaim(at, targets, false)) != 0 {
					t.Fatal("third or out-of-window attempt")
				}
			}
			if calls != 2 {
				t.Fatalf("calls=%d", calls)
			}
			raw, _ := os.ReadFile(s.file)
			if strings.Contains(string(raw), "synthetic-private-token") {
				t.Fatal("upstream error leaked into persisted state")
			}
		})
	}
}

func TestCheckinRestartDoesNotReplayFailuresOrInterruptedRequests(t *testing.T) {
	for _, mode := range []string{"retry", "interrupted-first", "interrupted-retry"} {
		t.Run(mode, func(t *testing.T) {
			s, now, target := checkinFixture(t)
			targets := []checkinTarget{target}
			s.planAndClaim(*now, targets, true)
			p := s.doc.Plans[target.key]
			restartAt := checkinAt(8, 50)
			switch mode {
			case "retry":
				p.Attempts, p.Status = 1, "failed"
				restartAt = checkinAt(9, 50)
			case "interrupted-first":
				p.Attempts, p.Status = 1, "running"
				restartAt = checkinAt(9, 0)
			case "interrupted-retry":
				p.Attempts, p.Status = 2, "running"
				restartAt = checkinAt(9, 50)
			}
			s.saveLocked("synthetic-fixture")
			n := newDailyCheckinScheduler(s.file)
			if len(n.planAndClaim(restartAt, targets, true)) != 0 || len(n.planAndClaim(restartAt, targets, false)) != 0 {
				t.Fatal("restart caught up a past request")
			}
			if mode == "interrupted-first" {
				jobs := n.planAndClaim(checkinAt(9, 45), targets, false)
				if len(jobs) != 1 || jobs[0].attempt != 2 {
					t.Fatal("uncertain first attempt may only use the one scheduled retry")
				}
			} else if len(n.planAndClaim(checkinAt(9, 59), targets, false)) != 0 {
				t.Fatal("missed or exhausted attempt reappeared")
			}
		})
	}
}

func TestCheckinNewAccountsDoNotTriggerEarlyOrCatchupRequests(t *testing.T) {
	s, now, target := checkinFixture(t)
	s.planAndClaim(*now, nil, true)
	if jobs := s.planAndClaim(checkinAt(8, 50), []checkinTarget{target}, false); len(jobs) != 0 {
		t.Fatal("late account addition triggered catchup")
	}
	if s.view(target, checkinAt(8, 50)).Status != "missed" {
		t.Fatal("past plan should be marked missed")
	}
}

func TestCheckinUTC8DayRollAndClockRollback(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	s.planAndClaim(*now, targets, true)
	s.doc.Plans[target.key].Status, s.doc.Plans[target.key].Attempts = "success", 1
	s.saveLocked("synthetic-fixture")
	yesterday := s.view(target, *now)
	tomorrow := time.Date(2026, 10, 25, 16, 1, 0, 0, time.UTC) // UTC+8 October 26
	if got := checkinDisplay(yesterday, "cn", tomorrow); got != "待签" {
		t.Fatalf("yesterday success leaked into today: %s", got)
	}
	if len(s.planAndClaim(tomorrow, targets, false)) != 0 || s.doc.Date != "2026-10-26" {
		t.Fatal("midnight triggered a checkin or failed to roll the Beijing day")
	}
	if view := s.view(target, tomorrow); view.Status != "pending" || view.Attempts != 0 {
		t.Fatalf("new day state: %+v", view)
	}
	if len(s.planAndClaim(checkinAt(8, 45), targets, false)) != 0 || !s.blocked {
		t.Fatal("clock rollback must fail closed instead of repeating a past day")
	}
}

func TestCheckinStorageFailuresFailClosedAndKeepOriginal(t *testing.T) {
	s, now, target := checkinFixture(t)
	const broken = `{"schema":1,"secret":"synthetic-private-value"`
	if err := os.WriteFile(s.file, []byte(broken), 0600); err != nil {
		t.Fatal(err)
	}
	if len(s.planAndClaim(*now, []checkinTarget{target}, true)) != 0 || !s.blocked {
		t.Fatal("corrupt state must disable dispatch")
	}
	raw, _ := os.ReadFile(s.file)
	if string(raw) != broken {
		t.Fatal("corrupt state was overwritten")
	}
	if s.view(target, *now).Status != "unknown" {
		t.Fatal("storage failure must not be shown as success")
	}
}

func TestCheckinReservationWriteFailurePreventsDispatch(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	s.planAndClaim(*now, targets, true)
	if err := os.Remove(s.file); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(s.file, 0700); err != nil {
		t.Fatal(err)
	}
	if len(s.planAndClaim(checkinAt(8, 45), targets, false)) != 0 || !s.blocked {
		t.Fatal("must never dispatch without durably reserving the attempt")
	}
}

func TestCheckinConcurrentTicksAndDuplicateCredentialsReserveOnce(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target, target}
	s.planAndClaim(*now, targets, true)
	var wg sync.WaitGroup
	out := make(chan []checkinJob, 24)
	for i := 0; i < cap(out); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out <- s.planAndClaim(checkinAt(8, 45), targets, false)
		}()
	}
	wg.Wait()
	close(out)
	count := 0
	for jobs := range out {
		count += len(jobs)
	}
	if count != 1 {
		t.Fatalf("same account duplicated: %d", count)
	}
}

func TestCheckinRandomTimesStayInWindowsAndArePerAccount(t *testing.T) {
	s, now, target := checkinFixture(t)
	s.random = randomCheckinOffset
	targets := make([]checkinTarget, 32)
	for i := range targets {
		targets[i] = target
		targets[i].key = strings.Repeat("0", 62) + string("0123456789abcdef"[i/16]) + string("0123456789abcdef"[i%16])
	}
	s.planAndClaim(*now, targets, true)
	start, retry, end := checkinWindows(*now)
	distinct := map[int64]bool{}
	for _, p := range s.doc.Plans {
		if p.FirstAt < start.UnixMilli() || p.FirstAt >= retry.UnixMilli() || p.RetryAt < retry.UnixMilli() || p.RetryAt >= end.UnixMilli() {
			t.Fatal("random target outside the allowed windows")
		}
		distinct[p.FirstAt] = true
	}
	if len(distinct) < 2 {
		t.Fatal("accounts do not have independent schedules")
	}
}

func TestCheckinTransportExplicitSuccessAndOneHTTPRequest(t *testing.T) {
	for _, tc := range []struct {
		name, body, result string
		status             int
	}{
		{"success", `{"code":0}`, "ok", 200},
		{"already", `{"code":10001,"msg":"今天已签到"}`, "already", 409},
		{"missing-code", `{}`, "failed", 200},
		{"http-failure", `{"code":0}`, "failed", 500},
		{"auth-failure", `{"code":14001,"msg":"已签到"}`, "failed", 401},
		{"bad-json", `<html>error</html>`, "failed", 200},
		{"business-failure", `{"code":10002}`, "failed", 200},
		{"unrelated-error-message", `{"code":10002,"msg":"已签到状态检查失败"}`, "failed", 200},
		{"no-redirect-replay", `{"code":0}`, "failed", 307},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Header.Get("Authorization") != "Bearer synthetic-token" || r.Header.Get("X-Trace-ID") != "" {
					t.Error("unexpected change to upstream authentication/fingerprint headers")
				}
				if tc.status == 307 {
					w.Header().Set("Location", "/would-replay")
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			oldOrigin, oldClient := profileCN.Origin, cfg.HttpClient
			profileCN.Origin, cfg.HttpClient = server.URL, server.Client()
			defer func() { profileCN.Origin, cfg.HttpClient = oldOrigin, oldClient }()
			acc := &Account{Auth: &StoredAuth{Auth: StoredTokens{AccessToken: "synthetic-token"}}}
			got, err := checkinAccountWithTrace(context.Background(), acc, "synthetic-checkin-trace")
			if got != tc.result || calls != 1 || (err != nil) != (got == "failed") {
				t.Fatalf("got=%s err=%v calls=%d", got, err, calls)
			}
		})
	}
}

func TestCheckinDoesNotHoldModelLockDuringNetworkIO(t *testing.T) {
	arrived, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-release
		w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()
	oldOrigin, oldClient := profileCN.Origin, cfg.HttpClient
	profileCN.Origin, cfg.HttpClient = server.URL, server.Client()
	defer func() { profileCN.Origin, cfg.HttpClient = oldOrigin, oldClient }()
	acc := &Account{Auth: &StoredAuth{Auth: StoredTokens{AccessToken: "synthetic-token"}}}
	done := make(chan struct{})
	go func() { checkinAccount(context.Background(), acc); close(done) }()
	<-arrived
	available := acc.lock.TryRLock()
	if available {
		acc.lock.RUnlock()
	}
	close(release)
	<-done
	if !available {
		t.Fatal("checkin network IO blocked model credential reads")
	}
}

func TestCheckinSnapshotAndTableShowTodayWithoutChangingModelLedger(t *testing.T) {
	isolateAdminFeatures(t)
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	s.planAndClaim(*now, targets, true)
	s.doc.Plans[target.key].Status, s.doc.Plans[target.key].Attempts = "success", 1
	s.saveLocked("synthetic-fixture")
	// Status snapshots use the real clock; align only this display fixture.
	s.doc.Date = checkinDay(time.Now())
	if err := writeStatusSnapshot(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(statusSnapshotFile)
	var snapshot statusSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil || len(snapshot.Accounts) != 1 {
		t.Fatalf("snapshot: %v", err)
	}
	view := snapshot.Accounts[0].Checkin
	if view.Status != "success" || view.Date != checkinDay(time.Now()) {
		t.Fatalf("snapshot checkin missing: %+v", view)
	}
	table := renderAccountTable(snapshot.Accounts)
	// 本 fork 账号表保留「免费模型」列（上游没有），签到列插入在套餐之后。
	if !strings.Contains(table, "签到") || !strings.Contains(table, "已签") || !strings.Contains(table, "免费模型") {
		t.Fatalf("wrong monitor table: %s", table)
	}
	if target.account.ModelStates != nil {
		t.Fatal("checkin presentation modified model ledger")
	}
	before, _ := os.ReadFile(s.file)
	w := adminRequest(newWebUIHandler(), http.MethodGet, "/admin/api/status", "feature-admin-key", "")
	var response webStatusResponse
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil ||
		len(response.Accounts) != 1 || response.Accounts[0].Checkin.Status != "success" {
		t.Fatalf("authenticated status API lost today's checkin: HTTP %d", w.Code)
	}
	after, _ := os.ReadFile(s.file)
	if string(before) != string(after) {
		t.Fatal("reading status rewrote the schedule")
	}
	row := taskByID(t, taskViewDefinitions(), "daily_checkin")
	if row.Schedule != "每日上午随机时间签到" {
		t.Fatalf("task page advertises old scheduling semantics: %s", row.Schedule)
	}
}

func TestCheckinCredentialIdentityChangeCannotUseOldReservation(t *testing.T) {
	s, _, target := checkinFixture(t)
	ctx := context.WithValue(context.Background(), checkinExpectedIdentity{}, target.key)
	accountMu.Lock()
	target.account.Auth.Account.UID = "different-synthetic-user"
	accountMu.Unlock()
	// No HTTP client is required: identity mismatch must be rejected before IO.
	result, err := checkinAccountWithTrace(ctx, target.account, "synthetic-identity-change")
	if result != "skipped" || err != nil {
		t.Fatalf("identity change not stopped: %s %v store=%s", result, err, s.file)
	}
}

func TestCheckinRealBackgroundLoopRunsSinglePendingAccountImmediately(t *testing.T) {
	s, now, target := checkinFixture(t)
	s.planAndClaim(*now, []checkinTarget{target}, true)
	*now = checkinAt(8, 50) // persisted 08:45 plan is in the past
	var calls atomic.Int32
	n := newDailyCheckinScheduler(s.file)
	n.now = func() time.Time { return *now }
	n.perform = func(context.Context, *Account, string) (string, error) {
		calls.Add(1)
		return "ok", nil
	}
	dailyCheckins = n
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { n.run(ctx); close(done) }()
	timer := time.NewTimer(1200 * time.Millisecond)
	<-timer.C // include at least one real timer tick, not just helper calls
	cancel()
	<-done
	if calls.Load() != 1 || n.view(target, *now).Status != "success" {
		t.Fatal("background startup did not sign the single pending account exactly once")
	}
}

func TestCheckinStrictWindowBoundaries(t *testing.T) {
	for _, stage := range []string{"first-start", "first-end", "retry-start", "retry-end"} {
		t.Run(stage, func(t *testing.T) {
			s, now, target := checkinFixture(t)
			s.random = func(time.Duration) (time.Duration, error) { return 0, nil }
			targets := []checkinTarget{target}
			s.planAndClaim(*now, targets, true)
			at, want := checkinAt(8, 0), 1
			if strings.HasPrefix(stage, "retry") {
				p := s.doc.Plans[target.key]
				p.Status, p.Attempts = "failed", 1
				at = checkinAt(9, 30)
			}
			switch stage {
			case "first-end":
				at, want = checkinAt(9, 30), 0
			case "retry-end":
				at, want = checkinAt(10, 0), 0
			}
			if got := s.planAndClaim(at, targets, false); len(got) != want {
				t.Fatalf("boundary %s got jobs=%d want=%d", stage, len(got), want)
			}
		})
	}
}

func TestCheckinEndToEndTwoHTTPAttemptsSurviveRestart(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/billing/meter/daily-checkin" || r.Method != http.MethodPost {
			t.Error("unexpected upstream operation")
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"code":500,"msg":"synthetic temporary failure"}`))
		} else {
			w.Write([]byte(`{"code":0}`))
		}
	}))
	defer server.Close()
	oldOrigin, oldClient := profileCN.Origin, cfg.HttpClient
	profileCN.Origin, cfg.HttpClient = server.URL, server.Client()
	defer func() { profileCN.Origin, cfg.HttpClient = oldOrigin, oldClient }()
	s.planAndClaim(*now, targets, true)
	if calls.Load() != 0 {
		t.Fatal("startup made an HTTP request")
	}
	*now = checkinAt(8, 45)
	s.runJob(context.Background(), s.planAndClaim(*now, targets, false)[0])
	if calls.Load() != 1 || s.view(target, *now).Status != "failed" {
		t.Fatal("first HTTP failure was not recorded")
	}
	*now = checkinAt(9, 0)
	n := newDailyCheckinScheduler(s.file)
	n.now = func() time.Time { return *now }
	dailyCheckins = n
	n.planAndClaim(*now, targets, true)
	if calls.Load() != 1 || len(n.planAndClaim(*now, targets, false)) != 0 {
		t.Fatal("restart retried outside the retry window")
	}
	*now = checkinAt(9, 45)
	jobs := n.planAndClaim(*now, targets, false)
	if len(jobs) != 1 || jobs[0].attempt != 2 {
		t.Fatal("retry slot lost after restart")
	}
	n.runJob(context.Background(), jobs[0])
	if calls.Load() != 2 || n.view(target, *now).Status != "success" {
		t.Fatal("retry HTTP success missing")
	}
	*now = checkinAt(9, 59)
	restarted := newDailyCheckinScheduler(n.file)
	restarted.planAndClaim(*now, targets, true)
	if len(restarted.planAndClaim(*now, targets, false)) != 0 || calls.Load() != 2 {
		t.Fatal("third HTTP request after success/restart")
	}
}

func checkinStartupTargets(t *testing.T, count int) []checkinTarget {
	t.Helper()
	accountMu.Lock()
	defer accountMu.Unlock()
	accounts = nil
	var targets []checkinTarget
	for i := 0; i < count; i++ {
		a := &Account{Path: fmt.Sprintf("synthetic-startup-%d.json", i), Auth: &StoredAuth{
			Edition: "cn", Auth: StoredTokens{AccessToken: "synthetic-startup-token"},
			Account: StoredAccount{UID: fmt.Sprintf("synthetic-startup-user-%d", i)},
		}}
		accounts = append(accounts, a)
		targets = append(targets, checkinTargetLocked(a))
	}
	return targets
}

func TestCheckinStartupSinglePendingAfterEightIsImmediate(t *testing.T) {
	for _, at := range []time.Time{checkinAt(8, 0), checkinAt(8, 20), checkinAt(10, 30), checkinAt(23, 59)} {
		t.Run(at.Format("15-04"), func(t *testing.T) {
			s, now, target := checkinFixture(t)
			targets := []checkinTarget{target}
			s.planAndClaim(*now, targets, true)
			*now = at
			jobs := s.planAndClaim(*now, targets, true)
			if len(jobs) != 1 || jobs[0].attempt != 1 {
				t.Fatal("single pending account should run immediately on startup after 08:00")
			}
			var saved checkinDocument
			raw, _ := os.ReadFile(s.file)
			if json.Unmarshal(raw, &saved) != nil || saved.Plans[target.key].Attempts != 1 ||
				saved.Plans[target.key].StartupAt != at.UnixMilli() {
				t.Fatal("startup attempt was not durably reserved")
			}
			s.perform = func(context.Context, *Account, string) (string, error) { return "ok", nil }
			s.runJob(context.Background(), jobs[0])
			if s.view(target, *now).Status != "success" || len(s.planAndClaim(*now, targets, true)) != 0 {
				t.Fatal("completed account was replayed on another restart")
			}
		})
	}
}

func TestCheckinStartupMultipleAccountsOneMinuteApart(t *testing.T) {
	s, now, _ := checkinFixture(t)
	targets := checkinStartupTargets(t, 3)
	s.planAndClaim(*now, targets, true)
	s.perform = func(context.Context, *Account, string) (string, error) { return "ok", nil }
	*now = checkinAt(8, 20)
	for i := range targets {
		jobs := s.planAndClaim(*now, targets, i == 0)
		if len(jobs) != 1 || jobs[0].key != targets[i].key {
			t.Fatalf("queue item %d did not dispatch alone", i)
		}
		s.runJob(context.Background(), jobs[0])
		*now = now.Add(59 * time.Second)
		if len(s.planAndClaim(*now, targets, false)) != 0 {
			t.Fatal("next account dispatched before the minute elapsed")
		}
		*now = now.Add(time.Second)
	}
	if len(s.planAndClaim(*now, targets, false)) != 0 {
		t.Fatal("finished queue repeated")
	}
	for _, target := range targets {
		if p := s.doc.Plans[target.key]; p.Attempts != 1 || p.Status != "success" {
			t.Fatal("queue did not sign each account exactly once")
		}
	}
}

func TestCheckinStartupSpacingSurvivesRestartAndSlowRequest(t *testing.T) {
	s, now, _ := checkinFixture(t)
	targets := checkinStartupTargets(t, 3)
	s.planAndClaim(*now, targets, true)
	*now = checkinAt(8, 20)
	first := s.planAndClaim(*now, targets, true)[0]
	// Even an unusually delayed worker cannot overlap a later queue request.
	*now = checkinAt(8, 22)
	if len(s.planAndClaim(*now, targets, false)) != 0 {
		t.Fatal("another startup request overlapped the unfinished request")
	}
	s.perform = func(context.Context, *Account, string) (string, error) { return "ok", nil }
	s.runJob(context.Background(), first)
	*now = now.Add(20 * time.Second)
	n := newDailyCheckinScheduler(s.file)
	n.now = func() time.Time { return *now }
	reversed := []checkinTarget{targets[2], targets[1], targets[0]}
	if len(n.planAndClaim(*now, reversed, true)) != 0 {
		t.Fatal("restart collapsed the interval after the previous request")
	}
	*now = checkinAt(8, 22).Add(59 * time.Second)
	if len(n.planAndClaim(*now, reversed, false)) != 0 {
		t.Fatal("persisted one-minute interval lost")
	}
	*now = checkinAt(8, 23)
	jobs := n.planAndClaim(*now, reversed, false)
	if len(jobs) != 1 || jobs[0].key != targets[1].key {
		t.Fatal("restart lost persisted queue order or due time")
	}
}

func TestCheckinStartupAfterRetryWindowDoesNotAddExtraRetry(t *testing.T) {
	s, now, target := checkinFixture(t)
	*now = checkinAt(14, 0)
	targets := []checkinTarget{target}
	jobs := s.planAndClaim(*now, targets, true) // new deployment with no state file
	if len(jobs) != 1 {
		t.Fatal("new pending account after 08:00 must start without an existing file")
	}
	s.perform = func(context.Context, *Account, string) (string, error) {
		return "failed", errors.New("synthetic error")
	}
	s.runJob(context.Background(), jobs[0])
	if !s.doc.Plans[target.key].RetryEnd || len(s.planAndClaim(now.Add(time.Minute), targets, false)) != 0 {
		t.Fatal("startup catchup illegally extended the failed-retry window")
	}
	if len(s.planAndClaim(*now, targets, true)) != 0 {
		t.Fatal("failed account entered a new startup queue")
	}
}

func TestCheckinStartupRetryAlsoWaitsOneMinute(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	s.planAndClaim(*now, targets, true)
	*now = checkinAt(9, 50)
	first := s.planAndClaim(*now, targets, true)[0]
	s.perform = func(context.Context, *Account, string) (string, error) {
		return "failed", errors.New("synthetic error")
	}
	s.runJob(context.Background(), first)
	*now = now.Add(59 * time.Second)
	if len(s.planAndClaim(*now, targets, false)) != 0 {
		t.Fatal("failed startup account retried too quickly")
	}
	*now = now.Add(time.Second)
	jobs := s.planAndClaim(*now, targets, false)
	if len(jobs) != 1 || jobs[0].attempt != 2 {
		t.Fatal("one allowed retry missing from the existing retry window")
	}
	s.runJob(context.Background(), jobs[0])
	if len(s.planAndClaim(now.Add(time.Minute), targets, false)) != 0 {
		t.Fatal("startup account exceeded its two daily attempts")
	}
}

func TestCheckinStartupSkipsNonPendingAndDoesNotCrossMidnight(t *testing.T) {
	s, now, _ := checkinFixture(t)
	targets := checkinStartupTargets(t, 5)
	s.planAndClaim(*now, targets, true)
	for i, state := range []string{"success", "failed", "missed", "running"} {
		p := s.doc.Plans[targets[i].key]
		p.Status = state
		if state != "missed" {
			p.Attempts = 1
		}
	}
	*now = checkinAt(8, 30)
	jobs := s.planAndClaim(*now, targets, true)
	if len(jobs) != 1 || jobs[0].key != targets[4].key {
		t.Fatal("only pending, never-attempted accounts may enter startup catchup")
	}
	// Separate near-midnight state: only the first slot fits the current day.
	late := newDailyCheckinScheduler("late-checkin-state.json")
	late.random = s.random
	at := checkinAt(23, 59).Add(30 * time.Second)
	if got := late.planAndClaim(at, targets, true); len(got) != 1 {
		t.Fatal("near-midnight first startup slot missing")
	}
	missed := 0
	for _, p := range late.doc.Plans {
		if p.Status == "missed" {
			missed++
		}
	}
	if missed != len(targets)-1 {
		t.Fatal("startup queue must not spill into the next Beijing day")
	}
}

func TestCheckinStartupHTTPQueuePersistsAcrossRepeatedRestarts(t *testing.T) {
	s, now, _ := checkinFixture(t)
	targets := checkinStartupTargets(t, 3)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v2/billing/meter/daily-checkin" {
			t.Error("unexpected startup activity")
		}
		calls.Add(1)
		w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()
	oldOrigin, oldClient := profileCN.Origin, cfg.HttpClient
	profileCN.Origin, cfg.HttpClient = server.URL, server.Client()
	defer func() { profileCN.Origin, cfg.HttpClient = oldOrigin, oldClient }()
	s.planAndClaim(*now, targets, true)
	*now = checkinAt(8, 20)
	s.runJob(context.Background(), s.planAndClaim(*now, targets, true)[0])
	if calls.Load() != 1 {
		t.Fatal("first startup HTTP request must be immediate and single")
	}
	*now = now.Add(30 * time.Second)
	n := newDailyCheckinScheduler(s.file)
	n.now = func() time.Time { return *now }
	dailyCheckins = n
	if len(n.planAndClaim(*now, targets, true)) != 0 || calls.Load() != 1 {
		t.Fatal("quick restart replayed or collapsed HTTP spacing")
	}
	*now = checkinAt(8, 21)
	n.runJob(context.Background(), n.planAndClaim(*now, targets, false)[0])
	*now = checkinAt(8, 22)
	n.runJob(context.Background(), n.planAndClaim(*now, targets, false)[0])
	if calls.Load() != 3 {
		t.Fatal("startup queue did not make exactly one HTTP request per account")
	}
	restarted := newDailyCheckinScheduler(s.file)
	if len(restarted.planAndClaim(*now, targets, true)) != 0 || calls.Load() != 3 {
		t.Fatal("completed startup queue repeated after restart")
	}
}

func TestCheckinStartupYesterdayDoesNotCountAsToday(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	s.planAndClaim(*now, targets, true)
	s.doc.Plans[target.key].Status, s.doc.Plans[target.key].Attempts = "success", 1
	s.saveLocked("synthetic-yesterday")
	*now = time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC) // 08:00 Beijing, next day
	jobs := s.planAndClaim(*now, targets, true)
	if len(jobs) != 1 || jobs[0].date != "2026-10-26" || jobs[0].attempt != 1 {
		t.Fatal("yesterday success prevented today's startup checkin")
	}
}

func TestCheckinStartupInterruptedRemovedAccountDoesNotBlockQueue(t *testing.T) {
	s, now, _ := checkinFixture(t)
	targets := checkinStartupTargets(t, 2)
	s.planAndClaim(*now, targets, true)
	*now = checkinAt(8, 20)
	s.planAndClaim(*now, targets, true) // first reservation, process exits before completion
	n := newDailyCheckinScheduler(s.file)
	*now = now.Add(10 * time.Second)
	remaining := []checkinTarget{targets[1]}
	if len(n.planAndClaim(*now, remaining, true)) != 0 {
		t.Fatal("uncertain last request did not preserve the safety interval")
	}
	if n.doc.Plans[targets[0].key].Status != "failed" {
		t.Fatal("removed interrupted account was not recovered")
	}
	*now = now.Add(time.Minute)
	if jobs := n.planAndClaim(*now, remaining, false); len(jobs) != 1 || jobs[0].key != targets[1].key {
		t.Fatal("removed account's in-flight marker permanently blocked pending accounts")
	}
}

func TestCheckinStartupRetryDeadlineWinsOverMinuteSpacing(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	s.planAndClaim(*now, targets, true)
	*now = checkinAt(9, 59).Add(30 * time.Second)
	job := s.planAndClaim(*now, targets, true)[0]
	s.perform = func(context.Context, *Account, string) (string, error) {
		return "failed", errors.New("synthetic failure")
	}
	s.runJob(context.Background(), job)
	if s.view(target, *now).NextAt != 0 {
		t.Fatal("UI advertised a retry after 10:00")
	}
	*now = checkinAt(10, 0).Add(30 * time.Second)
	if len(s.planAndClaim(*now, targets, false)) != 0 {
		t.Fatal("startup failure retried beyond the original retry window")
	}
}
