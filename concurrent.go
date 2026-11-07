package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

// 本 fork 升级前单账号严格串行（每账号同时 1 个请求），因此默认值取 1 保持
// 行为不变；需要放宽风控换吞吐时在 config.json 的 concurrent.MaxAcc 显式调大。
// 上游默认 4 是因为它没有单账号串行锁。
const defaultMaxAcc = 1

type concurrentFileConfig struct {
	MaxAcc    int `json:"MaxAcc"`
	MaxGlobal int `json:"MaxGlobal"`
}

// Empty/null/wrongly typed values fall back per field. Unknown keys still
// follow the strict validation used by the rest of the runtime configuration.
func (c *concurrentFileConfig) UnmarshalJSON(raw []byte) error {
	*c = concurrentFileConfig{}
	var fields struct {
		MaxAcc    json.RawMessage `json:"MaxAcc"`
		MaxGlobal json.RawMessage `json:"MaxGlobal"`
	}
	if raw = bytes.TrimSpace(raw); len(raw) == 0 || raw[0] != '{' {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fields); err != nil {
		return err
	}
	if n := 0; json.Unmarshal(fields.MaxAcc, &n) == nil {
		c.MaxAcc = n
	}
	if n := 0; json.Unmarshal(fields.MaxGlobal, &n) == nil {
		c.MaxGlobal = n
	}
	return nil
}

func normalizeConcurrentConfig(c concurrentFileConfig) concurrentFileConfig {
	if c.MaxAcc <= 0 {
		c.MaxAcc = defaultMaxAcc
	}
	if c.MaxGlobal < 0 {
		c.MaxGlobal = 0
	}
	return c
}

// Both limits are acquired atomically. A busy account never consumes a global
// slot while waiting. Hot reload changes limits, never live counters/channels.
type modelConcurrencyLimiter struct {
	mu       sync.Mutex
	limits   concurrentFileConfig
	total    int
	accounts map[string]int
	changed  chan struct{}
}

func newModelConcurrencyLimiter() *modelConcurrencyLimiter {
	return &modelConcurrencyLimiter{
		limits:   normalizeConcurrentConfig(concurrentFileConfig{}),
		accounts: make(map[string]int), changed: make(chan struct{}),
	}
}

var modelConcurrency = newModelConcurrencyLimiter()

func logConcurrentConfig(trace string) {
	limits, active, _ := modelConcurrency.snapshot("")
	log.Printf("[并发配置] traceId=%s 单账号上限=%d 全局上限=%d 当前占用=%d 说明=全局0表示不限；排队使用新上限，已有流不中断，缩容待已有请求自然释放",
		trace, limits.MaxAcc, limits.MaxGlobal, active)
}

func (l *modelConcurrencyLimiter) notifyLocked() {
	close(l.changed)
	l.changed = make(chan struct{})
}

func (l *modelConcurrencyLimiter) configure(c concurrentFileConfig) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limits = normalizeConcurrentConfig(c)
	l.notifyLocked()
}

func (l *modelConcurrencyLimiter) snapshot(key string) (concurrentFileConfig, int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limits, l.total, l.accounts[key]
}

func (l *modelConcurrencyLimiter) acquire(ctx context.Context, key string, onWait func()) (func(), error) {
	waitLogged := false
	for {
		l.mu.Lock()
		if err := ctx.Err(); err != nil {
			l.mu.Unlock()
			return nil, err
		}
		if l.accounts[key] < l.limits.MaxAcc && (l.limits.MaxGlobal == 0 || l.total < l.limits.MaxGlobal) {
			l.total++
			l.accounts[key]++
			l.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					l.mu.Lock()
					defer l.mu.Unlock()
					l.total--
					l.accounts[key]--
					if l.accounts[key] == 0 {
						delete(l.accounts, key)
					}
					l.notifyLocked()
				})
			}, nil
		}
		changed := l.changed
		l.mu.Unlock()
		if !waitLogged && onWait != nil {
			onWait()
			waitLogged = true
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func acquireModelConcurrency(r *http.Request, acc *Account, reqID uint64, trace string) (func(), error) {
	key, err := filepath.Abs(acc.Path)
	if err != nil {
		key = filepath.Clean(acc.Path)
	}
	started := time.Now()
	limiter := modelConcurrency
	audit := func(stage string) {
		limits, total, account := limiter.snapshot(key)
		log.Printf("[模型并发] traceId=%s requestId=%d 阶段=%s 账号=%s 单账号占用=%d/%d 全局占用=%d/%d 等待或占用耗时=%v 说明=全局0表示不限；不记录凭据或正文",
			trace, reqID, stage, acc.Path, account, limits.MaxAcc, total, limits.MaxGlobal, time.Since(started))
		debugEvent(r, "debug", "model_concurrency_"+stage, map[string]any{
			"account_active": account, "global_active": total,
			"max_acc": limits.MaxAcc, "max_global": limits.MaxGlobal,
			"elapsed_ms": time.Since(started).Milliseconds(),
		})
	}
	release, err := limiter.acquire(r.Context(), key, func() { audit("排队") })
	if err != nil {
		audit("取消未发送")
		return nil, err
	}
	audit("准入")
	var once sync.Once
	return func() { once.Do(func() { release(); audit("释放") }) }, nil
}

// Hold a slot for the whole stream, not just response headers. EOF/read error,
// Close, explicit cancellation and client cancellation all release exactly once.
func bindModelConcurrency(r *http.Request, resp *http.Response, cancel context.CancelFunc, release func()) context.CancelFunc {
	stop := context.AfterFunc(r.Context(), release)
	finish := func() { stop(); release() }
	resp.Body = &concurrentResponseBody{ReadCloser: resp.Body, finish: finish}
	return func() { cancel(); finish() }
}

type concurrentResponseBody struct {
	io.ReadCloser
	finish func()
}

func (b *concurrentResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.finish()
	}
	return n, err
}

func (b *concurrentResponseBody) Close() error {
	defer b.finish()
	return b.ReadCloser.Close()
}
