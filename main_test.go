package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMain 将整个测试进程切到临时工作目录，确保任何写状态/缓存/日志的测试
// 都不会污染仓库目录（这些文件按设计写在进程 cwd）。
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "workbuddy-gateway-test-*")
	if err != nil {
		panic(err)
	}
	if err := os.Chdir(dir); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// chdirTemp 将测试工作目录切到临时目录，避免测试写入仓库内的状态/缓存文件。
func chdirTemp(t *testing.T) {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
}

// 验证 429 消息中的重置时间解析
func TestParseResetTime(t *testing.T) {
	msg := `{"code":429,"message":"upstream 429: {\"code\":6004,\"msg\":\"您的使用量已超出频率限制，将在 2026-09-04 07:48:15 UTC+8 重置，您也可以切换其他模型继续使用。\",\"requestId\":\"149e3299-ddf3-4ad1-aaa8-cc752c37630a\"}","type":"upstream_error"}`
	got, ok := parseResetTime(msg)
	if !ok {
		t.Fatal("parseResetTime should succeed")
	}
	want := time.Date(2026, 9, 4, 7, 48, 15, 0, time.FixedZone("UTC+8", 8*3600))
	if !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	t.Logf("parsed reset time: %v", got)
}

// 验证无法解析时返回 false
func TestParseResetTimeInvalid(t *testing.T) {
	if _, ok := parseResetTime("some random error"); ok {
		t.Fatal("should not parse random string")
	}
}

// 验证国际站英文限流消息的重置时间解析（通用兜底正则）
func TestParseResetTimeEnglish(t *testing.T) {
	cases := []string{
		`{"code":6004,"msg":"Your usage has exceeded the rate limit. It will reset at 2026-09-05 01:57:00 UTC+8."}`,
		`upstream 429: rate limit exceeded, reset at 2026-09-05 01:57:00`,
		`{"msg":"quota exceeded, will reset on 2026-09-05T01:57:00 UTC+8"}`,
	}
	want := time.Date(2026, 9, 5, 1, 57, 0, 0, time.FixedZone("UTC+8", 8*3600))
	for _, msg := range cases {
		got, ok := parseResetTime(msg)
		if !ok {
			t.Fatalf("parseResetTime(%q) should succeed", msg)
		}
		if !got.Equal(want) {
			t.Fatalf("parseResetTime(%q) = %v, want %v", msg, got, want)
		}
	}
}

// 验证站点 Profile 选择：空值/未知回退国内站，intl 系列值命中国际站
func TestProfileForEdition(t *testing.T) {
	cases := []struct {
		edition string
		wantKey string
	}{
		{"", "cn"},
		{"cn", "cn"},
		{"unknown", "cn"},
		{"intl", "intl"},
		{"INTL", "intl"},
		{" workbuddy.ai ", "intl"},
		{"international", "intl"},
	}
	for _, c := range cases {
		if got := profileForEdition(c.edition).Key; got != c.wantKey {
			t.Errorf("profileForEdition(%q).Key = %s, want %s", c.edition, got, c.wantKey)
		}
	}
	if p := profileForEdition("intl"); p.Base != "https://www.workbuddy.ai" || p.Origin != "https://www.workbuddy.ai" {
		t.Errorf("intl profile base/origin unexpected: %+v", p)
	}
	if p := profileForEdition("cn"); p.Base != "https://copilot.tencent.com" || p.Platform != "VSCode" {
		t.Errorf("cn profile base/platform unexpected: %+v", p)
	}
}

// 验证各站点上游 URL 构建与旧版常量完全一致（国内站回归）+ 国际站正确
func TestUpstreamProfileURLs(t *testing.T) {
	cn := profileForEdition("cn")
	if got := cn.authStateURL(); got != "https://copilot.tencent.com/v2/plugin/auth/state?platform=VSCode" {
		t.Errorf("cn authStateURL = %s", got)
	}
	if got := cn.chatURL(); got != "https://copilot.tencent.com/v2/chat/completions" {
		t.Errorf("cn chatURL = %s", got)
	}
	if got := cn.tokenRefreshURL(); got != "https://copilot.tencent.com/v2/plugin/auth/token/refresh" {
		t.Errorf("cn tokenRefreshURL = %s", got)
	}
	if got := cn.quotaSummaryURL(); got != "https://www.codebuddy.cn/billing/meter/get-user-resource-summary" {
		t.Errorf("cn quotaSummaryURL = %s", got)
	}
	if got := cn.dailyCheckinURL(); got != "https://www.codebuddy.cn/v2/billing/meter/daily-checkin" {
		t.Errorf("cn dailyCheckinURL = %s", got)
	}

	itl := profileForEdition("intl")
	if got := itl.authStateURL(); got != "https://www.workbuddy.ai/v2/plugin/auth/state?platform=workbuddy-ai" {
		t.Errorf("intl authStateURL = %s", got)
	}
	if got := itl.authTokenURL("abc-123"); got != "https://www.workbuddy.ai/v2/plugin/auth/token?state=abc-123" {
		t.Errorf("intl authTokenURL = %s", got)
	}
	if got := itl.loginAcctURL("abc-123"); got != "https://www.workbuddy.ai/v2/plugin/login/account?state=abc-123" {
		t.Errorf("intl loginAcctURL = %s", got)
	}
	if got := itl.chatURL(); got != "https://www.workbuddy.ai/v2/chat/completions" {
		t.Errorf("intl chatURL = %s", got)
	}
	if got := itl.quotaSummaryURL(); got != "https://www.workbuddy.ai/billing/meter/get-user-resource-summary" {
		t.Errorf("intl quotaSummaryURL = %s", got)
	}
}

// 验证账号站点路由：优先取凭据内 edition；失效标记恢复（Auth 为 nil）时取账号上的 edition
func TestAccountProfile(t *testing.T) {
	acc := &Account{Auth: &StoredAuth{Edition: "intl"}}
	if acc.Profile().Key != "intl" {
		t.Fatalf("expected intl via Auth.Edition, got %s", acc.Profile().Key)
	}
	// 失效标记恢复场景：凭据文件已删除
	acc2 := &Account{Edition: "intl"}
	if acc2.Profile().Key != "intl" {
		t.Fatalf("expected intl via Account.Edition, got %s", acc2.Profile().Key)
	}
	// 旧版凭据（无 edition 字段）回退国内站
	acc3 := &Account{Auth: &StoredAuth{}}
	if acc3.Profile().Key != "cn" {
		t.Fatalf("expected cn fallback, got %s", acc3.Profile().Key)
	}
}

// 验证限流识别（429 状态码 / code 6004 / 频率限制关键词，含中英文）
func TestIsRateLimited(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   bool
	}{
		{429, "{}", true},
		{400, `{"code":6004,"msg":"x"}`, true},
		{400, "频率限制", true},
		{400, "frequency limit", true},
		{400, "rate limit exceeded", true},
		{400, "Rate Limit Exceeded", true},
		{400, "ratelimit", true},
		{400, "Too Many Requests", true},
		{400, "some other error", false},
		{500, "{}", false},
	}
	for _, c := range cases {
		if got := isRateLimited(c.status, c.body); got != c.want {
			t.Errorf("isRateLimited(%d, %q) = %v, want %v", c.status, c.body, got, c.want)
		}
	}
}

// 验证轮询选择：两个账号交替返回
func TestNextAccountRoundRobin(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{
		{Path: "a.json", Auth: &StoredAuth{}},
		{Path: "b.json", Auth: &StoredAuth{}},
	}
	rrIndex = 0
	accountMu.Unlock()

	a1, _ := nextAccount()
	a2, _ := nextAccount()
	a3, _ := nextAccount()
	if a1.Path != "a.json" || a2.Path != "b.json" || a3.Path != "a.json" {
		t.Fatalf("round robin failed: %s %s %s", a1.Path, a2.Path, a3.Path)
	}
	t.Logf("round-robin order: %s %s %s", a1.Path, a2.Path, a3.Path)
}

// 验证冷却屏蔽：冷却中的账号被跳过，由另一账号代偿
func TestNextAccountSkipsCooldown(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{
		{Path: "a.json", Auth: &StoredAuth{}, CooldownUntil: time.Now().Add(time.Hour)},
		{Path: "b.json", Auth: &StoredAuth{}},
	}
	rrIndex = 0
	accountMu.Unlock()

	a, _ := nextAccount()
	if a.Path != "b.json" {
		t.Fatalf("expected b.json (only non-cooldown), got %s", a.Path)
	}
	t.Logf("cooldown skip works, selected %s", a.Path)
}

// 验证全部冷却时返回错误
func TestNextAccountAllCooldown(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{
		{Path: "a.json", Auth: &StoredAuth{}, CooldownUntil: time.Now().Add(time.Hour)},
		{Path: "b.json", Auth: &StoredAuth{}, CooldownUntil: time.Now().Add(2 * time.Hour)},
	}
	rrIndex = 0
	accountMu.Unlock()

	_, err := nextAccount()
	if err == nil {
		t.Fatal("expected error when all accounts in cooldown")
	}
	t.Logf("all-cooldown error: %v", err)
}

// 验证冷却到期后自动恢复
func TestNextAccountCooldownExpiry(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{
		{Path: "a.json", Auth: &StoredAuth{}, CooldownUntil: time.Now().Add(-time.Minute)},
		{Path: "b.json", Auth: &StoredAuth{}, CooldownUntil: time.Now().Add(time.Hour)},
	}
	rrIndex = 0
	accountMu.Unlock()

	a, _ := nextAccount()
	if a.Path != "a.json" {
		t.Fatalf("expected a.json (cooldown expired), got %s", a.Path)
	}
	t.Logf("expired cooldown recovers, selected %s", a.Path)
}

func TestNextAccountForModelPrefersKnownFreeExhausted(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{
		{Path: "empty-free.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaExhausted: true, ModelStates: map[string]*modelRuntimeState{
			"free-model": {CostClass: modelCostFree},
		}},
		{Path: "available.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10},
	}
	rrIndex = 0
	accountMu.Unlock()

	acc, kind, err := nextAccountForModel("free-model", nil)
	if err != nil {
		t.Fatal(err)
	}
	if acc.Path != "empty-free.json" || kind != selectionFreeExhausted {
		t.Fatalf("expected free exhausted account, got %s kind=%s", acc.Path, kind)
	}
}

func TestNextAccountForModelAllowsOneUnknownProbe(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{
		{Path: "a.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaExhausted: true},
	}
	rrIndex = 0
	accountMu.Unlock()

	acc, kind, err := nextAccountForModel("unknown-model", nil)
	if err != nil || acc.Path != "a.json" || kind != selectionProbeExhausted {
		t.Fatalf("expected one controlled probe, acc=%v kind=%s err=%v", acc, kind, err)
	}
	if _, _, err = nextAccountForModel("unknown-model", nil); err == nil || !strings.Contains(err.Error(), "等待探测=1") {
		t.Fatalf("expected probe cooldown error, got %v", err)
	}
}

func TestNextAccountForModelSkipsPaidAndQuotaBlocked(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{
		{Path: "paid.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaExhausted: true, ModelStates: map[string]*modelRuntimeState{
			"paid-model": {CostClass: modelCostPaid, QuotaBlocked: true},
		}},
	}
	rrIndex = 0
	accountMu.Unlock()
	if _, _, err := nextAccountForModel("paid-model", nil); err == nil || !strings.Contains(err.Error(), "额度阻断=1") {
		t.Fatalf("expected model quota block, got %v", err)
	}
}

func TestModelCooldownOnlyBlocksTriggeringModel(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{{Path: "a.json", Auth: &StoredAuth{}, ModelStates: map[string]*modelRuntimeState{
		"limited-model": {CostClass: modelCostFree, CooldownUntil: time.Now().Add(time.Hour)},
	}}}
	rrIndex = 0
	accountMu.Unlock()
	if _, _, err := nextAccountForModel("limited-model", nil); err == nil || !strings.Contains(err.Error(), "模型冷却=1") {
		t.Fatalf("expected model cooldown, got %v", err)
	}
	acc, _, err := nextAccountForModel("other-model", nil)
	if err != nil || acc.Path != "a.json" {
		t.Fatalf("other model should remain usable, acc=%v err=%v", acc, err)
	}
}

func TestMarkModelQuotaBlockedDoesNotOverwriteAccountBalance(t *testing.T) {
	oldDir, _ := os.Getwd()
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)
	acc := &Account{Path: "a.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 123}
	accountMu.Lock()
	oldAccounts := accounts
	accounts = []*Account{acc}
	accountMu.Unlock()
	defer func() {
		accountMu.Lock()
		accounts = oldAccounts
		accountMu.Unlock()
	}()
	markModelQuotaBlocked(acc, "paid-model", "14018")
	accountMu.Lock()
	state := acc.ModelStates["paid-model"]
	remaining := acc.QuotaRemaining
	exhausted := acc.QuotaExhausted
	accountMu.Unlock()
	if state == nil || !state.QuotaBlocked || state.CostClass != modelCostPaid {
		t.Fatalf("model block missing: %+v", state)
	}
	if remaining != 123 || exhausted {
		t.Fatalf("model 14018 must not overwrite account balance: remaining=%v exhausted=%v", remaining, exhausted)
	}
}

func TestQuotaRecoveryClearsBlocksKeepsModelCooldown(t *testing.T) {
	until := time.Now().Add(time.Hour)
	acc := &Account{QuotaExhausted: true, ModelStates: map[string]*modelRuntimeState{
		"m": {CostClass: modelCostPaid, QuotaBlocked: true, CooldownUntil: until},
	}}
	accountMu.Lock()
	acc.QuotaExhausted = false
	for _, state := range acc.ModelStates {
		state.QuotaBlocked = false
		state.NextProbeAt = time.Time{}
	}
	state := acc.ModelStates["m"]
	accountMu.Unlock()
	if state.QuotaBlocked || !state.CooldownUntil.Equal(until) {
		t.Fatalf("quota recovery should clear only quota block: %+v", state)
	}
}

func TestIsModelRateLimited(t *testing.T) {
	if !isModelRateLimited(`{"code":6004,"msg":"您也可以切换其他模型继续使用"}`) {
		t.Fatal("6004 should be model-level rate limit")
	}
	if isModelRateLimited(`{"code":14018,"msg":"额度已用尽"}`) {
		t.Fatal("14018 should not be model rate limit")
	}
}

func TestIsQuotaExhausted(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   bool
	}{
		{429, `{"error":{"data":{"code":14018,"msg":"额度已用尽"}}}`, true},
		{429, `{"error":{"data":{"code":14018,"msg":"Credits exhausted"}}}`, true},
		{429, `{"code":6004,"msg":"rate limit"}`, false},
		{400, `{"code":14018,"msg":"额度已用尽"}`, true},
	}
	for _, tc := range cases {
		if got := isQuotaExhausted(tc.status, tc.body); got != tc.want {
			t.Errorf("isQuotaExhausted(%d, %q)=%v want %v", tc.status, tc.body, got, tc.want)
		}
	}
}

func TestIsAlreadyCheckedIn(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   bool
	}{
		{409, `{"code":10001,"msg":"今天已签到"}`, true},
		{400, `{"code":14001,"msg":"今日已签到"}`, true},
		{409, `{"msg":"Already checked in today"}`, true},
		{500, "connection reset", false},
		{400, `{"code":10002,"msg":"积分不足"}`, false},
	}
	for _, tc := range cases {
		if got := isAlreadyCheckedIn(tc.status, tc.body); got != tc.want {
			t.Errorf("isAlreadyCheckedIn(%d, %q)=%v want %v", tc.status, tc.body, got, tc.want)
		}
	}
}

func TestNextDailyCheckinUTC8(t *testing.T) {
	loc := time.FixedZone("test", 8*60*60)
	before := time.Date(2026, 9, 15, 8, 59, 0, 0, loc)
	if got := nextDailyCheckin(before); !got.Equal(time.Date(2026, 9, 15, 9, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))) {
		t.Fatalf("before 09:00 got %v", got)
	}
	after := time.Date(2026, 9, 15, 9, 1, 0, 0, loc)
	if got := nextDailyCheckin(after); !got.Equal(time.Date(2026, 9, 16, 9, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))) {
		t.Fatalf("after 09:00 got %v", got)
	}
}

func TestCheckinAccountCNAndSkipIntl(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/v2/billing/meter/daily-checkin" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-access" || r.Header.Get("X-User-Id") != "user-1" {
			t.Errorf("missing checkin auth headers")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"msg":"OK","data":{}}`))
	}))
	defer server.Close()

	oldOrigin := profileCN.Origin
	oldClient := cfg.HttpClient
	profileCN.Origin = server.URL
	cfg.HttpClient = server.Client()
	defer func() {
		profileCN.Origin = oldOrigin
		cfg.HttpClient = oldClient
	}()

	cn := &Account{Path: "cn.json", Auth: &StoredAuth{
		Edition: "cn",
		Auth:    StoredTokens{AccessToken: "test-access"},
		Account: StoredAccount{UID: "user-1"},
	}}
	result, err := checkinAccount(context.Background(), cn)
	if err != nil || result != "ok" || calls != 1 {
		t.Fatalf("cn checkin result=%q calls=%d err=%v", result, calls, err)
	}

	intl := &Account{Path: "intl.json", Auth: &StoredAuth{
		Edition: "intl",
		Auth:    StoredTokens{AccessToken: "test-access"},
	}}
	result, err = checkinAccount(context.Background(), intl)
	if err != nil || result != "global_skipped" || calls != 1 {
		t.Fatalf("intl should be skipped: result=%q calls=%d err=%v", result, calls, err)
	}
}

// 验证授权失效识别（401 / 403+业务信封 / invalid token / 登录过期等）。
//
// 403 的语义已拆分：无业务信封的 403（WAF 拦截页/空体）不是授权失效，
// 由 TestIsWafBlocked 覆盖；此处只保留带业务信封的 403。
func TestIsAuthFailure(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   bool
	}{
		{401, "{}", true},
		{403, `{"code":11140,"msg":"request illegal"}`, false}, // 业务 403 无失效文案
		{400, `{"msg":"invalid token"}`, true},
		{400, "unauthorized", true},
		{400, "登录已过期", true},
		{400, "登录失效，请重新登录", true},
		{400, "some other error", false},
		{429, "频率限制", false},
		{500, "{}", false},
	}
	for _, c := range cases {
		if got := isAuthFailure(c.status, c.body); got != c.want {
			t.Errorf("isAuthFailure(%d, %q) = %v, want %v", c.status, c.body, got, c.want)
		}
	}
}

// 验证 WAF 拦截识别（403 + 无业务信封）。
//
// 回归背景：修复前 isAuthFailure 对任何 403 都返回 true，导致 WAF 拦截页
// （HTML/空体）被误判为授权失效 → disableAccount → os.Remove(凭据文件)，
// 把有效期数月的凭据直接删掉。本测试锁定拆分后的语义。
func TestIsWafBlocked(t *testing.T) {
	wafHTML := `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8" />` +
		`<title>WAF Block Page</title></head><body></body></html>`
	cases := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"403+HTML拦截页", 403, wafHTML, true},
		{"403+空体", 403, "", true},
		{"403+纯文本", 403, "Forbidden", true},
		{"403+业务信封", 403, `{"code":11140,"msg":"request illegal"}`, false},
		{"403+仅msg字段", 403, `{"msg":"内容包含敏感信息"}`, false},
		{"401不是WAF", 401, wafHTML, false},
		{"429不是WAF", 429, "频率限制", false},
		{"200不是WAF", 200, wafHTML, false},
	}
	for _, c := range cases {
		if got := isWafBlocked(c.status, c.body); got != c.want {
			t.Errorf("%s: isWafBlocked(%d, %q) = %v, want %v", c.name, c.status, c.body, got, c.want)
		}
	}
}

// 验证 WAF 与授权失效互斥：同一响应不可能同时归入两类。
func TestWafAndAuthMutuallyExclusive(t *testing.T) {
	bodies := []string{
		"",
		"<!DOCTYPE html><title>WAF Block Page</title>",
		`{"code":11140,"msg":"request illegal"}`,
		`{"msg":"invalid token"}`,
		"unauthorized",
	}
	for _, status := range []int{401, 403} {
		for _, body := range bodies {
			waf := isWafBlocked(status, body)
			auth := isAuthFailure(status, body)
			if waf && auth {
				t.Errorf("status=%d body=%q 同时命中 WAF 与授权失效", status, body)
			}
		}
	}
}

// 验证请求级错误的判定：内容审核 / 参数畸形 / 上下文超限。
//
// 这三类的共同语义是「换任何账号都会复现」——账号健康，
// 罚账号只会白白消耗健康号的可用性。
func TestRequestLevelErrors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		check  func(int, string) bool
		want   bool
	}{
		{"内容审核-blocked by security policy", 400,
			`{"code":11128,"msg":"Your request was blocked by security policy"}`, isContentBlocked, true},
		{"内容审核-unapproved channel", 400,
			`{"msg":"unapproved channel"}`, isContentBlocked, true},
		{"内容审核-illegal api invocation", 400,
			`{"msg":"illegal api invocation"}`, isContentBlocked, true},
		{"内容审核-大小写不敏感", 400,
			`{"msg":"BLOCKED BY SECURITY POLICY"}`, isContentBlocked, true},
		{"内容审核-普通400不命中", 400,
			`{"msg":"some other error"}`, isContentBlocked, false},
		{"内容审核-2xx不判定", 200,
			`{"msg":"blocked by security policy"}`, isContentBlocked, false},

		{"参数畸形-Unmarshal失败", 400,
			`{"code":11101,"msg":"Unmarshal chat params failed"}`, isBadParams, true},
		{"参数畸形-仅code", 400,
			`{"code":11101}`, isBadParams, true},
		{"参数畸形-其他400", 400,
			`{"code":12345}`, isBadParams, false},

		{"超限-11115", 400,
			`{"code":11115,"msg":"prompt is too long"}`, isPromptTooLong, true},
		{"超限-字符串code", 400,
			`{"code":"11115"}`, isPromptTooLong, true},
		{"超限-仅文案", 413,
			`{"msg":"Prompt is too long"}`, isPromptTooLong, true},
		{"超限-429不判定", 429,
			`{"code":11115}`, isPromptTooLong, false},
		{"超限-5xx不判定", 500,
			`{"code":11115}`, isPromptTooLong, false},
	}
	for _, c := range cases {
		if got := c.check(c.status, c.body); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// 验证账号级故障判定（11140 request illegal / 14017 trial 未激活）。
func TestIsAccountFault(t *testing.T) {
	cases := []struct {
		body string
		want bool
	}{
		{`{"code":11140,"msg":"request illegal"}`, true},
		{`{"code":11140,"msg":"Request Illegal"}`, true},
		{`{"code":14017,"msg":"trial not activated"}`, true},
		{`{"msg":"The trial version is not yet activated"}`, true},
		// 11140 也承载模型级限流文案，不能按 code 判为账号故障
		{`{"code":11140,"msg":"The model provider is rate-limiting requests."}`, false},
		{`{"code":6004,"msg":"频率限制"}`, false},
	}
	for _, c := range cases {
		if got := isAccountFault(c.body); got != c.want {
			t.Errorf("isAccountFault(%q) = %v, want %v", c.body, got, c.want)
		}
	}
}

// 验证 classifyUpstream 的判定优先级与调度语义。
//
// 关键不变量：
//  1. 请求级错误不罚账号（punishesAccount=false）
//  2. promptTooLong 不轮转（换任何账号都会超限）
//  3. WAF 与 session_dead 分离（前者保留凭据，后者删除）
func TestClassifyUpstream(t *testing.T) {
	wafHTML := `<!DOCTYPE html><title>WAF Block Page</title>`
	cases := []struct {
		name   string
		status int
		body   string
		want   errKind
	}{
		{"WAF拦截页", 403, wafHTML, errWafBlock},
		{"WAF空体", 403, "", errWafBlock},
		{"401会话失效", 401, `{}`, errSessionDead},
		{"12153离线会话", 400, `{"code":12153,"msg":"Offline user session not found"}`, errSessionDead},
		{"invalid token文案", 400, `{"msg":"invalid token"}`, errSessionDead},
		{"11140账号故障", 403, `{"code":11140,"msg":"request illegal"}`, errAccountFault},
		{"14018余额耗尽", 429, `{"code":14018,"msg":"额度已用尽"}`, errHardCredit},
		{"14018非429", 400, `{"code":14018,"msg":"Credits exhausted"}`, errHardCredit},
		{"6004模型级限流", 400, `{"code":6004,"msg":"切换其他模型"}`, errModelBlocked},
		{"429账号级限流", 429, `{"msg":"频率限制"}`, errSoftRate},
		{"11102模型不存在", 400, `{"code":11102,"msg":"service info not found"}`, errModelBlocked},
		{"11115上下文超限", 400, `{"code":11115,"msg":"prompt is too long"}`, errPromptTooLong},
		{"内容审核", 400, `{"msg":"blocked by security policy"}`, errContentBlocked},
		{"参数畸形", 400, `{"code":11101,"msg":"Unmarshal chat params failed"}`, errBadParams},
		{"404偶发", 404, `{}`, errNotFound},
		{"500服务端", 500, `{}`, errServer},
		{"其他4xx", 400, `{"msg":"unknown"}`, errClient},
		{"200成功", 200, `{}`, errNone},
	}
	for _, c := range cases {
		if got := classifyUpstream(c.status, c.body); got != c.want {
			t.Errorf("%s: classifyUpstream(%d, %q) = %v, want %v",
				c.name, c.status, c.body, got, c.want)
		}
	}
}

// 验证调度语义：请求级错误不罚账号，promptTooLong 不轮转。
func TestErrKindDispatchSemantics(t *testing.T) {
	// 不罚账号的分类：请求的问题，换账号照样复现
	for _, k := range []errKind{errContentBlocked, errBadParams, errPromptTooLong} {
		if k.punishesAccount() {
			t.Errorf("%v 不应罚账号", k)
		}
	}
	// 其余分类都归咎账号（冷却或禁用）
	for _, k := range []errKind{errWafBlock, errSessionDead, errAccountFault, errHardCredit,
		errSoftRate, errModelBlocked, errNotFound, errServer, errClient} {
		if !k.punishesAccount() {
			t.Errorf("%v 应罚账号", k)
		}
	}
	// 只有 promptTooLong 不轮转
	if errPromptTooLong.rotatesAccount() {
		t.Error("promptTooLong 不应轮转")
	}
	for _, k := range []errKind{errWafBlock, errSessionDead, errContentBlocked, errClient} {
		if !k.rotatesAccount() {
			t.Errorf("%v 应轮转", k)
		}
	}
}

// 验证轮询跳过已失效（Disabled）账号
func TestNextAccountSkipsDisabled(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{
		{Path: "a.json", Auth: &StoredAuth{}, Disabled: true, DisabledReason: "revoked"},
		{Path: "b.json", Auth: &StoredAuth{}},
	}
	rrIndex = 0
	accountMu.Unlock()

	a, _ := nextAccount()
	if a.Path != "b.json" {
		t.Fatalf("expected b.json (only non-disabled), got %s", a.Path)
	}
	t.Logf("disabled skip works, selected %s", a.Path)
}

// 验证全部失效时返回错误并提示重新登录
func TestNextAccountAllDisabled(t *testing.T) {
	accountMu.Lock()
	accounts = []*Account{
		{Path: "a.json", Auth: &StoredAuth{}, Disabled: true, DisabledReason: "expired"},
		{Path: "b.json", Auth: &StoredAuth{}, Disabled: true, DisabledReason: "revoked"},
	}
	rrIndex = 0
	accountMu.Unlock()

	_, err := nextAccount()
	if err == nil {
		t.Fatal("expected error when all accounts disabled")
	}
	t.Logf("all-disabled error: %v", err)
}

// 验证 disableAccount：标记失效、删除凭据文件、写入失效标记文件
func TestDisableAccount(t *testing.T) {
	dir := t.TempDir()
	authPath := dir + "/workbuddy-test.json"
	// 写一个假的凭据文件
	if err := os.WriteFile(authPath, []byte(`{"auth":{"accessToken":"x"}}`), 0600); err != nil {
		t.Fatal(err)
	}

	acc := &Account{Path: authPath, Auth: &StoredAuth{
		Auth:    StoredTokens{AccessToken: "x"},
		Account: StoredAccount{Nickname: "tester", UID: "uid-1"},
	}}

	disableAccount(acc, "令牌刷新失败 (HTTP 401): invalid token")

	accountMu.Lock()
	disabled := acc.Disabled
	reason := acc.DisabledReason
	accountMu.Unlock()
	if !disabled {
		t.Fatal("account should be marked disabled")
	}
	if reason == "" {
		t.Fatal("disabled reason should be recorded")
	}
	// 凭据文件应被删除
	if _, err := os.Stat(authPath); !os.IsNotExist(err) {
		t.Fatalf("credential file should be deleted, stat err=%v", err)
	}
	// 失效标记文件应存在
	if _, err := os.Stat(markerPath(authPath)); err != nil {
		t.Fatalf("marker file should exist: %v", err)
	}
	t.Logf("disableAccount works: disabled=%v reason=%q", disabled, reason)
}

// 验证失效标记文件可被恢复为失效账号（Auth 为 nil），并提示重新登录
func TestLoadDisabledMarkers(t *testing.T) {
	dir := t.TempDir()
	authPath := dir + "/workbuddy-test.json"

	// 构造失效标记文件
	marker := disabledMarker{
		Path:       authPath,
		Reason:     "授权失效（令牌刷新失败 HTTP 401）",
		DisabledAt: time.Now().Unix(),
		Nickname:   "tester",
		UID:        "uid-1",
	}
	data, _ := json.Marshal(marker)
	if err := os.WriteFile(markerPath(authPath), data, 0600); err != nil {
		t.Fatal(err)
	}

	// 模拟 -auth 显式指定该路径
	oldAuthFile := cfg.AuthFile
	oldAuthDir := cfg.AuthDir
	oldAuthExplicit := cfg.AuthExplicit
	defer func() {
		cfg.AuthFile = oldAuthFile
		cfg.AuthDir = oldAuthDir
		cfg.AuthExplicit = oldAuthExplicit
	}()
	cfg.AuthFile = authPath
	cfg.AuthDir = ""
	cfg.AuthExplicit = true

	accounts = nil
	loadDisabledMarkers()

	if len(accounts) != 1 {
		t.Fatalf("expected 1 disabled account, got %d", len(accounts))
	}
	acc := accounts[0]
	if !acc.Disabled || acc.Auth != nil {
		t.Fatalf("expected disabled account with nil Auth, got disabled=%v authNil=%v", acc.Disabled, acc.Auth == nil)
	}
	if acc.Nickname != "tester" || acc.UID != "uid-1" {
		t.Fatalf("marker nickname/uid not restored: %+v", acc)
	}
	t.Logf("loadDisabledMarkers restores: %s (nickname=%s)", acc.Path, acc.Nickname)
}

// 验证登录成功后清除失效标记
func TestClearDisabledMarker(t *testing.T) {
	dir := t.TempDir()
	authPath := dir + "/workbuddy-test.json"
	marker := disabledMarker{Path: authPath, Reason: "revoked"}
	data, _ := json.Marshal(marker)
	if err := os.WriteFile(markerPath(authPath), data, 0600); err != nil {
		t.Fatal(err)
	}

	clearDisabledMarker(authPath)
	if _, err := os.Stat(markerPath(authPath)); !os.IsNotExist(err) {
		t.Fatalf("marker file should be removed, stat err=%v", err)
	}
	t.Log("clearDisabledMarker works")
}

// 验证自动发现模式：未指定 -auth/-auth-dir 时，扫描当前目录下所有 workbuddy*.json
func TestCollectConfiguredAuthPathsAutoDiscover(t *testing.T) {
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)

	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	// 模拟目录内多个凭据文件 + 非凭据文件 + 失效标记 + 运行时状态快照（应被排除）
	for _, name := range []string{"workbuddy.json", "workbuddy2.json", "workbuddy-3.json", "other.json", "workbuddy.json.disabled", "workbuddy-status.json"} {
		if err := os.WriteFile(name, []byte(`{"auth":{"accessToken":"x"}}`), 0600); err != nil {
			t.Fatal(err)
		}
	}

	oldAuthFile := cfg.AuthFile
	oldAuthDir := cfg.AuthDir
	oldAuthExplicit := cfg.AuthExplicit
	defer func() {
		cfg.AuthFile = oldAuthFile
		cfg.AuthDir = oldAuthDir
		cfg.AuthExplicit = oldAuthExplicit
	}()
	cfg.AuthFile = "workbuddy.json"
	cfg.AuthDir = ""
	cfg.AuthExplicit = false

	paths := collectConfiguredAuthPaths()
	if len(paths) != 3 {
		t.Fatalf("expected 3 workbuddy json files, got %d: %v", len(paths), paths)
	}
	want := []string{"workbuddy-3.json", "workbuddy.json", "workbuddy2.json"} // sort.Strings 排序结果
	for i, p := range want {
		if paths[i] != p {
			t.Fatalf("paths[%d] = %s, want %s (all: %v)", i, paths[i], p, paths)
		}
	}
	t.Logf("auto-discover paths: %v", paths)
}

// 验证自动发现模式：目录内没有任何凭据文件时回退到默认路径
func TestCollectConfiguredAuthPathsAutoDiscoverFallback(t *testing.T) {
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)

	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	// 目录为空

	oldAuthFile := cfg.AuthFile
	oldAuthDir := cfg.AuthDir
	oldAuthExplicit := cfg.AuthExplicit
	defer func() {
		cfg.AuthFile = oldAuthFile
		cfg.AuthDir = oldAuthDir
		cfg.AuthExplicit = oldAuthExplicit
	}()
	cfg.AuthFile = "workbuddy.json"
	cfg.AuthDir = ""
	cfg.AuthExplicit = false

	paths := collectConfiguredAuthPaths()
	if len(paths) != 1 || paths[0] != "workbuddy.json" {
		t.Fatalf("expected fallback to default workbuddy.json, got %v", paths)
	}
	t.Logf("fallback path: %v", paths)
}

// 验证凭据热加载：新增 / 更新 / 删除凭据文件均原地收敛账号池，无需重启
func TestReloadAccounts(t *testing.T) {
	dir := t.TempDir()

	oldAuthFile := cfg.AuthFile
	oldAuthDir := cfg.AuthDir
	oldAuthExplicit := cfg.AuthExplicit
	defer func() {
		cfg.AuthFile = oldAuthFile
		cfg.AuthDir = oldAuthDir
		cfg.AuthExplicit = oldAuthExplicit
		accountMu.Lock()
		accounts = nil
		rrIndex = 0
		accountMu.Unlock()
	}()
	cfg.AuthFile = "workbuddy.json"
	cfg.AuthDir = dir
	cfg.AuthExplicit = false

	accountMu.Lock()
	accounts = nil
	rrIndex = 0
	accountMu.Unlock()

	pa := filepath.Join(dir, "workbuddy-a.json")

	// 1) 新增凭据文件 → 自动入池
	if err := os.WriteFile(pa, []byte(`{"auth":{"accessToken":"a1"},"account":{"nickname":"A"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if !reloadAccounts() {
		t.Fatal("expected changed=true after adding new credential file")
	}
	if len(accounts) != 1 || accounts[0].Auth.Auth.AccessToken != "a1" {
		t.Fatalf("expected 1 account with a1, got %+v", accounts)
	}
	t.Logf("hot-add works: %s joined the pool", accounts[0].Path)

	// 2) 内容变更（含站点切换 cn -> intl）→ 原地替换凭据
	if err := os.WriteFile(pa, []byte(`{"auth":{"accessToken":"a2-longer"},"account":{"nickname":"A2"},"edition":"intl"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if !reloadAccounts() {
		t.Fatal("expected changed=true after credential content change")
	}
	if len(accounts) != 1 || accounts[0].Auth.Auth.AccessToken != "a2-longer" {
		t.Fatalf("expected hot-reloaded a2 credential, got %+v", accounts)
	}
	if accounts[0].Profile().Key != "intl" {
		t.Fatalf("expected intl profile after reload, got %s", accounts[0].Profile().Key)
	}
	t.Logf("hot-update works: edition now %s", accounts[0].Profile().Key)

	// 3) 失效幻影账号：文件被删但账号 Disabled → 保留（用于提示重新登录）
	accountMu.Lock()
	accounts[0].Disabled = true
	accountMu.Unlock()
	if err := os.Remove(pa); err != nil {
		t.Fatal(err)
	}
	if reloadAccounts() {
		t.Fatal("disabled phantom account should be kept, expected no change")
	}
	if len(accounts) != 1 || !accounts[0].Disabled {
		t.Fatalf("disabled phantom should be kept, got %d accounts", len(accounts))
	}
	t.Log("disabled phantom kept after file deletion")

	// 4) 非失效账号文件被删 → 移出账号池
	accountMu.Lock()
	accounts[0].Disabled = false
	accounts[0].Auth = &StoredAuth{Auth: StoredTokens{AccessToken: "x"}}
	accountMu.Unlock()
	if !reloadAccounts() {
		t.Fatal("expected changed=true after credential file deletion")
	}
	if len(accounts) != 0 {
		t.Fatalf("expected empty pool after deletion, got %d", len(accounts))
	}
	t.Log("hot-remove works: pool emptied after file deletion")
}

// 验证 tailLines 读取文件末尾 N 行
func TestTailLines(t *testing.T) {
	dir := t.TempDir()
	f := dir + "/test.log"
	content := "line1\nline2\nline3\nline4\nline5\n"
	if err := os.WriteFile(f, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	lines, err := tailLines(f, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 || lines[0] != "line4" || lines[1] != "line5" {
		t.Fatalf("tailLines got %v", lines)
	}
	t.Logf("tailLines(2) = %v", lines)
}

// 验证状态快照写入：含 active / cooldown / disabled 三种状态
func TestWriteStatusSnapshot(t *testing.T) {
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	accountMu.Lock()
	accounts = []*Account{
		{Path: "a.json", Auth: &StoredAuth{
			Auth:    StoredTokens{AccessToken: "x", ExpiresAt: time.Now().Add(time.Hour).Unix()},
			Account: StoredAccount{Nickname: "alice", UID: "u1"},
		}, QuotaTotal: 2000, QuotaUsed: 1500, QuotaRemaining: 500, IsPaidUser: true, QuotaKnown: true},
		{Path: "b.json", Auth: &StoredAuth{
			Auth:    StoredTokens{AccessToken: "x", ExpiresAt: time.Now().Add(time.Hour).Unix()},
			Account: StoredAccount{Nickname: "bob", UID: "u2"},
		}, CooldownUntil: time.Now().Add(30 * time.Minute), CooldownMsg: "频率限制"},
		{Path: "c.json", Disabled: true, DisabledReason: "revoked", Nickname: "carol", UID: "u3"},
	}
	accountMu.Unlock()

	writeStatusSnapshot()

	data, err := os.ReadFile(statusSnapshotFile)
	if err != nil {
		t.Fatal(err)
	}
	var snap statusSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Accounts) != 3 {
		t.Fatalf("expected 3 accounts in snapshot, got %d", len(snap.Accounts))
	}
	states := map[string]bool{}
	for _, a := range snap.Accounts {
		states[a.State] = true
	}
	if !states["active"] || !states["cooldown"] || !states["disabled"] {
		t.Fatalf("expected all three states in snapshot, got %v", states)
	}
	if snap.Accounts[0].QuotaTotal != 2000 || snap.Accounts[0].QuotaUsed != 1500 || snap.Accounts[0].QuotaRemaining != 500 || !snap.Accounts[0].IsPaidUser || !snap.Accounts[0].QuotaKnown {
		t.Fatalf("quota fields not preserved in snapshot: %+v", snap.Accounts[0])
	}
	t.Logf("snapshot states: %v", states)
}

func TestWriteStatusSnapshotMarksExpiredToken(t *testing.T) {
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	accountMu.Lock()
	oldAccounts := accounts
	accounts = []*Account{{Path: "expired.json", Auth: &StoredAuth{
		Auth:    StoredTokens{AccessToken: "x", ExpiresAt: time.Now().Add(-time.Hour).Unix()},
		Account: StoredAccount{Nickname: "expired-user"},
	}}}
	accountMu.Unlock()
	defer func() {
		accountMu.Lock()
		accounts = oldAccounts
		accountMu.Unlock()
	}()

	writeStatusSnapshot()
	data, err := os.ReadFile(statusSnapshotFile)
	if err != nil {
		t.Fatal(err)
	}
	var snap statusSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Accounts) != 1 || snap.Accounts[0].State != "expired" {
		t.Fatalf("expected expired state, got %+v", snap.Accounts)
	}
}

func TestRenderAccountTableUsesFullYearAndNoEmoji(t *testing.T) {
	expires := time.Date(2027, 9, 5, 1, 36, 55, 0, time.Local)
	table := renderAccountTable([]accountSnapshot{{
		Path:           "workbuddy4.json",
		Edition:        "intl",
		Nickname:       "user@example.com",
		State:          "paid_exhausted",
		TokenExpiresAt: expires.Unix(),
		QuotaTotal:     1100,
		QuotaUsed:      1100,
		QuotaRemaining: 0,
		IsPaidUser:     false,
		QuotaKnown:     true,
		QuotaExhausted: true,
		FreeModels:     1,
		ModelCooldowns: 2,
	}})
	for _, want := range []string{"凭据文件", "workbuddy4.json", "国际站", "付费耗尽", "2027-09-05 01:36:55", "总额度", "已用", "剩余", "付费用户", "免费模型", "模型冷却", "1100", "否"} {
		if !strings.Contains(table, want) {
			t.Fatalf("table missing %q:\n%s", want, table)
		}
	}
	if strings.Contains(table, "说明") {
		t.Fatalf("table should not contain removed description column:\n%s", table)
	}
	if strings.ContainsAny(table, "✅🔒❌🕐📊📜⚠️") {
		t.Fatalf("table should not contain emoji:\n%s", table)
	}
}

func TestUsageCreditAndObserveModelCost(t *testing.T) {
	for _, tc := range []struct {
		usage map[string]any
		want  float64
		ok    bool
	}{
		{map[string]any{"credit": float64(0)}, 0, true},
		{map[string]any{"credit": "1.25"}, 1.25, true},
		{map[string]any{"total_tokens": 1}, 0, false},
		{nil, 0, false},
	} {
		got, ok := usageCredit(tc.usage)
		if got != tc.want || ok != tc.ok {
			t.Errorf("usageCredit(%v)=%v/%v want %v/%v", tc.usage, got, ok, tc.want, tc.ok)
		}
	}

	oldDir, _ := os.Getwd()
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)
	acc := &Account{Path: "empty.json", Auth: &StoredAuth{}, QuotaExhausted: true}
	accountMu.Lock()
	oldAccounts := accounts
	accounts = []*Account{acc}
	accountMu.Unlock()
	defer func() {
		accountMu.Lock()
		accounts = oldAccounts
		accountMu.Unlock()
	}()
	var logBuf bytes.Buffer
	oldLogWriter := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(oldLogWriter)
	// 样本过小：credit=0 但 total_tokens 太低，不得判定为免费
	observeModelCredit(acc, "tiny-model", map[string]any{"credit": float64(0), "total_tokens": float64(2)}, 10)
	accountMu.Lock()
	tiny := acc.ModelStates["tiny-model"]
	accountMu.Unlock()
	if tiny != nil && tiny.CostClass == modelCostFree {
		t.Fatalf("tiny sample must not be learned as free: %+v", tiny)
	}

	// 样本充足：credit=0 且 total_tokens 达标，判定为免费
	observeModelCredit(acc, "free-model", map[string]any{"credit": float64(0), "total_tokens": float64(500)}, 1)
	accountMu.Lock()
	state := acc.ModelStates["free-model"]
	accountMu.Unlock()
	if state == nil || state.CostClass != modelCostFree || state.QuotaBlocked {
		t.Fatalf("free model not learned: %+v", state)
	}
	if text := logBuf.String(); !strings.Contains(text, "[FreeModel]") || !strings.Contains(text, "付费余额耗尽账号 empty.json") {
		t.Fatalf("missing explicit free model exhausted-account log: %s", text)
	}
	observeModelCredit(acc, "paid-model", map[string]any{"credit": 2.5}, 2)
	accountMu.Lock()
	paid := acc.ModelStates["paid-model"]
	accountMu.Unlock()
	if paid == nil || paid.CostClass != modelCostPaid {
		t.Fatalf("paid model not learned: %+v", paid)
	}
}

func TestKnownFreeModelUsesExhaustedAccountAndLogs(t *testing.T) {
	chdirTemp(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"credit\":0,\"total_tokens\":500}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	oldBase, oldOrigin := profileCN.Base, profileCN.Origin
	oldClient := cfg.HttpClient
	accountMu.Lock()
	oldAccounts, oldRR := accounts, rrIndex
	acc := &Account{Path: "empty-free.json", Auth: &StoredAuth{Edition: "cn", Auth: StoredTokens{AccessToken: "x", ExpiresAt: time.Now().Add(time.Hour).Unix()}}, QuotaExhausted: true,
		ModelStates: map[string]*modelRuntimeState{"free-model": {CostClass: modelCostFree}}}
	accounts, rrIndex = []*Account{acc}, 0
	accountMu.Unlock()
	profileCN.Base, profileCN.Origin = server.URL, server.URL
	cfg.HttpClient = server.Client()
	defer func() {
		profileCN.Base, profileCN.Origin = oldBase, oldOrigin
		cfg.HttpClient = oldClient
		accountMu.Lock()
		accounts, rrIndex = oldAccounts, oldRR
		accountMu.Unlock()
	}()

	var logs bytes.Buffer
	oldWriter := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(oldWriter)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"free-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	handleChatCompletions(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	text := logs.String()
	for _, want := range []string{
		"请求的是已知免费模型 free-model，选择付费余额耗尽账号 empty-free.json 发起请求",
		"请求的是免费模型 free-model，已明确使用付费余额耗尽账号 empty-free.json 完成请求，usage.credit=0",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing log %q:\n%s", want, text)
		}
	}
}

func TestUnknownModelQuotaProbeStopsAfterOneExhaustedAccount(t *testing.T) {
	chdirTemp(t)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"data":{"code":14018,"msg":"额度已用尽"}}}`)
	}))
	defer server.Close()

	oldBase, oldOrigin := profileCN.Base, profileCN.Origin
	oldClient := cfg.HttpClient
	accountMu.Lock()
	oldAccounts, oldRR := accounts, rrIndex
	a := &Account{Path: "a.json", Auth: &StoredAuth{Edition: "cn", Auth: StoredTokens{AccessToken: "a", ExpiresAt: time.Now().Add(time.Hour).Unix()}}, QuotaExhausted: true}
	b := &Account{Path: "b.json", Auth: &StoredAuth{Edition: "cn", Auth: StoredTokens{AccessToken: "b", ExpiresAt: time.Now().Add(time.Hour).Unix()}}, QuotaExhausted: true}
	accounts, rrIndex = []*Account{a, b}, 0
	accountMu.Unlock()
	profileCN.Base, profileCN.Origin = server.URL, server.URL
	cfg.HttpClient = server.Client()
	defer func() {
		profileCN.Base, profileCN.Origin = oldBase, oldOrigin
		cfg.HttpClient = oldClient
		accountMu.Lock()
		accounts, rrIndex = oldAccounts, oldRR
		accountMu.Unlock()
	}()

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"unknown-paid","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	handleChatCompletions(rec, req)
	if rec.Code != http.StatusServiceUnavailable || calls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, calls, rec.Body.String())
	}
	accountMu.Lock()
	blockedA := a.ModelStates["unknown-paid"] != nil && a.ModelStates["unknown-paid"].QuotaBlocked
	_, touchedB := b.ModelStates["unknown-paid"]
	accountMu.Unlock()
	if !blockedA || touchedB {
		t.Fatalf("expected only first exhausted account probed: blockedA=%v touchedB=%v", blockedA, touchedB)
	}
}

func TestParseQuotaSummary(t *testing.T) {
	data := []byte(`{"Packages":[{"CycleTotalCapacity":"1500","CycleUsedCapacity":"69.98999993","CycleRemainCapacity":"1430.01000007"},{"CycleTotalCapacity":"500","CycleUsedCapacity":"500","CycleRemainCapacity":"0"}],"IsPaidUser":true}`)
	total, used, remaining, paid, err := parseQuotaSummary(data)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2000 || used != 569.98999993 || remaining != 1430.01000007 || !paid {
		t.Fatalf("unexpected quota summary: total=%v used=%v remaining=%v paid=%v", total, used, remaining, paid)
	}
}

func TestLockAccountWithContextTimeout(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if lockAccountWithContext(ctx, &mu) {
		t.Fatal("lock should time out while mutex is held")
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Fatalf("unexpected timeout duration: %v", elapsed)
	}
}

func TestFormatQuotaRoundsToTwoDecimals(t *testing.T) {
	cases := map[float64]string{
		4566:               "4566",
		72.01999992:        "72.02",
		4493.9800000800005: "4493.98",
		0.001:              "0",
	}
	for input, want := range cases {
		if got := formatQuota(input); got != want {
			t.Errorf("formatQuota(%v)=%q want %q", input, got, want)
		}
	}
}

func TestRenderAccountTableRowsHaveEqualDisplayWidth(t *testing.T) {
	table := renderAccountTable([]accountSnapshot{
		{Path: "workbuddy1.json", Nickname: "user-a", Edition: "cn", State: "quota_exhausted", QuotaKnown: true, QuotaTotal: 2000, QuotaUsed: 2000},
		{Path: "workbuddy2.json", Nickname: "user-b", Edition: "cn", State: "active", QuotaKnown: true, QuotaTotal: 2000, QuotaUsed: 69.98999993, QuotaRemaining: 1930.01000007},
	})
	lines := strings.Split(table, "\n")
	want := displayWidth(lines[0])
	for i, line := range lines {
		if got := displayWidth(line); got != want {
			t.Fatalf("line %d display width=%d want=%d:\n%s", i, got, want, table)
		}
	}
}

// 构造 messages 便于表驱动测试
func msg(role, content string) map[string]any {
	return map[string]any{"role": role, "content": content}
}

// 提取 messages 各条 role，便于断言
func rolesOf(obj map[string]any) []string {
	messages, _ := obj["messages"].([]any)
	roles := make([]string, 0, len(messages))
	for _, m := range messages {
		roles = append(roles, roleOfMessage(m))
	}
	return roles
}

// 验证会话结构归一化：修复上游 11128「first message is not system prompt」
func TestEnsureLeadingSystemMessage(t *testing.T) {
	cases := []struct {
		name       string
		messages   []any
		wantRoles  []string
		wantInject bool // 首条是否应为注入的保底 system
	}{
		{
			name:      "首条已是 system 保持原样",
			messages:  []any{msg("system", "You are helpful"), msg("user", "hi")},
			wantRoles: []string{"system", "user"},
		},
		{
			name:       "首条 user 且无 system（国内站宽容/国际站必须，统一注入 system）",
			messages:   []any{msg("user", "hi")},
			wantRoles:  []string{"system", "user"},
			wantInject: true,
		},
		{
			name:       "首条 assistant（续写）注入 system",
			messages:   []any{msg("assistant", "Sure")},
			wantRoles:  []string{"system", "assistant"},
			wantInject: true,
		},
		{
			name:       "首条 tool（仅回传工具结果）注入 system",
			messages:   []any{msg("tool", "result")},
			wantRoles:  []string{"system", "tool"},
			wantInject: true,
		},
		{
			name:      "后续 system 提升到首位",
			messages:  []any{msg("user", "hi"), msg("system", "You are helpful"), msg("user", "bye")},
			wantRoles: []string{"system", "user", "user"},
		},
		{
			name:      "首条 developer 归一化为 system",
			messages:  []any{msg("developer", "You are helpful"), msg("user", "hi")},
			wantRoles: []string{"system", "user"},
		},
		{
			name:       "空 messages 注入 system",
			messages:   []any{},
			wantRoles:  []string{"system"},
			wantInject: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := map[string]any{"messages": c.messages}
			ensureLeadingSystemMessage(obj)
			got := rolesOf(obj)
			if len(got) != len(c.wantRoles) {
				t.Fatalf("roles = %v, want %v", got, c.wantRoles)
			}
			for i := range got {
				if got[i] != c.wantRoles[i] {
					t.Fatalf("roles = %v, want %v", got, c.wantRoles)
				}
			}
			messages, _ := obj["messages"].([]any)
			first, _ := messages[0].(map[string]any)
			if c.wantInject {
				if content, _ := first["content"].(string); content != defaultSystemPrompt {
					t.Fatalf("injected system content = %q, want %q", content, defaultSystemPrompt)
				}
			}
			t.Logf("roles -> %v", got)
		})
	}
}

// 验证缺失 / 非法 messages 字段时也能安全注入（不得 panic）
func TestEnsureLeadingSystemMessageMissingField(t *testing.T) {
	obj := map[string]any{}
	ensureLeadingSystemMessage(obj)
	messages, ok := obj["messages"].([]any)
	if !ok || len(messages) != 1 {
		t.Fatalf("expected 1 injected message, got %#v", obj["messages"])
	}
	if roleOfMessage(messages[0]) != "system" {
		t.Fatalf("expected system first, got %v", roleOfMessage(messages[0]))
	}
	t.Log("missing messages field handled")
}

// 验证 developer 角色（GPT-5/Codex）被归一化为 system，避免上游 11128
// "Illegal API invocation from an unapproved channel"
func TestSanitizeMessagesNormalizesDeveloperRole(t *testing.T) {
	obj := map[string]any{"messages": []any{
		msg("system", "You are helpful."),
		msg("developer", "Be terse."),
		msg("user", "hi"),
	}}
	sanitizeMessages(obj)
	roles := rolesOf(obj)
	for _, r := range roles {
		if r == "developer" {
			t.Fatalf("developer role should be normalized, got %v", roles)
		}
	}
	if roles[1] != "system" {
		t.Fatalf("roles = %v, want second = system", roles)
	}
	t.Logf("roles normalized: %v", roles)
}

// 验证上游 tool_calls 增量按 index 正确归并为一个完整工具调用
func TestApplyToolCallDeltaMerge(t *testing.T) {
	merged := map[int]*mergedToolCall{}
	var order []int
	applyToolCallDelta(merged, &order, []any{
		map[string]any{"index": float64(0), "id": "call_1", "type": "function",
			"function": map[string]any{"name": "get_weather", "arguments": ""}},
	})
	applyToolCallDelta(merged, &order, []any{
		map[string]any{"index": float64(0), "function": map[string]any{"arguments": `{"city":`}},
	})
	applyToolCallDelta(merged, &order, []any{
		map[string]any{"index": float64(0), "function": map[string]any{"arguments": `"Beijing"}`}},
	})
	if len(order) != 1 || order[0] != 0 {
		t.Fatalf("order = %v, want [0]", order)
	}
	st := merged[0]
	if st.ID != "call_1" || st.Name != "get_weather" {
		t.Fatalf("id/name = %q/%q", st.ID, st.Name)
	}
	if got := st.Args.String(); got != `{"city":"Beijing"}` {
		t.Fatalf("args = %q", got)
	}
	t.Logf("merged tool call: %s(%s) id=%s", st.Name, st.Args.String(), st.ID)
}

// 验证 aggregateCompletion 正确合并流式 tool_calls（修复旧的按片追加问题）
func TestAggregateCompletionToolCalls(t *testing.T) {
	sse := strings.Join([]string{
		`data: {"id":"cmpl-1","model":"hy3-preview","created":1700000000,"choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":"}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Beijing\"}"}}]}}]}`,
		`data: [DONE]`,
	}, "\n")

	out, err := aggregateCompletion(strings.NewReader(sse), "hy3-preview")
	if err != nil {
		t.Fatal(err)
	}
	var chat map[string]any
	if err := json.Unmarshal(out, &chat); err != nil {
		t.Fatal(err)
	}
	choices := chat["choices"].([]any)
	msg := choices[0].(map[string]any)["message"].(map[string]any)
	calls, ok := msg["tool_calls"].([]any)
	if !ok || len(calls) != 1 {
		t.Fatalf("expected 1 merged tool_call, got %#v", msg["tool_calls"])
	}
	call := calls[0].(map[string]any)
	fn := call["function"].(map[string]any)
	if fn["name"] != "get_weather" || fn["arguments"] != `{"city":"Beijing"}` || call["id"] != "call_1" {
		t.Fatalf("bad merged call: %#v", call)
	}
	t.Logf("aggregateCompletion merged: %v", call)
}

// 验证 Responses -> Chat Completions 请求转换（instructions/input/tools/tool_choice/参数）
func TestResponsesToChatRequest(t *testing.T) {
	respReq := map[string]any{
		"model":             "hy3-preview",
		"instructions":      "You are helpful.",
		"max_output_tokens": float64(128),
		"temperature":       float64(0.3),
		"input": []any{
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "input_text", "text": "weather?"},
			}},
			map[string]any{"type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": `{"city":"BJ"}`},
			map[string]any{"type": "function_call_output", "call_id": "call_1", "output": "sunny"},
		},
		"tools": []any{map[string]any{
			"type": "function", "name": "get_weather", "description": "Get weather",
			"parameters": map[string]any{"type": "object"},
		}},
		"tool_choice": "auto",
		"reasoning":   map[string]any{"effort": "high"},
	}

	chat, err := responsesToChatRequest(respReq, "hy3-preview")
	if err != nil {
		t.Fatal(err)
	}
	if chat["model"] != "hy3-preview" || chat["max_tokens"] != float64(128) || chat["temperature"] != float64(0.3) {
		t.Fatalf("bad top-level fields: %#v", chat)
	}
	if chat["reasoning_effort"] != "high" {
		t.Fatalf("reasoning_effort = %v", chat["reasoning_effort"])
	}
	messages := chat["messages"].([]any)
	if len(messages) != 4 {
		t.Fatalf("expected 4 messages (system+user+assistant+tool), got %d: %#v", len(messages), messages)
	}
	if rolesOf(map[string]any{"messages": messages})[0] != "system" {
		t.Fatalf("first message should be system")
	}
	assistant := messages[2].(map[string]any)
	if assistant["role"] != "assistant" {
		t.Fatalf("3rd message role = %v", assistant["role"])
	}
	toolMsg := messages[3].(map[string]any)
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != "call_1" || toolMsg["content"] != "sunny" {
		t.Fatalf("tool message = %#v", toolMsg)
	}
	tools := chat["tools"].([]any)
	fn := tools[0].(map[string]any)["function"].(map[string]any)
	if fn["name"] != "get_weather" {
		t.Fatalf("tools not flattened: %#v", tools)
	}
	if chat["tool_choice"] != "auto" {
		t.Fatalf("tool_choice = %v", chat["tool_choice"])
	}
	t.Log("responses request converted OK")
}

// 验证 responsesToChatRequest 对纯字符串 input 的处理与空 input 报错
func TestResponsesToChatRequestStringInput(t *testing.T) {
	chat, err := responsesToChatRequest(map[string]any{"model": "x", "input": "hello"}, "x")
	if err != nil {
		t.Fatal(err)
	}
	messages := chat["messages"].([]any)
	if len(messages) != 1 || messages[0].(map[string]any)["content"] != "hello" {
		t.Fatalf("bad messages: %#v", messages)
	}
	if _, err := responsesToChatRequest(map[string]any{"model": "x"}, "x"); err == nil {
		t.Fatal("empty input should error")
	}
}

// 验证 chat.completion -> Responses 非流式响应对象转换
func TestChatCompletionToResponses(t *testing.T) {
	chat := map[string]any{
		"id": "cmpl-123", "created": float64(1700000000), "model": "hy3-preview",
		"choices": []any{map[string]any{"index": 0, "message": map[string]any{
			"role": "assistant", "content": "hello", "reasoning_content": "thinking",
			"tool_calls": []any{map[string]any{
				"id": "call_1", "type": "function",
				"function": map[string]any{"name": "get_weather", "arguments": `{"city":"BJ"}`},
			}},
		}}},
		"usage": map[string]any{
			"prompt_tokens": float64(10), "completion_tokens": float64(5), "total_tokens": float64(15),
			"prompt_tokens_details":     map[string]any{"cached_tokens": float64(2)},
			"completion_tokens_details": map[string]any{"reasoning_tokens": float64(3)},
		},
	}
	raw, _ := json.Marshal(chat)
	out, err := chatCompletionToResponses(raw, "hy3-preview")
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatal(err)
	}
	if result["object"] != "response" || result["status"] != "completed" {
		t.Fatalf("bad envelope: %#v", result)
	}
	output := result["output"].([]any)
	if len(output) != 3 {
		t.Fatalf("expected 3 output items (reasoning+message+function_call), got %d", len(output))
	}
	if output[0].(map[string]any)["type"] != "reasoning" {
		t.Fatalf("output[0] = %#v", output[0])
	}
	if output[1].(map[string]any)["type"] != "message" {
		t.Fatalf("output[1] = %#v", output[1])
	}
	fc := output[2].(map[string]any)
	if fc["type"] != "function_call" || fc["call_id"] != "call_1" || fc["name"] != "get_weather" {
		t.Fatalf("function_call item bad: %#v", fc)
	}
	usage := result["usage"].(map[string]any)
	if usage["input_tokens"] != float64(10) && usage["input_tokens"] != int64(10) {
		t.Fatalf("usage input = %#v", usage["input_tokens"])
	}
	if usage["total_tokens"] != float64(15) && usage["total_tokens"] != int64(15) {
		t.Fatalf("usage total = %#v", usage["total_tokens"])
	}
	t.Logf("responses object output items: %d", len(output))
}

// -----------------------------------------------------------------------------
// 指纹脱敏测试
//
// 回归背景：修复前 sanitizeBlockedTemplates 是空操作——两个 ReplaceAll 的
// 新旧参数完全相同，等于什么都没做，而测试只覆盖了 developer 角色归一化，
// 没有断言过净化结果。这些测试锁定净化后的实际输出。
// -----------------------------------------------------------------------------

// 验证模板句最小改写：破坏上游逐字匹配，语义不变。
func TestSanitizeTextRewritesTemplates(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"身份句-CLI版带句号",
			"You are Claude Code, Anthropic's official CLI for Claude. You help.",
			"You are Claude Code, Anthropic's official CLI tool for Claude. You help.",
		},
		{
			"身份句-桌面版接逗号",
			"You are Claude Code, Anthropic's official CLI for Claude, running within the SDK.",
			"You are Claude Code, Anthropic's official CLI tool for Claude, running within the SDK.",
		},
		{
			"分支句",
			"Default branch (you will usually use this for PRs)",
			"Default branch (you will usually use this for PRs)",
		},
		{
			"Codex身份句",
			"You are a coding agent running in the Codex CLI, a terminal-based coding assistant.",
			"You are a coding agent running in the Codex CLI tool, a terminal-based coding assistant.",
		},
		{
			"反馈句",
			"To give feedback, users should report the issue at https://github.com/anthropics/claude-code/issues",
			"To provide feedback, users should report the issue at https://github.com/anthropics/claude-code/issues",
		},
		{
			"裸数字11128",
			"the error code 11128 appeared",
			"the error code 11-128 appeared",
		},
	}
	for _, c := range cases {
		if got := sanitizeText(c.in); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.name, got, c.want)
		}
	}
}

// 验证 header 键值段被整段剥离，裸键名被缩写。
func TestSanitizeTextStripsHeaders(t *testing.T) {
	cases := []struct {
		name        string
		in          string
		mustNotHave []string
		mustHave    []string
	}{
		{
			"键值段整段删除",
			"prefix x-anthropic-billing-header: cc_version=1.0; cc_entrypoint=cli; suffix",
			[]string{"x-anthropic-billing-header:", "cc_version", "cc_entrypoint"},
			[]string{"prefix", "suffix"},
		},
		{
			"裸键名缩写保留可读性",
			"see `x-anthropic-billing-header` for details",
			[]string{"x-anthropic-billing-header"},
			[]string{"x-anthropic-billing-hdr", "for details"},
		},
		{
			"大小写变体同样处理",
			"X-Anthropic-Billing-Header: v=1;",
			[]string{"X-Anthropic-Billing-Header:"},
			[]string{},
		},
		{
			"尾随裸kv清理",
			// 实际流量中 cc_* 参数成对出现（cc_version + cc_entrypoint），
			// 预检靠 cc_entrypoint= 命中后，sanitizeKvRe 把所有 cc_ 前缀参数一并清掉。
			"text cc_version=1.2.3; cc_entrypoint=cli; more",
			[]string{"cc_version=1.2.3", "cc_entrypoint"},
			[]string{"text", "more"},
		},
	}
	for _, c := range cases {
		got := sanitizeText(c.in)
		for _, bad := range c.mustNotHave {
			if strings.Contains(got, bad) {
				t.Errorf("%s: 结果仍含 %q\n got %q", c.name, bad, got)
			}
		}
		for _, good := range c.mustHave {
			if !strings.Contains(got, good) {
				t.Errorf("%s: 结果缺少 %q\n got %q", c.name, good, got)
			}
		}
	}
}

// 验证无指纹文本原样返回（零分配快速路径不应改变内容）。
func TestSanitizeTextLeavesCleanTextUntouched(t *testing.T) {
	inputs := []string{
		"",
		"hello world",
		"write a quicksort in Go",
		"the code 11101 is unrelated",
		"Anthropic is a company",
	}
	for _, in := range inputs {
		if got := sanitizeText(in); got != in {
			t.Errorf("sanitizeText(%q) = %q, 应原样返回", in, got)
		}
	}
}

// 验证 reasoning_content 与 tool_calls.arguments 同样被净化。
//
// 这两处是长期盲区：工具调用轮的 content 通常为 null，
// 若在 content 缺失时跳过整条消息，工具参数里的指纹会原样漏出。
func TestSanitizeMessagesCoversReasoningAndToolCalls(t *testing.T) {
	obj := map[string]any{"messages": []any{
		map[string]any{
			"role":              "assistant",
			"content":           nil, // 工具调用轮的典型形态
			"reasoning_content": "I should follow You are Claude Code, Anthropic's official CLI for Claude.",
			"tool_calls": []any{
				map[string]any{
					"id":   "call_1",
					"type": "function",
					"function": map[string]any{
						"name":      "read_file",
						"arguments": `{"path":"You are Claude Code, Anthropic's official CLI for Claude."}`,
					},
				},
			},
		},
	}}
	sanitizeMessages(obj)

	m := obj["messages"].([]any)[0].(map[string]any)
	rc, _ := m["reasoning_content"].(string)
	if strings.Contains(rc, "official CLI for Claude") {
		t.Errorf("reasoning_content 未净化: %q", rc)
	}
	if !strings.Contains(rc, "official CLI tool for Claude") {
		t.Errorf("reasoning_content 改写结果不符: %q", rc)
	}

	tc := m["tool_calls"].([]any)[0].(map[string]any)
	args, _ := tc["function"].(map[string]any)["arguments"].(string)
	if strings.Contains(args, "official CLI for Claude") {
		t.Errorf("tool_calls.arguments 未净化: %q", args)
	}
	if !strings.Contains(args, "official CLI tool for Claude") {
		t.Errorf("tool_calls.arguments 改写结果不符: %q", args)
	}
}

// 验证多模态 content 数组只动 text part，其他 part 不受影响。
func TestSanitizeMessagesMultimodalContent(t *testing.T) {
	obj := map[string]any{"messages": []any{
		map[string]any{
			"role": "user",
			"content": []any{
				map[string]any{"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."},
				map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/png;base64,AAAA"}},
			},
		},
	}}
	sanitizeMessages(obj)

	parts := obj["messages"].([]any)[0].(map[string]any)["content"].([]any)
	text := parts[0].(map[string]any)["text"].(string)
	if !strings.Contains(text, "official CLI tool for Claude") {
		t.Errorf("text part 未净化: %q", text)
	}
	img := parts[1].(map[string]any)["image_url"].(map[string]any)["url"].(string)
	if img != "data:image/png;base64,AAAA" {
		t.Errorf("image part 被改动: %q", img)
	}
}

// -----------------------------------------------------------------------------
// 会话头族测试
// -----------------------------------------------------------------------------

// 验证 prompt_cache_key 注入：同账号同会话稳定、跨账号隔离、跨会话区分。
func TestInjectPromptCacheKeyStabilityAndIsolation(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	keyOf := func(t *testing.T, in []byte) string {
		t.Helper()
		var obj map[string]any
		if err := json.Unmarshal(injectPromptCacheKey(in, "uid-aaaaaaaa", "conv-1"), &obj); err != nil {
			t.Fatalf("注入后不可解析: %v", err)
		}
		k, _ := obj["prompt_cache_key"].(string)
		return k
	}

	k1 := keyOf(t, body)
	if k1 == "" {
		t.Fatal("应注入 prompt_cache_key")
	}
	// 同账号同会话：稳定
	if k2 := keyOf(t, body); k2 != k1 {
		t.Fatalf("同账号同会话应稳定: %q vs %q", k1, k2)
	}
	// 键格式：wbgw-<uid8>-<32hex>
	if !strings.HasPrefix(k1, "wbgw-uid-aaaa-") || len(k1) != len("wbgw-uid-aaaa-")+32 {
		t.Fatalf("键格式不符: %q", k1)
	}

	// 跨账号必须隔离——否则会命中他人前缀缓存、泄露对话内容
	var other map[string]any
	_ = json.Unmarshal(injectPromptCacheKey(body, "uid-bbbbbbbb", "conv-1"), &other)
	if other["prompt_cache_key"] == k1 {
		t.Fatal("不同账号必须生成不同键（跨账号缓存命中会泄露对话）")
	}
	// 同账号跨会话：区分
	var otherConv map[string]any
	_ = json.Unmarshal(injectPromptCacheKey(body, "uid-aaaaaaaa", "conv-2"), &otherConv)
	if otherConv["prompt_cache_key"] == k1 {
		t.Fatal("同账号不同会话应生成不同键")
	}
}

// 验证注入优先级与边界：客户端显式键不覆盖；body 内会话标识优先于入站参数；
// 空 body / 坏 JSON / 无 UID 时行为确定。
func TestInjectPromptCacheKeyPrecedenceAndEdges(t *testing.T) {
	// 客户端已显式带 key → 原值保留，且返回原切片（不做无谓重编码）
	explicit := []byte(`{"prompt_cache_key":"client-key","model":"m"}`)
	got := injectPromptCacheKey(explicit, "uid-aaaaaaaa", "conv-1")
	var obj map[string]any
	_ = json.Unmarshal(got, &obj)
	if obj["prompt_cache_key"] != "client-key" {
		t.Fatalf("不应覆盖客户端显式键: %v", obj["prompt_cache_key"])
	}

	// body 内 conversation_id 优先于入站参数
	inBody := []byte(`{"conversation_id":"from-body","model":"m"}`)
	var a, b map[string]any
	_ = json.Unmarshal(injectPromptCacheKey(inBody, "uid-aaaaaaaa", "from-arg"), &a)
	_ = json.Unmarshal(injectPromptCacheKey(inBody, "uid-aaaaaaaa", "from-body"), &b)
	if a["prompt_cache_key"] != b["prompt_cache_key"] {
		t.Fatalf("body 内会话标识应优先于入站参数: %v vs %v", a["prompt_cache_key"], b["prompt_cache_key"])
	}

	// 空 body 原样返回
	if got := injectPromptCacheKey(nil, "uid-aaaaaaaa", "c"); got != nil {
		t.Fatal("空 body 应原样返回")
	}
	// 坏 JSON 原样返回（不二次错误化）
	bad := []byte(`{not json`)
	if got := injectPromptCacheKey(bad, "uid-aaaaaaaa", "c"); string(got) != string(bad) {
		t.Fatal("坏 JSON 应原样返回")
	}
	// 空 UID：仍注入，但隔离段为占位符（保证跨账号不碰撞的语义退化可见）
	var noUID map[string]any
	_ = json.Unmarshal(injectPromptCacheKey([]byte(`{"model":"m"}`), "", "c"), &noUID)
	if k, _ := noUID["prompt_cache_key"].(string); !strings.HasPrefix(k, "wbgw---") {
		t.Fatalf("空 UID 应使用占位隔离段: %q", k)
	}
}

// 验证注入在真实请求链路上生效：上游收到的 body 带 prompt_cache_key，且按账号隔离。
func TestUpstreamChatInjectsPromptCacheKey(t *testing.T) {
	chdirTemp(t)
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"credit\":0,\"total_tokens\":500}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	oldBase, oldOrigin := profileCN.Base, profileCN.Origin
	oldClient := cfg.HttpClient
	accountMu.Lock()
	oldAccounts, oldRR := accounts, rrIndex
	acc := &Account{Path: "ck.json", Auth: &StoredAuth{Edition: "cn", Auth: StoredTokens{AccessToken: "x", ExpiresAt: time.Now().Add(time.Hour).Unix()},
		Account: StoredAccount{UID: "uid-aaaaaaaa"}}}
	accounts, rrIndex = []*Account{acc}, 0
	accountMu.Unlock()
	profileCN.Base, profileCN.Origin = server.URL, server.URL
	cfg.HttpClient = server.Client()
	defer func() {
		profileCN.Base, profileCN.Origin = oldBase, oldOrigin
		cfg.HttpClient = oldClient
		accountMu.Lock()
		accounts, rrIndex = oldAccounts, oldRR
		accountMu.Unlock()
	}()

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"conversation_id":"conv-xyz","messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	handleChatCompletions(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(bodies) != 1 {
		t.Fatalf("上游应收到 1 次请求, got %d", len(bodies))
	}
	var sent map[string]any
	if err := json.Unmarshal([]byte(bodies[0]), &sent); err != nil {
		t.Fatalf("上游收到的 body 不可解析: %v", err)
	}
	key, _ := sent["prompt_cache_key"].(string)
	if !strings.HasPrefix(key, "wbgw-uid-aaaa-") {
		t.Fatalf("上游未收到按账号隔离的 cache key: %q", key)
	}
	// 会话标识来自 body（conv-xyz），与直接调用同源应得同键
	var direct map[string]any
	_ = json.Unmarshal(injectPromptCacheKey([]byte(bodies[0]), "uid-aaaaaaaa", "conv-xyz"), &direct)
	if direct["prompt_cache_key"] != key {
		t.Fatalf("链路上的键应与同源直算一致: %v vs %v", key, direct["prompt_cache_key"])
	}
}

// 验证消息级 ID 形态：32 hex（对齐官方 X-Request-ID / X-Conversation-Message-ID）
func TestNewMessageID(t *testing.T) {
	seen := make(map[string]bool)
	for range 100 {
		id := newMessageID()
		if len(id) != 32 {
			t.Fatalf("newMessageID() = %q, 长度应为 32", id)
		}
		if !validB3TraceID(id) {
			t.Fatalf("newMessageID() = %q, 应为合法 hex", id)
		}
		if seen[id] {
			t.Fatalf("newMessageID() 重复生成 %q", id)
		}
		seen[id] = true
	}
}

// 验证会话键提取：四键按序尝试，缺失返回空串
func TestResolveConversationID(t *testing.T) {
	cases := []struct {
		name string
		obj  map[string]any
		want string
	}{
		{"metadata.snake_case", map[string]any{"metadata": map[string]any{"conversation_id": "c1"}}, "c1"},
		{"metadata.camelCase", map[string]any{"metadata": map[string]any{"conversationId": "c2"}}, "c2"},
		{"顶层snake_case", map[string]any{"conversation_id": "c3"}, "c3"},
		{"顶层camelCase", map[string]any{"conversationId": "c4"}, "c4"},
		{"snake优先于camel", map[string]any{
			"metadata":       map[string]any{"conversation_id": "a", "conversationId": "b"},
			"conversationId": "c",
		}, "a"},
		{"缺失", map[string]any{"model": "x"}, ""},
		{"空对象", map[string]any{}, ""},
		{"非字符串类型", map[string]any{"conversationId": 12345}, ""},
	}
	for _, c := range cases {
		if got := resolveConversationID(c.obj); got != c.want {
			t.Errorf("%s: resolveConversationID = %q, want %q", c.name, got, c.want)
		}
	}
}

// 验证 B3 TraceId 合法性判定：16/32 hex 合法，其余（含横线 UUID）非法
func TestValidB3TraceID(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"0123456789abcdef0123456789abcdef", true},      // 32 hex
		{"0123456789abcdef", true},                      // 16 hex
		{"0123456789ABCDEF0123456789ABCDEF", true},      // 大写
		{"550e8400-e29b-41d4-a716-446655440000", false}, // 带横线 UUID
		{"", false},
		{"short", false},
		{"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", false}, // 非 hex
	}
	for _, c := range cases {
		if got := validB3TraceID(c.in); got != c.want {
			t.Errorf("validB3TraceID(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// 验证 backendHeaders 注入完整的会话头族，且聚合主键与消息级 ID 各司其职。
func TestBackendHeadersConversationFamily(t *testing.T) {
	convReqID := newMessageID()
	req, _ := http.NewRequest(http.MethodPost, "https://example.com/v2/chat/completions", nil)
	backendHeaders(req, &StoredAuth{Edition: "cn", Auth: StoredTokens{AccessToken: "tok"}}, &profileCN, convReqID, newMessageID())

	// 聚合主键：两处头同值
	if got := req.Header.Get("X-Conversation-Request-ID"); got != convReqID {
		t.Errorf("X-Conversation-Request-ID = %q, want %q", got, convReqID)
	}
	if got := req.Header.Get("X-Root-Request-ID"); got != convReqID {
		t.Errorf("X-Root-Request-ID = %q, want %q", got, convReqID)
	}
	// B3 TraceId 用聚合主键（32 hex 合法）
	if got := req.Header.Get("X-B3-TraceId"); got != convReqID {
		t.Errorf("X-B3-TraceId = %q, want %q", got, convReqID)
	}
	// 消息级 ID 独立
	if got := req.Header.Get("X-Conversation-Message-ID"); got == convReqID {
		t.Errorf("X-Conversation-Message-ID 不应与聚合主键相同")
	}
	if got := req.Header.Get("X-B3-SpanId"); got != req.Header.Get("X-Conversation-Message-ID")[:16] {
		t.Errorf("X-B3-SpanId = %q, want messageID 前 16 位", got)
	}
	if got := req.Header.Get("X-B3-Sampled"); got != "1" {
		t.Errorf("X-B3-Sampled = %q, want 1", got)
	}
	// 消息级 ID 形态合法
	if !validB3TraceID(req.Header.Get("X-Request-ID")) {
		t.Errorf("X-Request-ID 非合法 hex: %q", req.Header.Get("X-Request-ID"))
	}
}

// 验证非法 B3 TraceId 时回落到消息级 ID（不破坏链路关联）。
func TestBackendHeadersB3Fallback(t *testing.T) {
	badID := "550e8400-e29b-41d4-a716-446655440000" // 带横线，非法
	req, _ := http.NewRequest(http.MethodPost, "https://example.com/v2/chat/completions", nil)
	backendHeaders(req, &StoredAuth{Edition: "cn", Auth: StoredTokens{AccessToken: "tok"}}, &profileCN, badID, newMessageID())

	if got := req.Header.Get("X-Conversation-Request-ID"); got != badID {
		// 主键照发（上游 B3 之外的头不做 hex 校验）
		t.Errorf("X-Conversation-Request-ID = %q, want %q", got, badID)
	}
	if got := req.Header.Get("X-B3-TraceId"); got == badID {
		t.Errorf("X-B3-TraceId 不应透传非法值")
	}
	if !validB3TraceID(req.Header.Get("X-B3-TraceId")) {
		t.Errorf("X-B3-TraceId 回落值非法: %q", req.Header.Get("X-B3-TraceId"))
	}
}

// -----------------------------------------------------------------------------
// 降级重试测试
// -----------------------------------------------------------------------------

// 验证 nextMidnightCST 的边界语义：恒返回未来时刻，且为 CST 零点。
func TestNextMidnightCST(t *testing.T) {
	cases := []struct {
		name string
		now  time.Time
	}{
		{"白天", time.Date(2026, 9, 17, 14, 0, 0, 0, time.UTC)},
		{"23:59 CST", time.Date(2026, 9, 17, 15, 59, 0, 0, time.UTC)}, // CST 23:59
		{"00:00 CST", time.Date(2026, 9, 17, 16, 0, 0, 0, time.UTC)},  // CST 00:00
		{"00:01 CST", time.Date(2026, 9, 17, 16, 1, 0, 0, time.UTC)},  // CST 00:01
	}
	cst := time.FixedZone("CST", 8*60*60)
	for _, c := range cases {
		got := nextMidnightCST(c.now)
		if !got.After(c.now) {
			t.Errorf("%s: %v 未晚于 now %v", c.name, got, c.now)
		}
		// 必须落在 CST 零点
		y, m, d := got.In(cst).Date()
		if got.In(cst).Hour() != 0 || got.In(cst).Minute() != 0 || got.In(cst).Second() != 0 {
			t.Errorf("%s: %v 非 CST 零点", c.name, got)
		}
		t.Logf("%s: now=%v -> %v (CST %d-%02d-%02d 00:00)", c.name, c.now.In(cst), got.In(cst), y, m, d)
	}
	// 24h 内必有下一次零点
	if nextMidnightCST(time.Now()).Sub(time.Now()) > 24*time.Hour {
		t.Error("next midnight 超过 24h")
	}
}

// 验证降级门状态机：触发后 Active，次日重置；未触发不续期。
func TestDegradeGate(t *testing.T) {
	var g degradeGate
	if g.Active() {
		t.Fatal("初始状态不应为降级期")
	}
	g.Trigger()
	if !g.Active() {
		t.Fatal("触发后应为降级期")
	}
	first := g.until
	time.Sleep(5 * time.Millisecond)
	g.Trigger() // 已在降级期内 → 不续期
	if !g.until.Equal(first) {
		t.Errorf("降级期内重复触发不应续期: first=%v second=%v", first, g.until)
	}
}

// 验证 promptReplaceSystem 替换所有 system/developer 并保留其余消息原序。
func TestPromptReplaceSystem(t *testing.T) {
	obj := map[string]any{"messages": []any{
		msg("system", "old system"),
		msg("developer", "old dev"),
		msg("user", "hi"),
		msg("assistant", "hello"),
		msg("user", "again"),
	}}
	promptReplaceSystem(obj, "gw prompt")

	msgs := obj["messages"].([]any)
	if len(msgs) != 4 {
		t.Fatalf("len = %d, want 4 (1 gw + user/assistant/user)", len(msgs))
	}
	if roleOfMessage(msgs[0]) != "system" {
		t.Fatalf("首条应为 system")
	}
	if c, _ := msgs[0].(map[string]any)["content"].(string); c != "gw prompt" {
		t.Errorf("首条 content = %q, want gw prompt", c)
	}
	// user/assistant 原序保留
	for i, want := range []string{"user", "assistant", "user"} {
		if got := roleOfMessage(msgs[i+1]); got != want {
			t.Errorf("msgs[%d].role = %q, want %q", i+1, got, want)
		}
	}
}

// 验证 promptAppendSystem 在开头连续 system 块之后插入，既有消息逐字不动。
func TestPromptAppendSystem(t *testing.T) {
	obj := map[string]any{"messages": []any{
		msg("system", "s1"),
		msg("developer", "s2"),
		msg("user", "hi"),
		msg("system", "mid-system"), // 中途 system 不属于「开头连续块」
		msg("user", "again"),
	}}
	promptAppendSystem(obj, "gw prompt")

	msgs := obj["messages"].([]any)
	// 期望：s1(system), s2(developer 保持原样——归一化由管线下游的
	// sanitizeMessages 负责，append 只做插入), gw, user, mid-system, user
	want := []struct{ role, content string }{
		{"system", "s1"}, {"developer", "s2"}, {"system", "gw prompt"},
		{"user", "hi"}, {"system", "mid-system"}, {"user", "again"},
	}
	if len(msgs) != len(want) {
		t.Fatalf("len = %d, want %d", len(msgs), len(want))
	}
	for i, w := range want {
		m := msgs[i].(map[string]any)
		if gotRole := roleOfMessage(m); gotRole != w.role {
			t.Errorf("msgs[%d].role = %q, want %q", i, gotRole, w.role)
		}
		if gotC, _ := m["content"].(string); gotC != w.content {
			t.Errorf("msgs[%d].content = %q, want %q", i, gotC, w.content)
		}
	}
}

// 验证 effectivePromptText 的回落逻辑。
func TestEffectivePromptText(t *testing.T) {
	oldMode, oldText := cfg.PromptMode, cfg.PromptText
	defer func() { cfg.PromptMode, cfg.PromptText = oldMode, oldText }()

	cfg.PromptText = ""
	if got := effectivePromptText(); got != degradedPrompt {
		t.Errorf("空 PromptText 应回落 degradedPrompt, got %q", got)
	}
	cfg.PromptText = "custom text"
	if got := effectivePromptText(); got != "custom text" {
		t.Errorf("got %q, want custom text", got)
	}
}

// 验证 rewriteSystemTo 对序列化 body 的替换，以及坏 JSON 的原样返回。
func TestRewriteSystemTo(t *testing.T) {
	body := `{"model":"m","messages":[{"role":"system","content":"old"},{"role":"user","content":"hi"}]}`
	got := rewriteSystemTo([]byte(body), "new system")
	var obj map[string]any
	if err := json.Unmarshal(got, &obj); err != nil {
		t.Fatalf("输出非合法 JSON: %v", err)
	}
	msgs := obj["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("len = %d, want 2", len(msgs))
	}
	first := msgs[0].(map[string]any)
	if c, _ := first["content"].(string); c != "new system" {
		t.Errorf("首条 = %q, want new system", c)
	}

	// 坏 JSON 原样返回
	bad := []byte("{broken")
	if got := rewriteSystemTo(bad, "x"); string(got) != string(bad) {
		t.Errorf("坏 JSON 应原样返回, got %q", got)
	}
}

// -----------------------------------------------------------------------------
// Phase 4：成本账本（EMA 单价 / TTL / 分层选号 / 条件探索 / 持久化 / 展示）
// -----------------------------------------------------------------------------

// 验证成本账本的 EMA 单价学习：首次观测直接落账，后续观测按 α=0.3 平滑。
func TestModelCostEMA(t *testing.T) {
	acc := &Account{Path: "ema.json", Auth: &StoredAuth{}}
	observeModelCredit(acc, "ema-model", map[string]any{"credit": 1.0, "total_tokens": 1000}, 1)
	accountMu.Lock()
	state := acc.ModelStates["ema-model"]
	first, samples := state.CostPer1k, state.CostSamples
	accountMu.Unlock()
	if samples != 1 || first <= 0 || first > 2 {
		t.Fatalf("首次观测 per1k=%v samples=%d，期望 ≈1.0/1", first, samples)
	}
	observeModelCredit(acc, "ema-model", map[string]any{"credit": 3.0, "total_tokens": 1000}, 2)
	accountMu.Lock()
	state = acc.ModelStates["ema-model"]
	second, samples := state.CostPer1k, state.CostSamples
	accountMu.Unlock()
	if samples != 2 || second <= first || second >= 3 {
		t.Fatalf("EMA 后 per1k=%v samples=%d，期望介于 %v 与 3 之间", second, samples, first)
	}
}

// 验证成本观测的 TTL：过期观测降回未知层（tier 1），不再按免费/收费偏置选号。
func TestModelCostTTLExpiry(t *testing.T) {
	now := time.Now()
	stale := &modelRuntimeState{CostClass: modelCostPaid, CostPer1k: 5, CostSamples: 2, ObservedAt: now.Add(-7 * time.Hour)}
	if tier := modelCostTier(stale, now); tier != 1 {
		t.Fatalf("过期收费观测 tier=%d want 1", tier)
	}
	fresh := &modelRuntimeState{CostClass: modelCostPaid, CostPer1k: 5, CostSamples: 2, ObservedAt: now.Add(-time.Hour)}
	if tier := modelCostTier(fresh, now); tier != 2 {
		t.Fatalf("新鲜收费观测 tier=%d want 2", tier)
	}
	staleFree := &modelRuntimeState{CostClass: modelCostFree, CostSamples: 1, ObservedAt: now.Add(-7 * time.Hour)}
	if tier := modelCostTier(staleFree, now); tier != 1 {
		t.Fatalf("过期免费观测 tier=%d want 1", tier)
	}
	if _, ok := modelCostPer1kOf(&Account{ModelStates: map[string]*modelRuntimeState{"m": stale}}, "m", now); ok {
		t.Fatal("过期观测不应透出单价")
	}
	// 零余额账号 + 过期收费观测：回到受控探测路径（而不是永久屏蔽）
	acc := &Account{QuotaExhausted: true, ModelStates: map[string]*modelRuntimeState{"m": stale}}
	if kind, ok := usableForModelLocked(acc, "m", now); !ok || kind != selectionProbeExhausted {
		t.Fatalf("过期观测应回到探测路径: kind=%s ok=%v", kind, ok)
	}
}

// 验证成本分层选号：免费层 > 未知层 > 收费层；收费层内单价低者优先。
func TestNextAccountForModelCostTierOrder(t *testing.T) {
	oldInterval := cfg.CostExploreInterval
	cfg.CostExploreInterval = 0
	defer func() { cfg.CostExploreInterval = oldInterval }()

	now := time.Now()
	paid := &Account{Path: "paid.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10, ModelStates: map[string]*modelRuntimeState{
		"m": {CostClass: modelCostPaid, CostPer1k: 5, CostSamples: 1, ObservedAt: now},
	}}
	paidCheap := &Account{Path: "paid-cheap.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10, ModelStates: map[string]*modelRuntimeState{
		"m": {CostClass: modelCostPaid, CostPer1k: 1, CostSamples: 1, ObservedAt: now},
	}}
	unknown := &Account{Path: "unknown.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10}
	free := &Account{Path: "free.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10, ModelStates: map[string]*modelRuntimeState{
		"m": {CostClass: modelCostFree, CostSamples: 1, ObservedAt: now},
	}}

	accountMu.Lock()
	accounts = []*Account{paid, unknown, free}
	rrIndex = 0
	accountMu.Unlock()
	for i := range 2 {
		acc, _, err := nextAccountForModel("m", nil)
		if err != nil || acc.Path != "free.json" {
			t.Fatalf("第 %d 次应选免费层账号: acc=%v err=%v", i+1, acc, err)
		}
	}

	accountMu.Lock()
	accounts = []*Account{paid, unknown}
	rrIndex = 0
	accountMu.Unlock()
	acc, _, err := nextAccountForModel("m", nil)
	if err != nil || acc.Path != "unknown.json" {
		t.Fatalf("免费层缺失时应选未知层账号: acc=%v err=%v", acc, err)
	}

	accountMu.Lock()
	accounts = []*Account{paid, paidCheap}
	rrIndex = 0
	accountMu.Unlock()
	acc, _, err = nextAccountForModel("m", nil)
	if err != nil || acc.Path != "paid-cheap.json" {
		t.Fatalf("只剩收费层时应选单价低者: acc=%v err=%v", acc, err)
	}
}

// 验证 costTier 条件探索：免费层垄断时按窗口改道一次给未知账号；窗口内不重复。
func TestCostExploreRedirectsToUnknown(t *testing.T) {
	oldInterval := cfg.CostExploreInterval
	oldLast := costExploreLast
	cfg.CostExploreInterval = time.Hour
	accountMu.Lock()
	costExploreLast = map[string]time.Time{}
	accountMu.Unlock()
	defer func() {
		cfg.CostExploreInterval = oldInterval
		accountMu.Lock()
		costExploreLast = oldLast
		accountMu.Unlock()
	}()

	free := &Account{Path: "free.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10, ModelStates: map[string]*modelRuntimeState{
		"m": {CostClass: modelCostFree, CostSamples: 1, ObservedAt: time.Now()},
	}}
	unknown := &Account{Path: "unknown.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10}

	accountMu.Lock()
	accounts = []*Account{free, unknown}
	rrIndex = 0
	accountMu.Unlock()

	acc, _, err := nextAccountForModel("m", nil)
	if err != nil || acc.Path != "unknown.json" {
		t.Fatalf("窗口到期应改道未知账号: acc=%v err=%v", acc, err)
	}
	acc, _, err = nextAccountForModel("m", nil)
	if err != nil || acc.Path != "free.json" {
		t.Fatalf("窗口内应回到免费账号: acc=%v err=%v", acc, err)
	}

	// 窗口回拨后再次改道
	accountMu.Lock()
	costExploreLast["m"] = time.Now().Add(-2 * time.Hour)
	accountMu.Unlock()
	acc, _, err = nextAccountForModel("m", nil)
	if err != nil || acc.Path != "unknown.json" {
		t.Fatalf("窗口再次到期应改道: acc=%v err=%v", acc, err)
	}
}

// 验证 -cost-explore-interval 0 完全关停探索（回到纯成本分层行为）。
func TestCostExploreDisabledByZero(t *testing.T) {
	oldInterval := cfg.CostExploreInterval
	cfg.CostExploreInterval = 0
	defer func() { cfg.CostExploreInterval = oldInterval }()

	free := &Account{Path: "free.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10, ModelStates: map[string]*modelRuntimeState{
		"m": {CostClass: modelCostFree, CostSamples: 1, ObservedAt: time.Now()},
	}}
	unknown := &Account{Path: "unknown.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10}
	accountMu.Lock()
	accounts = []*Account{free, unknown}
	rrIndex = 0
	accountMu.Unlock()
	for i := range 2 {
		acc, _, err := nextAccountForModel("m", nil)
		if err != nil || acc.Path != "free.json" {
			t.Fatalf("探索关停时第 %d 次应选免费账号: acc=%v err=%v", i+1, acc, err)
		}
	}
}

// 验证探索改道到受控探测账号时的独立类型：失败按常规轮转回退，不走纯探测短路。
func TestCostExploreProbeKind(t *testing.T) {
	oldInterval := cfg.CostExploreInterval
	oldLast := costExploreLast
	cfg.CostExploreInterval = time.Hour
	accountMu.Lock()
	costExploreLast = map[string]time.Time{}
	accountMu.Unlock()
	defer func() {
		cfg.CostExploreInterval = oldInterval
		accountMu.Lock()
		costExploreLast = oldLast
		accountMu.Unlock()
	}()

	free := &Account{Path: "free.json", Auth: &StoredAuth{}, QuotaKnown: true, QuotaRemaining: 10, ModelStates: map[string]*modelRuntimeState{
		"m": {CostClass: modelCostFree, CostSamples: 1, ObservedAt: time.Now()},
	}}
	exhausted := &Account{Path: "exhausted.json", Auth: &StoredAuth{}, QuotaExhausted: true}

	accountMu.Lock()
	accounts = []*Account{free, exhausted}
	rrIndex = 0
	accountMu.Unlock()

	acc, kind, err := nextAccountForModel("m", nil)
	if err != nil || acc.Path != "exhausted.json" || kind != selectionExploreProbe {
		t.Fatalf("探索应改道耗尽账号且类型为 explore_probe: acc=%v kind=%s err=%v", acc, kind, err)
	}
	accountMu.Lock()
	nextProbe := exhausted.ModelStates["m"].NextProbeAt
	accountMu.Unlock()
	if nextProbe.IsZero() {
		t.Fatal("搭车探测应置位 NextProbeAt（探测节流同样生效）")
	}

	// 模拟改道账号失败（已进 attempted）：下一轮应回退免费层账号。
	acc, _, err = nextAccountForModel("m", map[*Account]bool{exhausted: true})
	if err != nil || acc.Path != "free.json" {
		t.Fatalf("探索失败后应回退免费层: acc=%v err=%v", acc, err)
	}
}

// 验证搭车学习在探测失败（需付费额度）时不短路 503，而是回退免费层完成请求。
func TestCostExploreProbeFailureFallsBack(t *testing.T) {
	chdirTemp(t)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if strings.Contains(r.Header.Get("Authorization"), "exhausted-token") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":{"data":{"code":14018,"msg":"额度已用尽"}}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"finish_reason\":\"stop\"}],\"usage\":{\"credit\":0,\"total_tokens\":500}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	oldBase, oldOrigin := profileCN.Base, profileCN.Origin
	oldClient := cfg.HttpClient
	oldInterval := cfg.CostExploreInterval
	oldLast := costExploreLast
	accountMu.Lock()
	oldAccounts, oldRR := accounts, rrIndex
	free := &Account{Path: "free.json", Auth: &StoredAuth{Edition: "cn", Auth: StoredTokens{AccessToken: "free-token", ExpiresAt: time.Now().Add(time.Hour).Unix()}}, QuotaKnown: true, QuotaRemaining: 10,
		ModelStates: map[string]*modelRuntimeState{"explore-model": {CostClass: modelCostFree, CostSamples: 1, ObservedAt: time.Now()}}}
	exhausted := &Account{Path: "exhausted.json", Auth: &StoredAuth{Edition: "cn", Auth: StoredTokens{AccessToken: "exhausted-token", ExpiresAt: time.Now().Add(time.Hour).Unix()}}, QuotaExhausted: true}
	accounts, rrIndex = []*Account{free, exhausted}, 0
	costExploreLast = map[string]time.Time{}
	accountMu.Unlock()
	cfg.CostExploreInterval = time.Hour
	profileCN.Base, profileCN.Origin = server.URL, server.URL
	cfg.HttpClient = server.Client()
	defer func() {
		cfg.CostExploreInterval = oldInterval
		profileCN.Base, profileCN.Origin = oldBase, oldOrigin
		cfg.HttpClient = oldClient
		accountMu.Lock()
		accounts, rrIndex = oldAccounts, oldRR
		costExploreLast = oldLast
		accountMu.Unlock()
	}()

	var logs bytes.Buffer
	oldWriter := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(oldWriter)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"explore-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	handleChatCompletions(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("探索失败应回退完成请求而非 503: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if calls != 2 {
		t.Fatalf("calls=%d want 2（搭车探测 1 + 回退 1）", calls)
	}
	accountMu.Lock()
	blocked := exhausted.ModelStates["explore-model"] != nil && exhausted.ModelStates["explore-model"].QuotaBlocked
	accountMu.Unlock()
	if !blocked {
		t.Fatal("搭车探测失败应标记该账号该模型额度阻断")
	}
	text := logs.String()
	if !strings.Contains(text, "搭车学习失败") || !strings.Contains(text, "回退常规选号") {
		t.Fatalf("缺少探索回退日志:\n%s", text)
	}
}

// 验证成本账本随状态快照持久化并在重启后恢复（含 EMA 值与样本数）。
func TestModelCostSnapshotRoundTrip(t *testing.T) {
	chdirTemp(t)
	accountMu.Lock()
	oldAccounts := accounts
	accounts = []*Account{{Path: "rt.json", Auth: &StoredAuth{}, ModelStates: map[string]*modelRuntimeState{
		"free-m": {CostClass: modelCostFree, CostSamples: 2, ObservedAt: time.Now()},
		"paid-m": {CostClass: modelCostPaid, CostPer1k: 2.5, CostSamples: 3, ObservedAt: time.Now()},
	}}}
	accountMu.Unlock()
	defer func() {
		accountMu.Lock()
		accounts = oldAccounts
		accountMu.Unlock()
	}()

	writeStatusSnapshot()
	accountMu.Lock()
	accounts[0].ModelStates = nil
	restoreAccountRuntimeStateLocked()
	states := accounts[0].ModelStates
	accountMu.Unlock()
	if len(states) != 2 {
		t.Fatalf("账本恢复失败: %+v", states)
	}
	if got := states["free-m"]; got == nil || got.CostClass != modelCostFree || got.CostSamples != 2 {
		t.Fatalf("免费观测恢复不一致: %+v", got)
	}
	if got := states["paid-m"]; got == nil || got.CostClass != modelCostPaid || got.CostPer1k != 2.5 || got.CostSamples != 3 {
		t.Fatalf("收费观测恢复不一致: %+v", got)
	}
}

// 验证 status 账本摘要：免费在前、收费按单价升序、过期观测不展示。
func TestFormatModelCostLedger(t *testing.T) {
	now := time.Now()
	acc := &Account{Path: "l.json", Auth: &StoredAuth{}, ModelStates: map[string]*modelRuntimeState{
		"a-free":  {CostClass: modelCostFree, CostSamples: 3, ObservedAt: now},
		"b-paid":  {CostClass: modelCostPaid, CostPer1k: 2.5, CostSamples: 1, ObservedAt: now},
		"c-stale": {CostClass: modelCostPaid, CostPer1k: 9, CostSamples: 1, ObservedAt: now.Add(-7 * time.Hour)},
	}}
	got := formatModelCostLedger(acc, now)
	if !strings.Contains(got, "2 个模型（免费 1 / 收费 1）") {
		t.Fatalf("账本摘要头部不符: %s", got)
	}
	if !strings.Contains(got, "a-free") || !strings.Contains(got, "2.5000/1k") {
		t.Fatalf("账本摘要缺少条目: %s", got)
	}
	if strings.Contains(got, "c-stale") {
		t.Fatalf("过期观测不应展示: %s", got)
	}
}

// -----------------------------------------------------------------------------
// Phase 4：多代理池（账号稳定绑定 + 客户端路由）
// -----------------------------------------------------------------------------

// 验证账号按凭据文件名稳定绑定到代理池成员：同文件恒同出口、目录无关。
func TestBoundProxyURLStableByFileName(t *testing.T) {
	old := cfg.ProxyURLs
	cfg.ProxyURLs = []string{"http://p1:1", "http://p2:2", "http://p3:3"}
	defer func() { cfg.ProxyURLs = old }()

	a1 := &Account{Path: "workbuddy-intl3.json"}
	a2 := &Account{Path: "workbuddy-intl3.json"}
	if got := boundProxyURL(a1); got == "" || got != boundProxyURL(a2) {
		t.Fatalf("同文件名应稳定绑定: %q vs %q", got, boundProxyURL(a2))
	}
	a3 := &Account{Path: "/opt/workbuddy/workbuddy-intl3.json"}
	if boundProxyURL(a1) != boundProxyURL(a3) {
		t.Fatalf("绑定应只取文件名（部署目录无关）: %q vs %q", boundProxyURL(a1), boundProxyURL(a3))
	}
	cfg.ProxyURLs = nil
	if boundProxyURL(a1) != "" {
		t.Fatal("未配置代理池应返回空绑定")
	}
}

// 验证客户端路由：未配置/未构建/找不到池成员时回退全局客户端。
func TestClientForAccountFallsBack(t *testing.T) {
	oldClient := cfg.HttpClient
	oldURLs := cfg.ProxyURLs
	oldClients := proxyClients
	defer func() {
		cfg.HttpClient = oldClient
		cfg.ProxyURLs = oldURLs
		proxyClients = oldClients
	}()

	global := &http.Client{}
	cfg.HttpClient = global
	cfg.ProxyURLs = nil
	proxyClients = map[string]*http.Client{}
	if clientForAccount(&Account{Path: "x.json"}) != global {
		t.Fatal("未配置代理池应回退全局客户端")
	}
	if clientForAccount(nil) != global {
		t.Fatal("nil 账号应回退全局客户端")
	}

	cfg.ProxyURLs = []string{"http://p1:1"}
	if clientForAccount(&Account{Path: "x.json"}) != global {
		t.Fatal("池客户端未构建时应回退全局客户端")
	}

	pool := &http.Client{}
	proxyClients = map[string]*http.Client{"http://p1:1": pool}
	if clientForAccount(&Account{Path: "x.json"}) != pool {
		t.Fatal("已构建池客户端应命中绑定客户端")
	}
}
