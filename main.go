package main

import (
	"bufio"
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptrace"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/skip2/go-qrcode"
)

const (
	version = "1.15.1"

	// 状态快照文件名：serve 后台周期写入，monitor 前台命令实时读取展示
	statusSnapshotFile = "workbuddy-status.json"
	logDir             = "logs"

	// defaultSystemPrompt 是当客户端首条消息不是 system 时注入的保底系统提示，
	// 用于满足腾讯上游「首条消息必须是 system prompt」的硬性要求 (code 11128)。
	defaultSystemPrompt = "You are a helpful assistant."

	// upstreamHeaderTimeout 限定「请求发送完成 → 收到上游响应头」的等待上限。
	//
	// 该值必须足够宽松：上游对 3MB+ 大请求的排队与预处理本身就可能接近 1 分钟
	// （实测 2.3MB 请求出现过 50.8s 才回响应头）。早期版本用 60s 会误杀这类
	// 正常但缓慢的请求，表现为 502「timeout awaiting response headers」。
	// 默认 5 分钟，只用于兜底「上游彻底无响应」；长时间无数据的流由
	// upstreamIdleTimeout 的空闲看门狗负责中断。可在 config.json 覆盖。
	upstreamHeaderTimeoutDefault = 5 * time.Minute

	// upstreamIdleTimeoutDefault 是流式响应的「空闲读超时」：只要持续有数据到达
	// 就永不超时，一旦超过该时长没有任何新数据，才判定上游卡死并中断。
	upstreamIdleTimeoutDefault = 120 * time.Second

	// defaultRequestTimeout 用于令牌刷新、额度查询、模型目录等控制类短请求。
	defaultRequestTimeout = 60 * time.Second

	// upstreamTransientRetries 是「请求体发送阶段」遇到瞬时网络错误的额外重试次数。
	//
	// 背景：网关与上游 CDN 边缘节点之间的单条 TCP 连接可能被对端重置
	// （connection reset by peer）、被关闭（use of closed network connection）、
	// 或命中已被回收的 keep-alive 连接。这类错误属于瞬时故障，且与请求体大小无关
	// （实测 >5MB 请求 95% 成功，而 0.5MB 请求也会偶发失败）。
	//
	// 只有确认请求头尚未写出时才重试；没有收到响应头不能证明上游没处理 POST。
	// 重试强制使用新连接，默认最多额外尝试 2 次，可在 config.json 覆盖。
	upstreamTransientRetriesDefault = 2

	// upstreamRetryBackoff 是两次重试之间的等待，给上游边缘节点留出恢复时间。
	upstreamRetryBackoffDefault = 300 * time.Millisecond
)

var errModelAccountPolicy = errors.New("model account policy rejected all accounts")

// errStreamClosedWithoutFinish 表示上游连接正常读完（没有读错误），
// 但整段流里从未出现非空 finish_reason。空串不算：上游会在每个中间分片上带 finish_reason:""。
// 这种结束不能当成完整回复，否则下游会把残缺输出当成成功结果。
var errStreamClosedWithoutFinish = errors.New("上游流正常结束，但没有 finish_reason，不能当成完整回复")

// 生效的超时值（默认取上面的 Default，可由 config.json 的 upstream 段覆盖）。
// 定义为变量既便于测试调小阈值，也便于运维按网络状况调整。
var (
	upstreamHeaderTimeout = upstreamHeaderTimeoutDefault
	upstreamIdleTimeout   = upstreamIdleTimeoutDefault
	// upstreamTransientRetries 为生效的瞬时网络错误重试次数。
	upstreamTransientRetries = upstreamTransientRetriesDefault
	// upstreamRetryBackoff 为生效的重试间隔（测试可调小）。
	upstreamRetryBackoff = upstreamRetryBackoffDefault
)

// isTransientNetworkError 判断错误是否为「瞬时网络故障」，这类错误值得重试。
//
// 覆盖：连接被重置/中止（RST）、连接被对端关闭、管道破裂、连接被强制关闭，
// 以及 Go 在复用 keep-alive 连接时常见的 "server closed idle connection"。
// 注意：context.Canceled 与 DeadlineExceeded 不属于此类——前者是客户端主动断开，
// 后者是明确超时，重试都无意义甚至有害。
func isTransientNetworkError(err error) bool {
	if err == nil {
		return false
	}
	// 客户端取消或超时：不重试
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		// 网络层超时（如 ResponseHeaderTimeout）说明上游确实没响应，可重试
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"connection reset by peer",
		"connection reset",
		"broken pipe",
		"use of closed network connection",
		"connection refused",
		"server closed idle connection",
		"unexpected eof",
		"http2: server sent goaway",
		"stream error",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// doUpstreamRequest 发送上游请求，并对瞬时网络错误做有界重试。
//
// 仅在「尚未写出完整请求头」时重试；已发送 POST 后收到 EOF 无法判定上游
// 是否执行过生成，不能安全重放。每次重试都重建请求对象与请求体 reader。
//
// 返回的 cancel 用于在流式读取结束后取消上游 context（调用方负责）。
func doUpstreamRequest(r *http.Request, acc *Account, prof *upstreamProfile, upstreamBytes []byte, reqID uint64, traceID string) (*http.Response, context.CancelFunc, error) {
	// 兼容直接调用与测试；正常入口已经在协议转换前准备完成。只生成一次，不能在 try 循环里重建。
	prepareConversationContext(r, nil)
	var lastErr error
	_, _, retries := upstreamSettings()
	for try := 0; try <= retries; try++ {
		upstreamCtx, upstreamCancel := context.WithCancel(r.Context())
		var headersWritten, bodyWritten atomic.Bool
		trace := &httptrace.ClientTrace{
			WroteHeaders: func() { headersWritten.Store(true) },
			WroteRequest: func(info httptrace.WroteRequestInfo) {
				if info.Err == nil {
					bodyWritten.Store(true)
				}
			},
		}
		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(upstreamCtx, trace), http.MethodPost, prof.chatURL(), bytes.NewReader(upstreamBytes))
		if err != nil {
			upstreamCancel()
			return nil, nil, err
		}
		backendHeaders(req, acc.Auth, prof)
		if debugLoggingEnabled() {
			req.Header.Set("X-Trace-ID", debugTraceID(r))
			req.Header.Set("X-Parent-Request-ID", strconv.FormatUint(reqID, 10))
		}
		client := chatClientForAccount(acc)
		if try > 0 {
			// 重试时不复用已被对端关闭的空闲连接。
			req.Close = true
			client.CloseIdleConnections()
			log.Printf("[网络重试] traceId=%s requestId=%d 账号=%s 第 %d/%d 次重试，上一尝试请求头未写出，已重建连接（错误: %s）",
				traceID, reqID, acc.Path, try, retries, safeDebugError(lastErr))
		}

		debugFullUpstreamRequest(r, req, upstreamBytes, try)
		resp, err := client.Do(req)
		debugFullUpstreamResponse(r, req, resp, try, headersWritten.Load(), bodyWritten.Load(), err)
		if err == nil {
			return resp, upstreamCancel, nil
		}
		upstreamCancel()
		lastErr = err
		if !isTransientNetworkError(err) {
			return nil, nil, err
		}
		if headersWritten.Load() || bodyWritten.Load() {
			log.Printf("[网络重试跳过] traceId=%s requestId=%d 账号=%s 请求头已写出=%t 请求体已写完=%t 原因=上游可能已处理POST，重放可能重复生成 错误=%s",
				traceID, reqID, acc.Path, headersWritten.Load(), bodyWritten.Load(), safeDebugError(err))
			debugEvent(r, "warn", "upstream_retry_skipped", map[string]any{
				"reason": "request_may_have_reached_upstream", "headers_written": headersWritten.Load(),
				"body_written": bodyWritten.Load(), "business_impact": "避免重复执行模型请求",
			})
			return nil, nil, err
		}
		if try < retries {
			debugEvent(r, "warn", "upstream_send_retry", map[string]any{
				"retry": try + 1, "reason": "request_headers_not_written",
				"business_impact": "换新连接重试尚未发出的模型请求",
			})
			select {
			case <-r.Context().Done():
				return nil, nil, err
			case <-time.After(upstreamRetryBackoff):
			}
		}
	}
	return nil, nil, lastErr
}

// -----------------------------------------------------------------------------
// 上游站点 Profile（国内站 / 国际站）
//
// 国际站 www.workbuddy.ai 与国内站 copilot.tencent.com 走同一套 /v2/plugin/* 协议
// （实测：auth/state、auth/token、login/account、auth/token/refresh、chat/completions
// 的路径与响应包络完全一致），差异仅在：
//   - 上游域名与 Web Origin（国际站位于腾讯 EdgeOne 国际 CDN）
//   - 登录 platform 参数（workbuddy-ai 而非 VSCode），登录在浏览器内完成（邮箱/验证码/SSO）
//   - 等待授权期间 auth/token 轮询返回 code 11217 (login ing...)
// 每个账号凭据文件通过 edition 字段记录所属站点，账号池支持国内/国际混挂轮询。
// -----------------------------------------------------------------------------

type upstreamProfile struct {
	Key       string        // 存储于凭据文件 edition 字段的站点标识
	Label     string        // 控制台展示名
	Base      string        // 上游 API 基础地址
	Origin    string        // Origin/Referer 伪装来源（各站 Web 控制台）
	Platform  string        // auth/state 的 platform 参数
	ClientUA  string        // User-Agent
	ClientID  string        // X-Client-ID
	ClientVer string        // X-Client-Version
	Product   string        // X-Product
	LoginTTL  time.Duration // login 命令等待授权完成的超时
}

var (
	profileCN = upstreamProfile{
		Key: "cn", Label: "国内站",
		Base: "https://copilot.tencent.com", Origin: "https://www.codebuddy.cn",
		Platform: "VSCode", ClientUA: "CLI/2.143.1 CodeBuddy/2.143.1",
		ClientID: "codebuddy-cli", ClientVer: "2.143.1", Product: "SaaS",
		LoginTTL: 5 * time.Minute,
	}
	profileINTL = upstreamProfile{
		Key: "intl", Label: "国际站",
		Base: "https://www.workbuddy.ai", Origin: "https://www.workbuddy.ai",
		Platform: "workbuddy-ai", ClientUA: "CLI/2.143.1 CodeBuddy/2.143.1",
		ClientID: "codebuddy-cli", ClientVer: "2.143.1", Product: "SaaS",
		LoginTTL: 15 * time.Minute, // 浏览器内登录（邮箱/验证码/SSO）比扫码慢，放宽超时
	}
)

// profileForEdition 根据凭据文件中的 edition 标识返回上游站点参数；空值/未知值回退国内站。
func profileForEdition(edition string) *upstreamProfile {
	switch strings.ToLower(strings.TrimSpace(edition)) {
	case "intl", "international", "global", "workbuddy.ai":
		return &profileINTL
	default:
		return &profileCN
	}
}

func (p *upstreamProfile) authStateURL() string {
	return p.Base + "/v2/plugin/auth/state?platform=" + url.QueryEscape(p.Platform)
}

func (p *upstreamProfile) loginAcctURL(state string) string {
	return p.Base + "/v2/plugin/login/account?state=" + url.QueryEscape(state)
}

func (p *upstreamProfile) authTokenURL(state string) string {
	return p.Base + "/v2/plugin/auth/token?state=" + url.QueryEscape(state)
}

func (p *upstreamProfile) tokenRefreshURL() string { return p.Base + "/v2/plugin/auth/token/refresh" }

func (p *upstreamProfile) chatURL() string { return p.Base + "/v2/chat/completions" }

func (p *upstreamProfile) quotaSummaryURL() string {
	return p.Origin + "/billing/meter/get-user-resource-summary"
}

func (p *upstreamProfile) enterpriseUsageURL() string {
	// 外部项目的企业号实测表明，get-user-resource* 对 enterpriseId 返回空包，
	// 必须改问分配额度：credit=本周期已用，limitNum=分配额度。
	return p.Origin + "/v2/billing/meter/get-enterprise-user-usage"
}

func (p *upstreamProfile) dailyCheckinURL() string {
	return p.Origin + "/v2/billing/meter/daily-checkin"
}

// -----------------------------------------------------------------------------
// 数据结构定义
// -----------------------------------------------------------------------------

type StoredAuth struct {
	Auth    StoredTokens  `json:"auth"`
	Account StoredAccount `json:"account"`
	Edition string        `json:"edition,omitempty"` // 站点标识：cn（国内站，默认）| intl（国际站 www.workbuddy.ai）
}

type StoredTokens struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresAt    int64  `json:"expiresAt"`
	Domain       string `json:"domain"`
	// 以下为本地续期元数据，官方客户端不读；旧凭据文件缺省为 0，不影响解析。
	RefreshExpiresAt int64 `json:"refreshExpiresAt,omitempty"` // 刷新令牌到期时间（Unix 秒），0=未知
	LastRefreshTime  int64 `json:"lastRefreshTime,omitempty"`  // 最近一次续期成功时间（Unix 秒），0=从未续期
}

type StoredAccount struct {
	UID          string `json:"uid"`
	EnterpriseID string `json:"enterpriseId"`
	Nickname     string `json:"nickname"`
}

type apiEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type tokenData struct {
	AccessToken      string `json:"accessToken"`
	RefreshToken     string `json:"refreshToken"`
	ExpiresIn        int64  `json:"expiresIn"`
	RefreshExpiresIn int64  `json:"refreshExpiresIn"`
	Domain           string `json:"domain"`
}

type accountData struct {
	UID          string `json:"uid"`
	EnterpriseID string `json:"enterpriseId"`
	Nickname     string `json:"nickname"`
}

type authStateData struct {
	State   string `json:"state"`
	AuthURL string `json:"authUrl"`
}

type quotaPackage struct {
	CycleTotalCapacity  string `json:"CycleTotalCapacity"`
	CycleUsedCapacity   string `json:"CycleUsedCapacity"`
	CycleRemainCapacity string `json:"CycleRemainCapacity"`
}

type quotaSummaryData struct {
	Packages []quotaPackage `json:"Packages"`
	// IsPaidUser 为上游「正式付费订阅」标记；Pro 试用用户该字段同样为 false。
	IsPaidUser bool `json:"IsPaidUser"`
	// ProTrialStatus 不能单独证明试用仍有效；必须结合当前权益资源。
	ProTrialStatus any `json:"ProTrialStatus"`
	// SubscriptionPackageCode 为当前订阅包编码，非空表示存在订阅包。
	SubscriptionPackageCode string `json:"SubscriptionPackageCode"`
}

// -----------------------------------------------------------------------------
// 全局状态与配置
// -----------------------------------------------------------------------------

type Config struct {
	Addr                string
	Port                int
	PortExplicit        bool
	WebPort             int
	AdminKey            string
	AuthFile            string
	AuthDir             string
	AuthExplicit        bool // 用户是否显式指定了 -auth（未指定时自动扫描目录下所有 workbuddy*.json）
	LoginIntl           bool // login -intl：登录国际站 (www.workbuddy.ai，浏览器内完成登录)
	APIKey              string
	ProxyURL            string
	Verbose             bool
	DebugEnabled        bool          // 仅由工作目录 config.json 的 debug.enabled 控制
	ReloadInterval      int           // 账号池热加载扫描间隔（秒），0 关闭
	MonitorInterval     int           // monitor 状态刷新间隔（秒）
	LogFile             string        // monitor 附加展示的日志文件路径
	JournalService      string        // monitor 附加展示的 systemd 服务名（journalctl -u）
	LogLines            int           // monitor 展示的最近日志行数
	PromptMode          string        // 系统提示词模式：passthrough（透传）/ custom（替换）/ append（插入）
	PromptText          string        // custom/append 模式使用的网关提示词文本，空 = 内置中性提示词
	ModelsRefresh       int           // 官方模型目录刷新间隔（分钟），0 关闭（实时接口 + npm 合并）
	AllowPriceProbes    bool          // 仅显式开启后台价格探测；真实生成请求可能消耗积分
	DisablePriceProbes  bool          // 旧禁用参数兼容，优先于 AllowPriceProbes；不影响客户端请求及显式 probe 命令
	KeepaliveHours      []int         // 主动续期时刻（本地小时），空表示关闭；到点主动刷新全部账号
	ProbeModels         string        // probe 专用：逗号分隔的模型列表
	ProbeLimit          int           // probe 专用：未显式指定模型时的取用数量
	WebUI               bool          // 仅显式 -webui 才启动独立管理端口
	CostExploreInterval time.Duration // costTier 条件探索窗口（默认 30m，0 关停）：免费层垄断 + 存在未知层账号时，按窗口把一次选号改道给未知号搭车学习
	ProxyURLs           []string      // 多代理池（-proxies，逗号分隔）：账号按凭据文件名稳定绑定到其中一个出口 IP
	ExtraModels         []string      // 额外模型 ID（-extra-models，逗号分隔）：上游可请求但目录未收录的模型，透出到 /v1/models 供客户端发现
	HttpClient          *http.Client
}

// Account 表示一个 CodeBuddy 账号凭据及其运行时状态。
type Account struct {
	Path           string                        // 凭据文件路径
	Auth           *StoredAuth                   // 凭据内容（失效标记恢复时可能为 nil）
	CooldownUntil  time.Time                     // 冷却截止时间（429 频率限制后自动屏蔽到该时间）
	CooldownMsg    string                        // 触发冷却的原因/上游提示
	Disabled       bool                          // 授权失效（token 过期且无法刷新/已撤销）：禁止调度
	DisabledReason string                        // 失效原因，用于控制台展示
	Nickname       string                        // 失效标记持久化字段：昵称（Auth 为 nil 时使用）
	UID            string                        // 失效标记持久化字段：UID
	Edition        string                        // 站点标识（cn/intl）：失效标记恢复时 Auth 为 nil 也可见
	QuotaTotal     float64                       // 最近一次额度查询返回的周期总额度
	QuotaUsed      float64                       // 最近一次额度查询返回的已用额度
	QuotaRemaining float64                       // 最近一次额度查询返回的剩余额度
	IsPaidUser     bool                          // 是否为付费用户（上游 IsPaidUser 原值）
	PlanLabel      string                        // 官方套餐名称或已知套餐短名称
	PlanCheckedAt  int64                         // 最近成功校验权益的时间；旧快照为0
	PlanStale      bool                          // 最近查询失败，展示时标记旧结果
	QuotaKnown     bool                          // 是否已成功获取过额度
	QuotaExhausted bool                          // 已确认额度为 0；额度扫描发现恢复后自动解除
	QuotaUnlimited bool                          // 企业版 limitNum=-1：分配额度不限，禁止按 0 余额阻断
	ModelStates    map[string]*modelRuntimeState // 按模型隔离的成本、限流和额度阻断状态
	fingerprint    string                        // 凭据文件变更指纹（mtime+size，凭据热加载用）
	lock           sync.Mutex                    // 单账号串行锁（防止同账号并发触发 11128）
	// 续期失败计数（仅内存，重启清零）：单次失败可能是上游抖动，连续失败才判定登录态失效。
	RefreshFailCount int
	LastRefreshError string
}

const (
	modelCostUnknown = "unknown"
	modelCostFree    = "free"
	modelCostPaid    = "paid"
	modelProbeDelay  = 5 * time.Minute

	// modelFreeMinTokens 判定“免费模型”所需的最小样本：上游对极小请求也可能记
	// credit=0（例如探针请求），那不是真正的免费，不能据此让零余额账号请求收费模型。
	modelFreeMinTokens = 100

	// modelCostTTL 成本观测有效期：超过该时长的免费/收费观测视为未知（重新学习）。
	// 限免/夜间免费是时段性的，陈旧观测复活会把流量错误地导向收费账号（wb2api 同款口径）。
	modelCostTTL = 6 * time.Hour

	// modelCostEMAAlpha 每千 token 单价的 EMA 平滑系数（约 5 次观测收敛，
	// wb2api 同口径）：单次异常值不主导账本。
	modelCostEMAAlpha = 0.3

	// modelCostExploreDefault costTier 条件探索的默认窗口（wb2api issue #136 方案 a′）：
	// 免费层垄断且存在未知层账号时，按窗口把一次选号改道给未知层搭车学习。
	// 30m ≈ 每模型 ≤48 次/天；0 = 关停。
	modelCostExploreDefault = 30 * time.Minute

	// wafCooldownBase WAF 403 拦截的账号软冷却时长。
	//
	// 与 429 频率限制的冷却语义区分：WAF 拦的是出口 IP 而非账号（多账号在同一
	// 出口 IP 上会一起中招），账号本身健康。软冷却让该账号先避让，轮换到其他
	// 账号继续服务；**绝不 disableAccount**——那会删除凭据文件，把 IP 级风控
	// 误判成需要重新登录的授权失效。
	//
	// 取值对齐 wb2api 的 wafCooldownBase（60s）：足够让上游频控窗口滑过，又不至于
	// 让账号长时间离池。wb2api 生产实测（单日 680 次命中）显示拦截多为突发，
	// 60s 后通常已恢复。
	wafCooldownBase = 60 * time.Second

	// 生效的首字节/空闲超时由 config.json 的 upstream 段覆盖，见 upstreamSettings。

	// accountFaultCooldown 账号级授权/配额故障的冷却时长。
	//
	// 适用 11140 request illegal / 14017 trial 未激活这类由账号自身状态决定的错误：
	// 短冷却后仍会复现，但**不删凭据**（可能是临时风控，且删了需人工重新登录）。
	// 取 10 分钟——比 WAF 软冷却长（账号级问题恢复更慢），比硬冷却短（不是余额耗尽）。
	accountFaultCooldown = 10 * time.Minute
)

type modelRuntimeState struct {
	CostClass     string
	CooldownUntil time.Time
	QuotaBlocked  bool
	NextProbeAt   time.Time
	LastReason    string
	ObservedAt    time.Time
	// CostPer1k 实测每千 token 单价（EMA 平滑值，<=0 = 实测免费）。
	// 与 CostClass 互补：CostClass 是二值结论，CostPer1k 保留「收费多少」的
	// 量级信息，用于同档内择优与 monitor 展示。
	CostPer1k float64
	// CostSamples 累计观测次数（EMA 收敛度参考）。
	CostSamples int
}

type accountSelectionKind string

const (
	selectionNormal         accountSelectionKind = "normal"
	selectionFreeExhausted  accountSelectionKind = "free_exhausted"
	selectionProbeExhausted accountSelectionKind = "probe_exhausted"
	// selectionExploreProbe 搭车学习改道到受控探测路径的独立类型：
	// 失败按常规轮转回退继续本次请求，不触发纯探测的 503 短路。
	selectionExploreProbe accountSelectionKind = "explore_probe"
)

// Profile 返回该账号对应的上游站点参数（国内站/国际站）。
func (acc *Account) Profile() *upstreamProfile {
	if acc.Auth != nil {
		return profileForEdition(acc.Auth.Edition)
	}
	return profileForEdition(acc.Edition)
}

// disabledMarker 是授权失效账号的持久化标记（凭据文件删除后用于控制台提示重新登录）。
type disabledMarker struct {
	Path       string `json:"path"`
	Reason     string `json:"reason"`
	DisabledAt int64  `json:"disabledAt"`
	Nickname   string `json:"nickname,omitempty"`
	UID        string `json:"uid,omitempty"`
	Edition    string `json:"edition,omitempty"` // 站点标识（cn/intl），用于控制台展示
}

// markerPath 返回与凭据文件同目录的失效标记文件路径。
func markerPath(authPath string) string {
	return authPath + ".disabled"
}

var (
	cfg        Config
	authLock   sync.RWMutex
	currAuth   *StoredAuth
	reqCounter uint64

	accountMu sync.Mutex // 账号池保护锁
	accounts  []*Account // 多账号池（单账号时长度为 1，行为与旧版完全一致）
	rrIndex   int        // 轮询游标

	// costExploreLast 各模型的上次条件探索时刻（accountMu 保护；运行态不持久化，
	// 重启归零 → 每个仍冻结的模型至多一次即时重探，已学到的账本经快照恢复）。
	costExploreLast   = map[string]time.Time{}
	costExploreEvents int64 // 累计条件探索次数（accountMu 保护，monitor/调试可读）

	// proxyClients 多代理池的 HTTP 客户端表（代理 URL → 客户端，initHTTPClient 构建）。
	proxyClients = map[string]*http.Client{}
	// proxyChatClients 同上，但为聊天上游专用（无总时长，见 initHTTPClient）。
	proxyChatClients = map[string]*http.Client{}
	// chatClient 聊天上游专用客户端（无总时长）。仅 upstreamChat 使用。
	chatClient *http.Client

	quotaScanTrigger = make(chan struct{}, 1)
	checkinTrigger   = make(chan struct{}, 1)
	dailyCheckinMu   sync.Mutex
)

// -----------------------------------------------------------------------------
// 主入口与命令行控制
// -----------------------------------------------------------------------------

func registerAutomaticPriceProbeFlags(fs *flag.FlagSet, target *Config) {
	fs.BoolVar(&target.AllowPriceProbes, "allow-price-probes", false, "允许后台自动价格探测（默认关闭；真实生成请求可能消耗积分）")
	// 兼容旧 systemd / 启动脚本，但不再出现在主要用法说明中。
	// 即使传入 -disable-price-probes=false，也不能绕过显式开启的要求。
	fs.BoolVar(&target.DisablePriceProbes, "disable-price-probes", false, "")
}

func (c Config) automaticPriceProbesAllowed() bool {
	// 两个参数同时出现时，禁用优先，且不依赖命令行参数的排列顺序。
	return c.AllowPriceProbes && !c.DisablePriceProbes
}

func main() {
	if len(os.Args) > 1 {
		first := os.Args[1]
		if first == "help" || first == "-h" || first == "--help" {
			printHelp()
			return
		}
		if first == "version" || first == "-v" || first == "--version" {
			fmt.Printf("WorkBuddy Local Gateway v%s\n", version)
			return
		}
	}

	command := "serve"
	args := os.Args[1:]
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		command = os.Args[1]
		args = os.Args[2:]
	}

	extraModelsList := ""
	proxyList := ""
	fs := flag.NewFlagSet(command, flag.ExitOnError)
	// legacyAPIKeyFlag：旧启动方式兼容。显式传入时覆盖 config.json 的
	// gateway.apiKey 并启用模型 API 鉴权；后续配置热加载也会保持该覆盖。
	var legacyAPIKeyFlag string
	fs.StringVar(&cfg.Addr, "addr", "127.0.0.1", "网关监听地址")
	fs.IntVar(&cfg.Port, "port", defaultAPIPort, "API 监听端口（显式传入时覆盖 config.json 的 gateway.apiPort）")
	fs.StringVar(&cfg.AuthFile, "auth", "workbuddy.json", "凭据存储文件路径（支持逗号分隔多个文件实现多账号）")
	fs.StringVar(&cfg.AuthDir, "auth-dir", "", "凭据目录：自动加载目录下所有 workbuddy*.json 作为多账号池")
	fs.StringVar(&cfg.ProxyURL, "proxy", "", "可选：上游请求代理 (如 http://127.0.0.1:7890)")
	fs.StringVar(&legacyAPIKeyFlag, "api-key", "", "可选：访问网关所需的 API Key（旧启动方式；优先使用 config.json 的 gateway.apiKey）")
	// 自定义参数错误提示，避免默认 PrintDefaults 把兼容占位参数列出来。
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "参数有误，请运行 %s --help 查看可用参数\n", os.Args[0])
	}
	fs.BoolVar(&cfg.Verbose, "verbose", false, "输出详细调试日志")
	fs.BoolVar(&cfg.LoginIntl, "intl", false, "login 专用：登录国际站 (www.workbuddy.ai，浏览器内完成登录)；默认登录国内站")
	fs.IntVar(&cfg.MonitorInterval, "interval", 3, "monitor 状态刷新间隔（秒）")
	fs.IntVar(&cfg.ReloadInterval, "reload-interval", 5, "账号池热加载扫描间隔（秒），0 关闭：运行期自动发现新增/更新/删除的凭据文件，免重启")
	fs.StringVar(&cfg.LogFile, "logfile", "", "monitor 附加跟随的日志文件路径（如 -logfile /var/log/workbuddy-gateway.log）")
	fs.StringVar(&cfg.JournalService, "journal", "", "monitor 附加跟随的 systemd 服务名（Linux 下用 journalctl -u <服务> -f 跟随）")
	fs.IntVar(&cfg.LogLines, "lines", 15, "monitor 每次刷新展示的最近日志行数")
	fs.IntVar(&cfg.ModelsRefresh, "models-refresh", 60, "模型目录刷新间隔（分钟），0 关闭（实时接口 + npm 合并）")
	fs.StringVar(&cfg.PromptMode, "prompt-mode", "passthrough", "系统提示词模式：passthrough（透传客户端原值）/ custom（网关提示词替换）/ append（网关提示词插入）")
	fs.StringVar(&cfg.PromptText, "prompt-text", "", "custom/append 模式的网关系统提示词文本，空 = 内置中性提示词")
	fs.DurationVar(&cfg.CostExploreInterval, "cost-explore-interval", 30*time.Minute, "costTier 条件探索窗口（默认 30m，0 关停）：免费层垄断时把一次选号改道给未知账号搭车学习")
	registerAutomaticPriceProbeFlags(fs, &cfg)
	keepaliveHours := hourList{22}
	fs.Var(&keepaliveHours, "keepalive-hours", "主动续期时刻（本地小时，逗号分隔，默认 22）；留空关闭")
	fs.StringVar(&cfg.ProbeModels, "models", "", "probe 专用：逗号分隔的待探测模型（默认取目录前几个）")
	fs.IntVar(&cfg.ProbeLimit, "limit", 5, "probe 专用：未显式指定模型时探测的模型数量上限")
	fs.BoolVar(&cfg.WebUI, "webui", false, "启用独立网页控制台；管理 Key、API 鉴权与端口由 config.json 配置")
	fs.StringVar(&proxyList, "proxies", "", "多代理池（逗号分隔）：账号按凭据文件名稳定绑定到其中一个出口 IP")
	fs.StringVar(&extraModelsList, "extra-models", "", "额外模型 ID（逗号分隔）：上游可请求但官方目录未收录的模型，透出到 /v1/models 供客户端发现（去重、大小写归一）")
	_ = fs.Parse(args)
	cfg.KeepaliveHours = []int(keepaliveHours)

	// -proxies 逗号分隔解析：去空白、忽略空项。
	for _, p := range strings.Split(proxyList, ",") {
		if p = strings.TrimSpace(p); p != "" {
			cfg.ProxyURLs = append(cfg.ProxyURLs, p)
		}
	}
	// -extra-models 逗号分隔解析。
	cfg.ExtraModels = parseExtraModels(extraModelsList)
	// 若未指定 -auth 且未指定 -auth-dir，则自动扫描当前目录下所有 workbuddy*.json 组成账号池，
	// 这样把多个凭据文件放进工作目录即可自动多账号，无需手写参数。
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "auth" {
			cfg.AuthExplicit = true
		}
		if f.Name == "port" {
			cfg.PortExplicit = true
		}
	})
	serveCommand := command == "serve" || command == "run" || command == "start"
	if serveCommand || command == "probe" {
		if err := loadRuntimeConfig(runtimeConfigFile); err != nil {
			fmt.Fprintf(os.Stderr, "启动失败: %v\n", err)
			os.Exit(1)
		}
		// 兼容旧启动方式的 -api-key：显式传入时覆盖 config.json 的 gateway.apiKey，
		// 并在后续配置热加载中保持覆盖，避免旧部署升级后突然失去鉴权。
		if legacyAPIKeyFlag = strings.TrimSpace(legacyAPIKeyFlag); legacyAPIKeyFlag != "" {
			legacyAPIKeyOverride = legacyAPIKeyFlag
			gatewayConfigMu.Lock()
			gatewaySettings.APIKeyEnabled = true
			gatewaySettings.APIKey = legacyAPIKeyFlag
			setGatewayKeysLocked(gatewaySettings)
			gatewayConfigMu.Unlock()
			log.Printf("[配置] -api-key 已覆盖 config.json 的 gateway.apiKey（旧启动方式兼容，Key 已隐藏）")
		}
	}

	// 初始化 HTTP 客户端
	initHTTPClient()
	closeLog := initFileLogging(command)
	defer closeLog()
	closeDebug := func() {}
	if serveCommand {
		var err error
		closeDebug, err = initDebugLogging()
		if err != nil {
			log.Fatalf("[Debug] 调试日志初始化失败: %v", err)
		}
	}
	defer closeDebug()

	switch command {
	case "serve", "run", "start":
		runServe()
	case "login":
		runLogin()
	case "status":
		runStatus()
	case "refresh":
		runRefresh()
	case "monitor":
		runMonitor()
	case "probe":
		runProbe()
	case "reset":
		runReset()
	case "version", "-v", "--version":
		fmt.Printf("WorkBuddy Local Gateway v%s\n", version)
	default:
		fmt.Printf("未知命令: %s\n\n", command)
		printHelp()
		os.Exit(1)
	}
}

// initFileLogging 将运行日志同时写入控制台和按日期命名的项目日志文件。
func initFileLogging(command string) func() {
	gatewayConfigMu.RLock()
	registerSecrets(cfg.APIKey, cfg.AdminKey, gatewaySettings.APIKey)
	gatewayConfigMu.RUnlock()
	// 文件初始化失败时，控制台仍要隐藏秘密值。
	log.SetOutput(&redactingLogWriter{writer: os.Stderr})
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("[Log] 无法创建日志目录 %s，将仅输出到控制台: %v", logDir, err)
		return func() {}
	}
	path := filepath.Join(logDir, "gateway-"+time.Now().Format("2006-01-02")+".log")
	f, err := newDailyLogWriter("gateway", ".log")
	if err != nil {
		log.Printf("[Log] 无法打开日志文件 %s，将仅输出到控制台: %v", path, err)
		return func() {}
	}
	log.SetOutput(&redactingLogWriter{writer: io.MultiWriter(os.Stderr, f)})
	log.Printf("[Log] 审计日志已启用，文件=%s，命令=%s，版本=%s", path, command, version)
	return func() { _ = f.Close() }
}

func printHelp() {
	fmt.Println(`WorkBuddy Local Gateway - 轻量化跨平台本地 AI 网关
将腾讯 CodeBuddy / 混元反代为标准 OpenAI 协议，支持直接透传任意模型到上游。
支持多账号池：轮询使用多个账号均衡额度；账号触发 429 频率限制后自动冷却屏蔽，
由其余账号代偿，冷却到期自动恢复。

用法:
  workbuddy-gateway [command] [options]

命令:
  serve       启动本地网关 (默认操作)
  login       登录并获取/更新凭据：国内站微信扫码；-intl 登录国际站 (浏览器内完成)
  status      查看账号池状态（含站点、冷却状态与过期时间）
  refresh     手动立即刷新所有账号访问令牌 (Access Token)
  monitor     前台实时监控：周期刷新展示账号状态 + 最近日志 (Ctrl+C 退出)
  probe       主动探测账号对指定模型的免费/收费属性（需 serve 正在运行）
  reset       清空除登录凭据外的全部本地数据（状态/缓存/日志/失效标记），并重新拉取模型与倍率
  version     查看版本信息
  help        查看帮助说明

参数选项:
  -addr <ip>        网关监听地址 (默认: 127.0.0.1)
  -port <port>      网关监听端口 (默认: 8317)
  -auth <path>      凭据文件路径；支持逗号分隔多个文件实现多账号
                    (默认: 自动发现当前目录下所有 workbuddy*.json)
  -auth-dir <dir>   凭据目录：自动加载目录下所有 workbuddy*.json 作为账号池
  -api-key <key>    可选：访问网关所需的 API Key（旧启动方式；config.json 的
                    gateway.apiKey 优先，命令行显式传入时覆盖配置）
  -intl             login 专用：登录国际站 www.workbuddy.ai（浏览器内完成登录）
  -reload-interval <sec>
                    账号池热加载扫描间隔（默认 5 秒，0 关闭）：运行期自动发现
                    新增/更新/删除的凭据文件，免重启生效
  -models-refresh <min>
                    模型目录刷新间隔（默认 60 分钟，0 关闭）
                    （目录来源：实时接口 + npm 静态包，合并去重）
  -prompt-mode <mode>
                    系统提示词模式：passthrough（默认，透传）/ custom（替换）/
                    append（插入网关提示词）
  -prompt-text <text>
                    custom/append 模式使用的网关提示词，空 = 内置中性提示词
  -cost-explore-interval <dur>
                    costTier 条件探索窗口（默认 30m，0 关停）：免费层垄断时
                    按窗口把一次选号改道给未知账号搭车学习（零新增上游请求）
  -proxies <url1,url2,...>
                    多代理池（逗号分隔）：账号按凭据文件名稳定绑定到其中一个
                    出口 IP；与 -proxy 可并用（-proxies 优先用于账号维度调用）
  -extra-models <id1,id2,...>
                    额外模型 ID：上游可请求但目录未收录的模型，透出到
                    /v1/models 供客户端发现
  -allow-price-probes
                    开启后台自动价格探测（默认关闭）；
                    会发起真实模型生成请求，可能消耗积分，请确认后开启；
                    不影响客户端请求、目录刷新及显式 probe 命令
  -keepalive-hours <h,h,...>
                    主动续期时刻（本地小时，逗号分隔，默认 22）
                    到点主动刷新全部账号登录凭据，不等访问令牌临近过期；
                    留空关闭。只调刷新接口，不请求模型、不消耗额度
  -webui            启用独立网页控制台 (默认 http://<addr>:8316/ui/)：
                    展示账号池/模型统计/日志/config.json/凭据（令牌脱敏），
                    并可切换模型 API Key 校验；管理 Key 与端口在 config.json
                    的 gateway 段配置，管理 Key 为空时可在服务器本机首次生成

probe 选项:
  -auth <path>      只探测指定凭据文件（文件名或路径均可）；默认探测全部账号
  -models <m1,m2>   指定要探测的模型；默认取模型目录前几个
  -limit <n>        未指定 -models 时探测的模型数量（默认 5，上限 50）
  -addr/-port       需与运行中的 serve 一致；模型 API 开启 Key 校验时 probe 自动携带 gateway.apiKey
  -proxy <url>      设置上游转发代理 (例如 http://127.0.0.1:7890 或 socks5://...)
  -verbose          输出详细调试日志 (请求/响应体)

monitor 选项:
  -interval <sec>   状态刷新间隔秒数 (默认: 3)
  -journal <svc>    同时展示 systemd 服务最近日志 (Linux, 如 -journal workbuddy-gateway)
  -logfile <path>   同时展示指定日志文件最近内容 (如 -logfile /var/log/wb.log)
  -lines <n>        每次刷新展示的最近日志行数 (默认: 15)

多账号说明:
  # 登录第二个账号（保存到不同文件）
  workbuddy-gateway login -auth workbuddy2.json

  # 登录国际站账号（www.workbuddy.ai，浏览器内完成登录）
  workbuddy-gateway login -intl -auth workbuddy-intl.json

  # 自动发现：把多个凭据文件放进工作目录即可自动多账号（无需任何参数）
  workbuddy-gateway serve        # 自动加载 ./workbuddy*.json

  # 启动时指定多个凭据文件（轮询 + 429 自动冷却代偿；国内/国际可混挂）
  workbuddy-gateway serve -auth workbuddy.json,workbuddy2.json

  # 或使用目录模式：目录内所有 workbuddy*.json 自动组成账号池
  workbuddy-gateway serve -auth-dir ./auths

  # 运行期新增/更新/删除凭据文件会自动热加载（默认每 5 秒），无需重启 serve

示例:
  # 首次使用扫码登录（国内站）
  workbuddy-gateway login

  # 登录国际站（www.workbuddy.ai）
  workbuddy-gateway login -intl

  # 启动本地网关 (监听 127.0.0.1:8317)
  workbuddy-gateway serve

  # 启动网关并指定端口和代理
  workbuddy-gateway serve -port 9000 -proxy http://127.0.0.1:7890

  # 多代理池：账号按文件名稳定散列到多个出口 IP（规避单 IP 风控）
  workbuddy-gateway serve -proxies http://127.0.0.1:7890,socks5://127.0.0.1:1080`)
}

func initHTTPClient() {
	jar, _ := cookiejar.New(nil)
	newTransport := func(proxyURL string) (*http.Transport, error) {
		// 首字节超时由 config.json 的 upstream 段热更新；hotConfigTransport 会在配置变化后克隆连接池。
		headerTimeout, _, _ := upstreamSettings()
		transport := &http.Transport{
			MaxIdleConns:          50,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   10,
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: false},
			ResponseHeaderTimeout: headerTimeout,
		}
		if proxyURL != "" {
			pURL, err := url.Parse(proxyURL)
			if err != nil {
				return nil, fmt.Errorf("无效的代理地址 %s: %w", proxyURL, err)
			}
			transport.Proxy = http.ProxyURL(pURL)
		}
		return transport, nil
	}
	transport, err := newTransport(cfg.ProxyURL)
	if err != nil {
		log.Fatalf("错误: %v", err)
	}
	// 共享客户端不设总时长：控制类调用（刷新、额度、目录、探测）由 doJSON 的
	// context 超时兜底；聊天长流不能被总时长硬切断。首字节超时由 hotConfigTransport
	// 在配置热更新后自动换用新的连接池。
	cfg.HttpClient = &http.Client{
		Transport: &hotConfigTransport{transport: transport},
		Jar:       jar,
	}
	// chatClient 聊天上游专用客户端（无总时长）。仅 upstreamChat 使用，
	// 首字节超时与空闲超时分别由 hotConfigTransport 与流中看门狗约束。
	chatClient = &http.Client{
		Timeout:   0,
		Transport: &hotConfigTransport{transport: transport},
		Jar:       jar,
	}
	// 多代理池（-proxies）：每个出口代理一个独立客户端（共享 cookie jar），
	// 账号按凭据文件名稳定绑定到其中一个（见 clientForAccount）。绑定只作用于
	// 账号维度的上游调用；未绑定的调用（npm 目录等）仍走全局客户端。
	// 每个出口同样配「默认」与「聊天」两个客户端，语义与全局一致。
	proxyClients = make(map[string]*http.Client, len(cfg.ProxyURLs))
	proxyChatClients = make(map[string]*http.Client, len(cfg.ProxyURLs))
	for _, proxyURL := range cfg.ProxyURLs {
		poolTransport, err := newTransport(proxyURL)
		if err != nil {
			log.Fatalf("错误: %v", err)
		}
		proxyClients[proxyURL] = &http.Client{
			Transport: &hotConfigTransport{transport: poolTransport},
			Jar:       jar,
		}
		proxyChatClients[proxyURL] = &http.Client{
			Timeout:   0,
			Transport: &hotConfigTransport{transport: poolTransport},
			Jar:       jar,
		}
	}
}

// boundProxyURL 返回账号稳定绑定的出口代理 URL；未配置代理池时返回空串。
// 绑定键用凭据文件名（而非完整路径）：部署目录变化不影响绑定稳定性，
// 同一账号永远走同一出口 IP（上游风控按 IP 记账，绑定漂移会放大风险）。
func boundProxyURL(acc *Account) string {
	if acc == nil || len(cfg.ProxyURLs) == 0 {
		return ""
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(filepath.Base(acc.Path)))
	return cfg.ProxyURLs[int(h.Sum32())%len(cfg.ProxyURLs)]
}

// clientForAccount 返回账号绑定的 HTTP 客户端。
// 用于所有非聊天调用：凭据刷新、额度查询、签到、模型目录、价格探测。
// 这些调用必须自带 ctx 超时（doJSON 已提供）；客户端本身不设总时长，避免误伤长流。
func clientForAccount(acc *Account) *http.Client {
	if acc == nil || len(cfg.ProxyURLs) == 0 {
		return cfg.HttpClient
	}
	if client := proxyClients[boundProxyURL(acc)]; client != nil {
		return client
	}
	return cfg.HttpClient
}

// chatClientForAccount 返回聊天上游调用专用客户端（无总时长）。
//
// 与 clientForAccount 的差异仅在 Client.Timeout：流式响应可能持续数分钟，
// 总时长会把正常长流硬切断。改由 ResponseHeaderTimeout（首字节前）与流中
// 空闲监控（静默掐流）两层约束，见 initHTTPClient 的分层说明。
// **只允许 upstreamChat 使用**。
func chatClientForAccount(acc *Account) *http.Client {
	if acc == nil || len(cfg.ProxyURLs) == 0 {
		if chatClient == nil {
			return clientForAccount(acc) // 测试未初始化时回落
		}
		return chatClient
	}
	if client := proxyChatClients[boundProxyURL(acc)]; client != nil {
		return client
	}
	return chatClientForAccount(nil)
}

// idleReadCloser 为上游流式响应体提供「空闲读超时」保护。
// 只要持续有数据到达就不断顺延截止时间；一旦超过 timeout 没有任何新数据，
// 就取消上游请求上下文，使阻塞中的 Read 立即返回错误，从而中断卡死的流。
type idleReadCloser struct {
	rc      io.ReadCloser
	timeout time.Duration
	cancel  context.CancelFunc
	mu      sync.Mutex
	timer   *time.Timer
	last    time.Time // 最近一次读到数据的时刻
	stopped bool
}

func newIdleReadCloser(rc io.ReadCloser, timeout time.Duration, cancel context.CancelFunc) *idleReadCloser {
	b := &idleReadCloser{rc: rc, timeout: timeout, cancel: cancel, last: time.Now()}
	b.mu.Lock()
	b.timer = time.AfterFunc(timeout, b.check)
	b.mu.Unlock()
	return b
}

// check 在计时到期时判断是否真的空闲超时；若期间有新数据到达则按剩余时间续期，
// 避免「数据刚好到达」与「计时器触发」之间的竞态导致误杀活跃流。
func (b *idleReadCloser) check() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return
	}
	idle := time.Since(b.last)
	if idle >= b.timeout {
		b.stopped = true
		b.cancel()
		return
	}
	b.timer.Reset(b.timeout - idle)
}

func (b *idleReadCloser) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.mu.Lock()
		b.last = time.Now() // 有数据到达即视为活跃
		b.mu.Unlock()
	}
	return n, err
}

func (b *idleReadCloser) Close() error {
	b.mu.Lock()
	b.stopped = true
	if b.timer != nil {
		b.timer.Stop()
	}
	b.mu.Unlock()
	b.cancel()
	return b.rc.Close()
}

// -----------------------------------------------------------------------------
// 凭据加载、保存与自动续期
// -----------------------------------------------------------------------------

func loadAuth() (*StoredAuth, error) {
	authLock.RLock()
	if currAuth != nil {
		defer authLock.RUnlock()
		return currAuth, nil
	}
	authLock.RUnlock()

	data, err := os.ReadFile(cfg.AuthFile)
	if err != nil {
		return nil, fmt.Errorf("读取凭据文件失败 (%s): %w", cfg.AuthFile, err)
	}
	var sa StoredAuth
	if err := json.Unmarshal(data, &sa); err != nil {
		return nil, fmt.Errorf("解析凭据文件失败: %w", err)
	}
	if sa.Auth.AccessToken == "" {
		return nil, fmt.Errorf("凭据文件中缺少 AccessToken")
	}
	registerCredentialSecrets(&sa)

	authLock.Lock()
	currAuth = &sa
	authLock.Unlock()
	return &sa, nil
}

func saveAuth(sa *StoredAuth) error {
	registerCredentialSecrets(sa)
	traceID := newTraceID()
	log.Printf("[凭据保存] traceId=%s 文件=%s 阶段=开始 说明=先写入磁盘，成功后再更新当前内存凭据", traceID, cfg.AuthFile)
	dir := filepath.Dir(cfg.AuthFile)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}
	data, err := json.MarshalIndent(sa, "", "  ")
	if err != nil {
		log.Printf("[凭据保存] traceId=%s 阶段=编码 结果=失败 原因=%v 业务影响=当前内存凭据未切换", traceID, err)
		return err
	}
	if err := os.WriteFile(cfg.AuthFile, data, 0600); err != nil {
		log.Printf("[凭据保存] traceId=%s 阶段=写入磁盘 结果=失败 原因=%v 业务影响=当前内存凭据未切换", traceID, err)
		return err
	}
	clearDisabledMarker(cfg.AuthFile)
	authLock.Lock()
	currAuth = sa
	authLock.Unlock()
	log.Printf("[凭据保存] traceId=%s 文件=%s 结果=成功 说明=磁盘写入完成，当前内存凭据已更新", traceID, cfg.AuthFile)
	return nil
}

// saveAuthTo 将凭据写入指定路径（多账号模式使用）。
// 写入成功后清除该路径的失效标记（表示账号已重新登录）。
func saveAuthTo(path string, sa *StoredAuth) error {
	registerCredentialSecrets(sa)
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}
	data, err := json.MarshalIndent(sa, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	clearDisabledMarker(path)
	return nil
}

// loadAccountFile 从指定路径读取并解析凭据（不修改全局缓存）。
func loadAccountFile(path string) (*StoredAuth, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取凭据文件失败 (%s): %w", path, err)
	}
	var sa StoredAuth
	if err := json.Unmarshal(data, &sa); err != nil {
		return nil, fmt.Errorf("解析凭据文件失败 (%s): %w", path, err)
	}
	if sa.Auth.AccessToken == "" {
		return nil, fmt.Errorf("凭据文件缺少 AccessToken (%s)", path)
	}
	registerCredentialSecrets(&sa)
	return &sa, nil
}

// isCredentialFile 判断自动发现模式下文件名是否为凭据文件。
// 排除运行时产物：workbuddy-status.json（monitor 状态快照）等。
func isCredentialFile(name string) bool {
	if !strings.HasPrefix(name, "workbuddy") || !strings.HasSuffix(name, ".json") {
		return false
	}
	return name != statusSnapshotFile
}

// collectConfiguredAuthPaths 解析应加载的凭据路径列表：
//  1. -auth-dir 指定目录 → 目录下所有 workbuddy*.json
//  2. 显式 -auth → 逗号分隔的凭据文件列表（保持用户顺序）
//  3. 均未指定（自动发现模式）→ 扫描当前目录下所有 workbuddy*.json，
//     这样把多个凭据文件放进工作目录即可自动组成多账号池；若一个都没有则回退默认 cfg.AuthFile
func collectConfiguredAuthPaths() []string {
	var paths []string

	if cfg.AuthDir != "" {
		entries, err := os.ReadDir(cfg.AuthDir)
		if err == nil {
			for _, e := range entries {
				if e.IsDir() || !isCredentialFile(e.Name()) {
					continue
				}
				paths = append(paths, filepath.Join(cfg.AuthDir, e.Name()))
			}
			sort.Strings(paths)
		}
		return paths
	}

	if cfg.AuthExplicit {
		for _, p := range strings.Split(cfg.AuthFile, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				paths = append(paths, p)
			}
		}
		return paths
	}

	// 自动发现模式：扫描当前工作目录（systemd WorkingDirectory 即凭据目录）
	entries, err := os.ReadDir(".")
	if err == nil {
		for _, e := range entries {
			if e.IsDir() || !isCredentialFile(e.Name()) {
				continue
			}
			paths = append(paths, e.Name())
		}
		sort.Strings(paths)
	}
	if len(paths) == 0 {
		// 无任何凭据文件时回退默认路径，便于给出"请先 login"的友好提示
		paths = append(paths, cfg.AuthFile)
	}
	return paths
}

// loadAccounts 构建账号池。
// -auth 支持逗号分隔多个凭据文件；-auth-dir 自动加载目录下所有 workbuddy*.json；
// 两者都未指定时自动发现当前目录下所有 workbuddy*.json（多账号免参数）。
func loadAccounts() error {
	accountMu.Lock()
	defer accountMu.Unlock()

	accounts = nil
	rrIndex = 0

	for _, p := range collectConfiguredAuthPaths() {
		sa, err := loadAccountFile(p)
		if err != nil {
			log.Printf("[Auth] 跳过无效凭据文件 %s: %v", p, err)
			continue
		}
		fp := ""
		if st, statErr := os.Stat(p); statErr == nil {
			fp = accountFingerprint(st)
		}
		accounts = append(accounts, &Account{Path: p, Auth: sa, Edition: profileForEdition(sa.Edition).Key, fingerprint: fp})
	}

	// 恢复持久化的失效账号标记（凭据文件已删除，仅供控制台提示重新登录）
	loadDisabledMarkers()
	restoreAccountRuntimeStateLocked()
	restoreStatsAccountStatesLocked()

	if len(accounts) == 0 {
		return fmt.Errorf("未找到有效凭据，请先执行 login 命令扫码登录")
	}
	return nil
}

// restoreAccountRuntimeStateLocked 从 monitor 快照恢复额度与模型级账本；调用方持有 accountMu。
func restoreAccountRuntimeStateLocked() {
	data, err := os.ReadFile(statusSnapshotFile)
	if err != nil {
		return
	}
	var snap statusSnapshot
	if json.Unmarshal(data, &snap) != nil {
		return
	}
	byPath := make(map[string]accountSnapshot, len(snap.Accounts))
	for _, state := range snap.Accounts {
		byPath[state.Path] = state
	}
	for _, acc := range accounts {
		state, ok := byPath[acc.Path]
		if !ok || acc.Disabled {
			continue
		}
		acc.QuotaTotal = state.QuotaTotal
		acc.QuotaUsed = state.QuotaUsed
		acc.QuotaRemaining = state.QuotaRemaining
		acc.IsPaidUser = state.IsPaidUser
		acc.PlanLabel = state.PlanLabel
		acc.PlanCheckedAt = state.PlanCheckedAt
		acc.PlanStale = state.PlanStale || state.PlanCheckedAt == 0 ||
			time.Now().Unix()-state.PlanCheckedAt >= 300
		if acc.PlanCheckedAt == 0 {
			acc.PlanLabel = planUnknown // 不沿用旧版本仅凭试用标记猜出的标签。
		}
		acc.QuotaKnown = state.QuotaKnown
		acc.QuotaUnlimited = state.QuotaUnlimited
		acc.QuotaExhausted = state.QuotaExhausted && !state.QuotaUnlimited
		if len(state.ModelStates) == 0 {
			continue
		}
		acc.ModelStates = make(map[string]*modelRuntimeState, len(state.ModelStates))
		for model, saved := range state.ModelStates {
			restored := &modelRuntimeState{
				CostClass: saved.CostClass, CooldownUntil: timeFromUnix(saved.CooldownUntil),
				QuotaBlocked: saved.QuotaBlocked, NextProbeAt: timeFromUnix(saved.NextProbeAt),
				LastReason: saved.LastReason, ObservedAt: timeFromUnix(saved.ObservedAt),
				CostPer1k: saved.CostPer1k, CostSamples: saved.CostSamples,
			}
			sanitizeRestoredModelState(restored)
			acc.ModelStates[normalizeModelName(model)] = restored
		}
	}
}

// sanitizeRestoredModelState 修正历史版本写入快照的脏数据。
//
// 旧版本在收到 14018（账号额度耗尽）时会错误地把 CostClass 覆盖成 paid，
// 但 14018 只说明该账号没额度，不能证明模型收费。快照里若出现
// 「QuotaBlocked=true + CostClass=paid + 原因是 14018」这一旧版本特征组合，
// 就把收费属性降回 unknown，交给后续 usage.credit 证据重新学习。
// 新版本不会再产生这种组合（markModelQuotaBlocked 不再改 CostClass）。
func sanitizeRestoredModelState(state *modelRuntimeState) {
	if state == nil || !state.QuotaBlocked || state.CostClass != modelCostPaid {
		return
	}
	reason := strings.ToLower(state.LastReason)
	if strings.Contains(reason, "14018") || strings.Contains(reason, "credits exhausted") ||
		strings.Contains(state.LastReason, "额度已用尽") {
		state.CostClass = modelCostUnknown
	}
}

func timeFromUnix(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.Unix(value, 0)
}

func unixOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.Unix()
}

// -----------------------------------------------------------------------------
// 凭据热加载：serve 运行期间自动发现凭据文件的新增/更新/删除，免重启收敛账号池
// -----------------------------------------------------------------------------

// accountFingerprint 基于文件 mtime+size 生成凭据文件变更指纹。
func accountFingerprint(st os.FileInfo) string {
	return fmt.Sprintf("%d:%d", st.ModTime().UnixNano(), st.Size())
}

// reloadAccounts 将磁盘上的凭据文件与内存账号池原地收敛（热加载）：
//   - 新增凭据文件 → 自动加入账号池；
//   - 凭据内容变化（重新登录/手动更新）→ 原地替换凭据、清除冷却/失效状态并恢复调度；
//   - 凭据文件被删除 → 移出账号池（已写入失效标记的幻影账号保留，用于提示重新登录）。
//
// 返回本次是否发生变更。调用方需自行处理 accountMu 之外的快照刷新。
func reloadAccounts() bool {
	changed, _ := reloadAccountsResult()
	return changed
}

func reloadAccountsResult() (bool, error) {
	accountMu.Lock()
	defer accountMu.Unlock()
	if cfg.AuthDir != "" || !cfg.AuthExplicit {
		dir := cfg.AuthDir
		if dir == "" {
			dir = "."
		}
		if _, err := os.ReadDir(dir); err != nil {
			return false, fmt.Errorf("凭据目录不可读取，账号池保持不变")
		}
	}

	changed := false
	var failures []error
	seen := make(map[string]bool)

	for _, p := range collectConfiguredAuthPaths() {
		st, err := os.Stat(p)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				failures = append(failures, fmt.Errorf("凭据文件无法读取"))
			}
			continue
		}
		seen[p] = true
		fp := accountFingerprint(st)

		var existing *Account
		for _, acc := range accounts {
			if acc.Path == p {
				existing = acc
				break
			}
		}

		if existing == nil {
			// 新凭据文件：加载并加入账号池（文件可能正在写入，失败则下轮重试）
			sa, err := loadAccountFile(p)
			if err != nil {
				failures = append(failures, fmt.Errorf("新增凭据不可解析，等待下轮重试"))
				log.Printf("[Reload] 凭据文件 %s 暂不可解析，下轮重试: %v", p, err)
				continue
			}
			accounts = append(accounts, &Account{
				Path:        p,
				Auth:        sa,
				Edition:     profileForEdition(sa.Edition).Key,
				fingerprint: fp,
			})
			log.Printf("[Reload] 发现新账号凭据 %s（%s），已自动加入账号池", p, profileForEdition(sa.Edition).Label)
			changed = true
			continue
		}

		if existing.fingerprint == fp {
			continue // 未变化
		}

		// 内容变化：原地替换凭据（重新登录或手动更新）
		sa, err := loadAccountFile(p)
		if err != nil {
			failures = append(failures, fmt.Errorf("已更新凭据不可解析，保留旧内容"))
			// 不更新指纹，下一轮重试（正常写入窗口极短，几乎必在下轮成功）
			log.Printf("[Reload] 凭据文件 %s 变更但暂不可解析，保留旧凭据: %v", p, err)
			continue
		}
		recovered := existing.Disabled
		existing.Auth = sa
		existing.Edition = profileForEdition(sa.Edition).Key
		existing.Disabled = false
		existing.DisabledReason = ""
		existing.CooldownUntil = time.Time{}
		existing.CooldownMsg = ""
		existing.QuotaKnown = false
		existing.QuotaExhausted = false
		existing.QuotaUnlimited = false
		existing.ModelStates = nil
		existing.fingerprint = fp
		clearDisabledMarker(p)
		if recovered {
			log.Printf("[Reload] 账号 %s 重新登录成功，已自动恢复调度", p)
		} else {
			log.Printf("[Reload] 账号 %s 凭据已更新（热加载，无需重启）", p)
		}
		changed = true
	}

	// 收敛：移除已删除的凭据（失效幻影账号保留）
	kept := make([]*Account, 0, len(accounts))
	for _, acc := range accounts {
		if !seen[acc.Path] && !acc.Disabled {
			log.Printf("[Reload] 凭据文件 %s 已删除，已移出账号池", acc.Path)
			changed = true
			continue
		}
		kept = append(kept, acc)
	}
	accounts = kept
	if len(accounts) > 0 {
		rrIndex %= len(accounts)
	} else {
		rrIndex = 0
	}
	return changed, errors.Join(failures...)
}

// accountReloaderLoop serve 后台凭据热加载协程：周期扫描凭据变化并原地收敛账号池。
func accountReloaderLoop() {
	interval := time.Duration(cfg.ReloadInterval) * time.Second
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastLogged time.Time
	var previousStatus string
	logScheduledTask("credential_reload", "scheduled", "", newTraceID(), time.Time{}, time.Now().Add(interval), "")
	for tick := range ticker.C {
		next := tick.Add(interval)
		started := time.Now()
		changed, err := reloadAccountsResult()
		if changed {
			writeStatusSnapshot() // 立即刷新 monitor 状态文件
			requestQuotaScan()
			requestCheckin()
			// 目录刷新成功后会自动触发价格探测；这里再直接触发一次，
			// 保证「原本没有某站点账号、后来加入」时也能立刻对该站点模型探测。
			requestModelsScan()
			requestModelsProbe()
		}
		status, detail := "success", "扫描完成，凭据没有变化"
		if changed {
			detail = "账号池变化已应用，相关刷新已通知"
		}
		if err != nil {
			status, detail = "partial", safeDebugError(err)
			if strings.HasPrefix(err.Error(), "凭据目录不可读取") {
				status = "failed"
			}
		}
		logFrequentTask("credential_reload", status, detail, started, next, &lastLogged, &previousStatus, changed)
	}
}

func normalizeModelName(model string) string {
	return strings.ToLower(strings.TrimSpace(model))
}

// parseExtraModels 解析 -extra-models 的逗号分隔列表：去空白、忽略空项、
// 大小写归一（normalizeModelName，与目录键同口径），保持首次出现顺序并去重。
func parseExtraModels(raw string) []string {
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		p = normalizeModelName(p)
		dup := false
		for _, existing := range out {
			if existing == p {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, p)
		}
	}
	return out
}

func modelStateLocked(acc *Account, model string) *modelRuntimeState {
	if acc.ModelStates == nil {
		acc.ModelStates = make(map[string]*modelRuntimeState)
	}
	model = normalizeModelName(model)
	state := acc.ModelStates[model]
	if state == nil {
		state = &modelRuntimeState{CostClass: modelCostUnknown}
		acc.ModelStates[model] = state
	}
	return state
}

// modelCostTier 成本分层（wb2api costTier 口径，移植裁剪版）：
//
//	0 = 已实测免费（限免期/夜间免费，最强偏好）
//	1 = 无观测或观测过期（学习期账号不饿死）
//	2 = 已实测收费（兜底层）
//
// 观测超过 modelCostTTL 即视为过期（时段性优惠不复活）。
func modelCostTier(state *modelRuntimeState, now time.Time) int {
	if state == nil {
		return 1
	}
	if !state.ObservedAt.IsZero() && now.Sub(state.ObservedAt) > modelCostTTL {
		return 1
	}
	switch state.CostClass {
	case modelCostFree:
		return 0
	case modelCostPaid:
		return 2
	default:
		return 1
	}
}

// modelCostPer1kOf 读取账号某模型的实测单价（EMA）；无有效观测（含观测过期）返回 false。
func modelCostPer1kOf(acc *Account, model string, now time.Time) (float64, bool) {
	if acc == nil {
		return 0, false
	}
	state := acc.ModelStates[normalizeModelName(model)]
	if state == nil || state.CostSamples == 0 {
		return 0, false
	}
	if !state.ObservedAt.IsZero() && now.Sub(state.ObservedAt) > modelCostTTL {
		return 0, false
	}
	return state.CostPer1k, true
}

func usableForModelLocked(acc *Account, model string, now time.Time) (accountSelectionKind, bool) {
	if acc.Disabled || acc.CooldownUntil.After(now) {
		return "", false
	}
	model = normalizeModelName(model)
	state := acc.ModelStates[model]
	if state == nil {
		if !acc.QuotaExhausted {
			return selectionNormal, true
		}
		return selectionProbeExhausted, true
	}
	if state.CooldownUntil.After(now) {
		return "", false
	}
	if state.QuotaBlocked {
		return "", false
	}
	if !acc.QuotaExhausted {
		return selectionNormal, true
	}
	// 成本分层（TTL 感知，见 modelCostTier）：实测免费 → 免费耗尽路径；
	// 实测收费 → 本轮不可用（由有余额账号兜底）；未知/观测过期 → 受控探测路径。
	switch modelCostTier(state, now) {
	case 0:
		return selectionFreeExhausted, true
	case 2:
		return "", false
	}
	if now.Before(state.NextProbeAt) {
		return "", false
	}
	return selectionProbeExhausted, true
}

// nextAccountForModel 按「免费耗尽账号优先 → 有余额账号 → 零余额未知模型受控探测」选号。
func nextAccountForModel(model string, attempted map[*Account]bool) (*Account, accountSelectionKind, error) {
	// 站点优先级必须在加锁前计算：preferredFreeSites 会读取账号账本并自行获取 accountMu，
	// 若在持锁期间调用会自锁。语义：该模型「一个站点免费、另一个站点收费」时优先用免费站点，
	// 直到该站点账号全部不可用；其余情况不搞优先，正常轮询。
	preferred := preferredFreeSites(model)

	accountMu.Lock()
	defer accountMu.Unlock()

	if len(accounts) == 0 {
		return nil, "", fmt.Errorf("账号池为空")
	}
	allowed := 0
	for _, acc := range accounts {
		if ok, _ := modelAccountAllowed(model, acc, accounts); ok {
			allowed++
		}
	}
	if allowed == 0 && modelAccountRuleConfigured(model) {
		return nil, "", fmt.Errorf("%w: 模型 %s 没有符合账号黑白名单的凭据文件", errModelAccountPolicy, model)
	}
	now := time.Now()

	type pickCandidate struct {
		idx  int
		acc  *Account
		kind accountSelectionKind
		tier int
	}
	// 选择优先级：免费耗尽 > 有余额 > 受控探测（原语义不变）；同优先级内叠加
	// 成本分层：已实测免费(0) > 未知/过期(1) > 已实测收费(2)，收费层内单价低者优先。
	kindRank := func(k accountSelectionKind) int {
		switch k {
		case selectionFreeExhausted:
			return 0
		case selectionNormal:
			return 1
		default:
			return 2
		}
	}
	pick := func(siteFilter func(string) bool) (*Account, accountSelectionKind, bool) {
		var cands []pickCandidate
		for i := range len(accounts) {
			idx := (rrIndex + i) % len(accounts)
			acc := accounts[idx]
			if attempted != nil && attempted[acc] {
				continue
			}
			if ok, _ := modelAccountAllowed(model, acc, accounts); !ok {
				continue
			}
			if siteFilter != nil && !siteFilter(accSiteLocked(acc)) {
				continue
			}
			kind, ok := usableForModelLocked(acc, model, now)
			if !ok {
				continue
			}
			cands = append(cands, pickCandidate{
				idx: idx, acc: acc, kind: kind,
				tier: modelCostTier(acc.ModelStates[normalizeModelName(model)], now),
			})
		}
		if len(cands) == 0 {
			return nil, "", false
		}
		best := cands[0]
		for _, c := range cands[1:] {
			if kindRank(c.kind) < kindRank(best.kind) ||
				(kindRank(c.kind) == kindRank(best.kind) && c.tier < best.tier) {
				best = c
			}
		}
		// 收费层内择优：单价低者优先（无有效单价的候选保持轮询序）。
		if best.kind == selectionNormal && best.tier == 2 {
			bestCost, hasBest := modelCostPer1kOf(best.acc, model, now)
			for _, c := range cands {
				if c.kind != selectionNormal || c.tier != 2 {
					continue
				}
				cost, ok := modelCostPer1kOf(c.acc, model, now)
				if !ok {
					continue
				}
				if !hasBest || cost < bestCost {
					best, bestCost, hasBest = c, cost, true
				}
			}
		}
		// costTier 条件探索（移植自 wb2api issue #136 方案 a′）：最优候选全在免费层
		// （tier 0 垄断）且存在未知层（tier 1）候选时，按探索窗口把一次选号改道给
		// 未知账号搭车学习——承接真实用户请求，零新增上游调用；学成即毕业。
		if model != "" && best.tier == 0 && cfg.CostExploreInterval > 0 {
			var explore *pickCandidate
			for i := range cands {
				c := &cands[i]
				if c.tier != 1 {
					continue
				}
				// 优先有余额账号（普通路径），其次受控探测路径。
				if explore == nil || (explore.kind != selectionNormal && c.kind == selectionNormal) {
					explore = c
				}
			}
			if explore != nil && now.Sub(costExploreLast[model]) >= cfg.CostExploreInterval {
				costExploreLast[model] = now
				costExploreEvents++
				log.Printf("[CostExplore] 模型 %s 免费层垄断，按窗口（%v）改道一次给未知账号 %s 搭车学习",
					model, cfg.CostExploreInterval, explore.acc.Path)
				best = *explore
				if best.kind == selectionProbeExhausted {
					// 改道到受控探测路径时换独立类型：失败按常规轮转回退继续本次
					// 请求，而不是按纯探测语义短路 503（免费层账号本可服务）。
					best.kind = selectionExploreProbe
				}
			}
		}
		if best.kind == selectionProbeExhausted || best.kind == selectionExploreProbe {
			modelStateLocked(best.acc, model).NextProbeAt = now.Add(modelProbeDelay)
		}
		rrIndex = (best.idx + 1) % len(accounts)
		return best.acc, best.kind, true
	}

	if len(preferred) > 0 {
		if acc, kind, ok := pick(func(site string) bool { return preferred[site] }); ok {
			return acc, kind, nil
		}
		log.Printf("[FreeSite] 模型 %s 的免费站点账号当前均不可用，回退到其余站点代偿", model)
	}
	if acc, kind, ok := pick(nil); ok {
		return acc, kind, nil
	}

	disabled, accountCooling, modelCooling, quotaBlocked, probeWaiting, policyBlocked := 0, 0, 0, 0, 0, 0
	earliest := time.Time{}
	for _, acc := range accounts {
		if ok, _ := modelAccountAllowed(model, acc, accounts); !ok {
			policyBlocked++
			continue
		}
		switch {
		case acc.Disabled:
			disabled++
		case acc.CooldownUntil.After(now):
			accountCooling++
			if earliest.IsZero() || acc.CooldownUntil.Before(earliest) {
				earliest = acc.CooldownUntil
			}
		default:
			state := acc.ModelStates[normalizeModelName(model)]
			if state == nil {
				if acc.QuotaExhausted {
					probeWaiting++
				}
				continue
			}
			if state.CooldownUntil.After(now) {
				modelCooling++
				if earliest.IsZero() || state.CooldownUntil.Before(earliest) {
					earliest = state.CooldownUntil
				}
			} else if state.QuotaBlocked || acc.QuotaExhausted && modelCostTier(state, now) == 2 {
				quotaBlocked++
			} else if acc.QuotaExhausted && now.Before(state.NextProbeAt) {
				probeWaiting++
			}
		}
	}
	msg := fmt.Sprintf("当前模型 %s 暂无可用账号：账号名单排除=%d，授权失效=%d，账号冷却=%d，模型冷却=%d，额度阻断=%d，等待探测=%d", model, policyBlocked, disabled, accountCooling, modelCooling, quotaBlocked, probeWaiting)
	if !earliest.IsZero() {
		msg += "，最早恢复=" + earliest.Format("2006-01-02 15:04:05")
	}
	return nil, "", fmt.Errorf("%s", msg)
}

func nextAccount() (*Account, error) {
	acc, _, err := nextAccountForModel("", nil)
	return acc, err
}

// markCooldown 将账号屏蔽至指定时间。
func markCooldown(acc *Account, until time.Time, msg string) {
	accountMu.Lock()
	acc.CooldownUntil = until
	acc.CooldownMsg = msg
	accountMu.Unlock()
	log.Printf("[Cooldown] 账号 %s 触发频率限制，自动屏蔽至 %s (提示: %s)",
		acc.Path, until.Format("2006-01-02 15:04:05"), msg)
}

func markModelCooldown(acc *Account, model string, until time.Time, msg string) {
	accountMu.Lock()
	state := modelStateLocked(acc, model)
	state.CooldownUntil = until
	state.LastReason = msg
	accountMu.Unlock()
	log.Printf("[ModelCooldown] 账号 %s 模型 %s 触发模型级频率限制，仅屏蔽该模型至 %s；其他模型仍可调度", acc.Path, model, until.Format("2006-01-02 15:04:05"))
	writeStatusSnapshot()
}

// markModelQuotaBlocked 记录一次「账号额度耗尽」观测。
//
// 注意：14018 的语义是「该账号的额度用光了」，与模型是否收费无关。
// 上游对额度为 0 的账号会整体拒绝（实测连免费模型也返回 14018），
// 因此这里只标记 QuotaBlocked（该账号暂时无法服务该模型），
// 绝不覆盖 CostClass —— 模型收费属性只能由 usage.credit 证据决定。
func markModelQuotaBlocked(acc *Account, model, msg string) {
	accountMu.Lock()
	state := modelStateLocked(acc, model)
	oldClass := state.CostClass
	state.QuotaBlocked = true
	state.LastReason = msg
	state.ObservedAt = time.Now()
	accountMu.Unlock()
	log.Printf("[ModelQuota] 账号 %s 模型 %s 返回额度耗尽(14018)：仅阻断该账号对该模型的调度，账号额度恢复后自动解除；模型收费属性保持=%s，未被本次错误覆盖",
		acc.Path, model, ifEmpty(oldClass, modelCostUnknown))
	writeStatusSnapshot()
}

// disableAccount 将账号标记为失效（授权过期/撤销），禁止调度并删除凭据文件，
// 同时写入持久化失效标记，便于控制台提示用户重新登录。
func disableAccount(acc *Account, reason string) {
	accountMu.Lock()
	acc.Disabled = true
	acc.DisabledReason = reason
	acc.CooldownUntil = time.Time{}
	path := acc.Path
	nickname := ""
	uid := ""
	edition := ""
	var authSnapshot *StoredAuth
	if acc.Auth != nil {
		nickname = acc.Auth.Account.Nickname
		uid = acc.Auth.Account.UID
		edition = profileForEdition(acc.Auth.Edition).Key
		acc.Nickname = nickname
		acc.UID = uid
		acc.Edition = edition
		copied := *acc.Auth
		authSnapshot = &copied
	}
	accountMu.Unlock()

	// 写持久化失效标记（凭据删除后仍能在 status 中提示重新登录）
	marker := disabledMarker{
		Path:       path,
		Reason:     reason,
		DisabledAt: time.Now().Unix(),
		Nickname:   nickname,
		UID:        uid,
		Edition:    edition,
	}
	if data, err := json.MarshalIndent(marker, "", "  "); err == nil {
		if werr := os.WriteFile(markerPath(path), data, 0600); werr != nil {
			log.Printf("[Auth] 账号 %s 失效标记写入失败: %v", path, werr)
		}
	}

	// 删除凭据前先做一次只读校验：删除不可逆，只有确认凭据真的不可用才销毁。
	if keep, note := credentialStillUsableForDelete(authSnapshot); keep {
		log.Printf("[Auth] 账号 %s 已停止调度；%s；凭据文件保留在 %s（如需彻底移除请手动删除），重新登录会自动覆盖",
			path, note, path)
		writeStatusSnapshot()
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Printf("[Auth] 账号 %s 凭据文件删除失败: %v", path, err)
	}
	log.Printf("[Auth] 账号 %s 授权失效，已禁止调度并删除凭据文件（删除前只读校验确认不可用）: %s", path, reason)
	writeStatusSnapshot()
}

// loadDisabledMarkers 扫描失效标记文件，将其恢复为账号池中的失效账号（Auth 为 nil）。
func loadDisabledMarkers() {
	// 收集标记文件路径：
	// - AuthDir 模式：目录下 *.json.disabled 即为标记文件
	// - 显式 -auth 模式：<凭据路径>.disabled 为标记文件
	// - 自动发现模式：扫描当前目录下所有 *.json.disabled 标记文件
	var markerPaths []string
	if cfg.AuthDir != "" {
		entries, err := os.ReadDir(cfg.AuthDir)
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasSuffix(name, ".json.disabled") {
				markerPaths = append(markerPaths, filepath.Join(cfg.AuthDir, name))
			}
		}
	} else if cfg.AuthExplicit {
		for _, p := range strings.Split(cfg.AuthFile, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				markerPaths = append(markerPaths, markerPath(p))
			}
		}
	} else {
		// 自动发现模式：扫描当前工作目录
		entries, err := os.ReadDir(".")
		if err == nil {
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				name := e.Name()
				if strings.HasPrefix(name, "workbuddy") && strings.HasSuffix(name, ".json.disabled") {
					markerPaths = append(markerPaths, name)
				}
			}
			sort.Strings(markerPaths)
		}
	}

	for _, mp := range markerPaths {
		data, err := os.ReadFile(mp)
		if err != nil {
			continue
		}
		var m disabledMarker
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		// 凭据文件比标记更新，说明用户已重新登录或手动续期，旧标记已过期。
		if fi, statErr := os.Stat(m.Path); statErr == nil {
			if mi, mErr := os.Stat(mp); mErr == nil && fi.ModTime().After(mi.ModTime()) {
				log.Printf("[Auth] 账号 %s 凭据文件比失效标记更新，视为已重新登录，清除失效标记", m.Path)
				_ = os.Remove(mp)
				continue
			}
		}
		// 凭据文件仍在时，标记负责把该账号重新置为失效（保留文件是为了可恢复）。
		applied := false
		for _, a := range accounts {
			if a.Path != m.Path {
				continue
			}
			a.Disabled = true
			a.DisabledReason = m.Reason
			if a.Nickname == "" {
				a.Nickname = m.Nickname
			}
			if a.UID == "" {
				a.UID = m.UID
			}
			applied = true
			break
		}
		if applied {
			continue
		}
		accounts = append(accounts, &Account{
			Path:           m.Path,
			Disabled:       true,
			DisabledReason: m.Reason,
			Nickname:       m.Nickname,
			UID:            m.UID,
			Edition:        m.Edition,
		})
	}
}

// clearDisabledMarker 在重新登录成功后清除失效标记。
func clearDisabledMarker(path string) {
	mp := markerPath(path)
	if err := os.Remove(mp); err != nil && !os.IsNotExist(err) {
		log.Printf("[Auth] 失效标记清除失败 (%s): %v", mp, err)
	}
}

// hasBusinessEnvelope 报告响应体是否带上游业务信封（含 `"code":` 或 `"msg":`）。
//
// 来源：移植自 Sliverkiss/workbuddy2api 的 internal/upstream/client.go
// （MIT License, Copyright (c) 2026 Sliverkiss），逐字保留。
//
// 不做 JSON 解析：信封存在性只需字段名命中。畸形 JSON 但含 `"msg":` 字样仍按
// 业务响应保守处理——宁漏判 WAF 也不误罚业务 403（后者有各自的权威分类）。
func hasBusinessEnvelope(body string) bool {
	return strings.Contains(body, `"code":`) || strings.Contains(body, `"msg":`)
}

// errKind 上游错误的权威分类。调度层据此决定「罚谁」：
//
//	账号的问题 → 冷却/禁用该账号，换号重试
//	请求的问题 → 不罚账号，直接透传（换任何账号都会得到同样结果）
//
// 移植自 wb2api 的 ErrKind，按 wbgw 的调度语义裁剪：只保留影响调度决策的
// 分类，观测类（如 ErrServer 细分）合并。
type errKind int

const (
	errNone           errKind = iota // 成功 / 未分类
	errWafBlock                      // 403 + 无业务信封：IP 级风控，账号健康 → 软冷却
	errSessionDead                   // 401 / 12153 offline session：真授权失效 → 禁用
	errAccountFault                  // 11140 request illegal / 14017 trial：账号级故障 → 冷却轮换
	errHardCredit                    // 402 / 14018 余额耗尽 → 硬冷却至次日
	errSoftRate                      // 429 / 限流文案 → 对齐上游重置时间
	errModelBlocked                  // 6004 模型级限流 / 11102 无此模型 → 只冷却该模型
	errNotFound                      // 404 上游偶发 → 短冷却
	errServer                        // 5xx 上游故障 → 换号
	errContentBlocked                // 400 + 审核文案：请求问题，不罚账号
	errBadParams                     // 400 + 11101 解析失败：请求问题，不罚账号
	errPromptTooLong                 // 11115 上下文超限：请求问题，不罚账号且不轮转
	errClient                        // 其他 4xx：换号（不同账号模型权限可能不同）
)

func (k errKind) String() string {
	switch k {
	case errWafBlock:
		return "waf_block"
	case errSessionDead:
		return "session_dead"
	case errAccountFault:
		return "account_fault"
	case errHardCredit:
		return "hard_credit"
	case errSoftRate:
		return "soft_rate"
	case errModelBlocked:
		return "model_blocked"
	case errNotFound:
		return "not_found"
	case errServer:
		return "server"
	case errContentBlocked:
		return "content_blocked"
	case errBadParams:
		return "bad_params"
	case errPromptTooLong:
		return "prompt_too_long"
	case errClient:
		return "client"
	default:
		return "none"
	}
}

// punishesAccount 报告该分类是否应归咎于账号（冷却/禁用）。
// 请求级错误（内容审核、参数畸形、上下文超限）换任何账号都会复现，
// 罚账号只会白白消耗健康号的可用性。
func (k errKind) punishesAccount() bool {
	switch k {
	case errContentBlocked, errBadParams, errPromptTooLong:
		return false
	default:
		return true
	}
}

// rotatesAccount 报告该分类是否值得换号重试。
// promptTooLong 例外：同一 body 换任何账号都超限，轮转纯属浪费。
func (k errKind) rotatesAccount() bool {
	return k != errPromptTooLong
}

// containsAnyFold 报告 body 是否含任一子串（大小写不敏感）。
func containsAnyFold(body string, patterns ...string) bool {
	low := strings.ToLower(body)
	for _, p := range patterns {
		if strings.Contains(low, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// isContentBlocked 内容策略拦截：HTTP 400 + 审核文案。
//
// 上游按逐字指纹审核，客户端注入的模板句（Claude Code / Codex 的 system 指令）
// 会触发此类拦截。这是误报——账号余额健康、未限流、session 未死，属请求问题。
func isContentBlocked(statusCode int, body string) bool {
	if statusCode < 400 {
		return false
	}
	return containsAnyFold(body,
		"blocked by security policy",
		"unapproved channel",
		"illegal api invocation",
	)
}

// isBadParams 请求级参数错误（HTTP 400 + code 11101 / 11129）。
// 发给上游的 body 有问题，换账号照样 400：
//   - 11101 / "Unmarshal chat params failed"：请求体解析失败；
//   - 11129 / "invalid function call parameters"：工具定义不符合模型要求
//     （如 parameters 缺 type 声明）。二者都是确定性的请求侧缺陷。
func isBadParams(statusCode int, body string) bool {
	if statusCode < 400 {
		return false
	}
	return containsAnyFold(body, "Unmarshal chat params failed") ||
		strings.Contains(body, `"code":11101`) ||
		strings.Contains(body, `"code":11129`) ||
		containsAnyFold(body, "invalid function call parameters")
}

// isPromptTooLong 上下文超限（code 11115 / "prompt is too long"）。
// 只在请求级 4xx 上判定：429 属限流语义优先，5xx 属服务端故障优先。
func isPromptTooLong(statusCode int, body string) bool {
	if statusCode != http.StatusBadRequest && statusCode != http.StatusNotFound &&
		statusCode != http.StatusRequestEntityTooLarge {
		return false
	}
	return strings.Contains(body, `"code":11115`) ||
		strings.Contains(body, `"code":"11115"`) ||
		containsAnyFold(body, "prompt is too long")
}

// isAccountFault 账号级授权/配额故障：这类错误由账号自身状态决定，
// 不是请求格式、不是临时限流、也不是内容误报——继续重试只会反复撞风控。
//
//   - "request illegal"（11140）→ 账号级授权风控，需重新登录
//   - code 14017 / "trial not activated" → 试用未激活账号
//
// 注意 11140 不能按 code 判定：同一 code 也承载模型级限流文案，
// 那种场景须保持 soft_rate（由 isRateLimited 先行命中）。
func isAccountFault(body string) bool {
	return containsAnyFold(body,
		"request illegal",
		"trial not activated",
		"trial version is not yet activated",
	)
}

// isSessionDead 会话失效（401 / 12153 offline session not found）。
func isSessionDead(statusCode int, body string) bool {
	if statusCode == http.StatusUnauthorized {
		return true
	}
	return containsAnyFold(body, "Offline user session not found") ||
		strings.Contains(body, "12153")
}

// isModelNotFound 该后端无此模型（11102），只认 400/404。
func isModelNotFound(statusCode int, body string) bool {
	if statusCode != http.StatusBadRequest && statusCode != http.StatusNotFound {
		return false
	}
	return strings.Contains(body, `"code":11102`) ||
		containsAnyFold(body, "service info not found")
}

// classifyUpstream 是上游错误的权威分类入口。判定顺序即优先级：
// 越具体的语义越先判，且**保持 wbgw 既有分支链的相对次序**，
// 新分类只插入到不改变既有判定的位置。
func classifyUpstream(statusCode int, body string) errKind {
	// 11102 最先判：它是「模型在后端不存在」的确定性答复，语义最具体。
	// 若落到 4xx 兜底，坏号会留在池内反复被选中。
	if isModelNotFound(statusCode, body) {
		return errModelBlocked
	}
	// 以下三层保持原 wbgw 顺序（余额 → 模型级限流 → 账号级限流）。
	// 注意 isQuotaExhausted 自带 429 判定（要求 14018 code 或具体文案），
	// 不可像 wb2api 那样把裸 429 提前——那会吞掉 14018 的余额语义。
	if isQuotaExhausted(statusCode, body) {
		return errHardCredit
	}
	if isModelRateLimited(body) {
		return errModelBlocked
	}
	if isRateLimited(statusCode, body) {
		return errSoftRate
	}
	// WAF 403 先于授权失效（原顺序）：无业务信封的 403 是 IP 级风控，
	// 落 errSessionDead 会 disableAccount 删凭据。
	if isWafBlocked(statusCode, body) {
		return errWafBlock
	}
	// 账号级终态：session 失效 / 授权故障。二者都需人工介入才能恢复，
	// 区别在于是否删除凭据（session_dead 删，account_fault 保留）。
	if isSessionDead(statusCode, body) {
		return errSessionDead
	}
	if isAccountFault(body) {
		return errAccountFault
	}
	// 授权失效文案（invalid token / 登录已过期等）：原 isAuthFailure 的其余分支。
	if isAuthFailure(statusCode, body) {
		return errSessionDead
	}
	// 请求级错误先于状态码兜底：这些分类不罚账号，误判代价是白白冷却健康号。
	if isPromptTooLong(statusCode, body) {
		return errPromptTooLong
	}
	if statusCode == http.StatusNotFound {
		return errNotFound
	}
	if statusCode >= 500 {
		return errServer
	}
	if statusCode >= 400 {
		if isContentBlocked(statusCode, body) {
			return errContentBlocked
		}
		if isBadParams(statusCode, body) {
			return errBadParams
		}
		return errClient
	}
	return errNone
}

// isWafBlocked 判断 403 响应是否为 WAF 拦截形态。
//
// 判定口径（移植自 wb2api 的 IsWafBlocked）：HTTP 403 且 body 无业务信封。
// APISIX WAF 拦截页返回 HTML（`<!DOCTYPE html>...<title>WAF Block Page</title>`）、
// 空体或纯文本，三者均命中；带业务信封的 403（如 11140 request illegal、
// 11-128 内容拦截）仍走 isAuthFailure 的既有文案判定，不受影响。
//
// 来源：Sliverkiss/workbuddy2api 的 internal/upstream/client.go
// （MIT License, Copyright (c) 2026 Sliverkiss），仅改名与裁剪注释。
//
// 为什么必须与授权失效分开：WAF 拦的是出口 IP 而非账号，账号本身健康。
// 若按授权失效处理会 disableAccount → os.Remove(凭据文件)，把有效期数月甚至
// 一年的凭据直接删掉，且需人工重新扫码登录才能恢复。
func isWafBlocked(statusCode int, body string) bool {
	return statusCode == http.StatusForbidden && !hasBusinessEnvelope(body)
}

// isAuthFailure 判断上游响应是否为授权失效（401/403 / invalid token / 登录过期等）。
//
// 403 的处理已收窄：仅当 body 带业务信封且命中失效文案时才算授权失效；
// 无信封的 403（WAF 拦截页/空体）由 isWafBlocked 单独识别，不在此列。
func isAuthFailure(statusCode int, body string) bool {
	if statusCode == http.StatusUnauthorized {
		return true
	}
	if statusCode == http.StatusForbidden && !hasBusinessEnvelope(body) {
		// WAF 拦截形态：不是授权失效（见 isWafBlocked）。
		return false
	}
	low := strings.ToLower(body)
	if strings.Contains(low, "invalid token") ||
		strings.Contains(low, "unauthorized") ||
		strings.Contains(low, "登录已过期") ||
		strings.Contains(low, "登录失效") ||
		strings.Contains(low, "token 已失效") ||
		strings.Contains(low, "authentication required") {
		return true
	}
	return false
}

var resetTimeRe = regexp.MustCompile(`将在\s*(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s*(UTC[+-]\d+(?::\d{2})?)?`)

// resetTimeGenericRe 兜底匹配任意「日期 + 时间(可选 UTC 偏移)」片段，
// 用于国际站等英文限流消息（如 "will reset at 2026-09-05 01:57:00 UTC+8"）。
var resetTimeGenericRe = regexp.MustCompile(`(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\s*(UTC[+-]\d+(?::\d{2})?))?`)

// parseResetTime 从上游 429 错误消息中解析频率限制重置时间。
// 国内站示例: "您的使用量已超出频率限制，将在 2026-09-04 07:48:15 UTC+8 重置"
// 国际站示例: "Your usage has exceeded the rate limit. It will reset at 2026-09-05 01:57:00 UTC+8."
func parseResetTime(s string) (time.Time, bool) {
	m := resetTimeRe.FindStringSubmatch(s)
	if m == nil {
		m = resetTimeGenericRe.FindStringSubmatch(s)
	}
	if m == nil {
		return time.Time{}, false
	}
	return parseDateTimeTZ(m[1], m[2], m[3])
}

// parseDateTimeTZ 按「日期 时间 (可选 UTC 偏移)」解析时间；未提供时区时默认按 UTC+8。
func parseDateTimeTZ(date, clock, zone string) (time.Time, bool) {
	offset := 8 * 3600 // 默认按 UTC+8 解析
	if zone != "" {
		z := zone
		z = strings.TrimPrefix(z, "UTC")
		z = strings.TrimPrefix(z, "utc")
		var h, mi int
		if _, err := fmt.Sscanf(z, "%d:%d", &h, &mi); err == nil {
			offset = h*3600 + mi*60
		} else if _, err := fmt.Sscanf(z, "%d", &h); err == nil {
			offset = h * 3600
		}
	}
	loc := time.FixedZone("UTC", offset)
	t, err := time.ParseInLocation("2006-01-02 15:04:05", date+" "+clock, loc)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// isRateLimited 判断上游响应是否属于频率限制（429 或 code 6004 / 频率限制提示，含中英文）。
func isRateLimited(statusCode int, body string) bool {
	if statusCode == http.StatusTooManyRequests {
		return true
	}
	if strings.Contains(body, `"code":6004`) || strings.Contains(body, "频率限制") || strings.Contains(body, "frequency limit") {
		return true
	}
	low := strings.ToLower(body)
	if strings.Contains(low, "rate limit") || strings.Contains(low, "ratelimit") || strings.Contains(low, "too many requests") {
		return true
	}
	return false
}

func isModelRateLimited(body string) bool {
	low := strings.ToLower(body)
	return strings.Contains(body, `"code":6004`) || strings.Contains(body, "切换其他模型") || strings.Contains(low, "switch to another model")
}

func isQuotaExhausted(statusCode int, body string) bool {
	if statusCode != http.StatusTooManyRequests && !strings.Contains(body, `"code":14018`) {
		return false
	}
	low := strings.ToLower(body)
	return strings.Contains(body, `"code":14018`) || strings.Contains(body, "额度已用尽") || strings.Contains(low, "credits exhausted")
}

// ensureValidTokenFor 对指定账号检查并刷新令牌（多账号版）。
// 失效账号（Disabled 或 Auth 为 nil）直接跳过，不参与调度。
func ensureValidTokenFor(acc *Account) error {
	if acc == nil {
		return nil
	}
	accountMu.Lock()
	disabled := acc.Disabled
	hasAuth := acc.Auth != nil
	expiresAt := int64(0)
	if hasAuth {
		expiresAt = acc.Auth.Auth.ExpiresAt
	}
	accountMu.Unlock()
	if disabled || !hasAuth {
		return nil
	}
	now := time.Now()
	if now.Unix() > expiresAt-900 {
		log.Printf("[Auth] 账号 %s Token 需要续期，当前过期时间=%s，刷新阈值=过期前15分钟，开始调用上游刷新接口", acc.Path, time.Unix(expiresAt, 0).Format("2006-01-02 15:04:05"))
		if err := doRefreshTokenFor(acc); err != nil {
			accountMu.Lock()
			isDisabled := acc.Disabled
			accountMu.Unlock()
			if !isDisabled {
				markCooldown(acc, now.Add(time.Minute), "Token 自动续期失败: "+err.Error())
			}
			return err
		}
	}
	return nil
}

func ensureValidToken() error {
	sa, err := loadAuth()
	if err != nil {
		return err
	}
	// 如果距过期不足 15 分钟，则自动刷新
	if time.Now().Unix() > sa.Auth.ExpiresAt-900 {
		log.Printf("[Auth] 访问令牌即将或已经过期 (ExpiresAt=%s)，正在自动刷新...", time.Unix(sa.Auth.ExpiresAt, 0).Format("2006-01-02 15:04:05"))
		return doRefreshToken(sa)
	}
	return nil
}

func doRefreshToken(sa *StoredAuth) error {
	if sa == nil || sa.Auth.RefreshToken == "" {
		return fmt.Errorf("无法刷新：缺少 RefreshToken")
	}
	if _, err := refreshTokenPayload(sa, nil); err != nil {
		return err
	}
	if err := saveAuth(sa); err != nil {
		return fmt.Errorf("写回凭据失败: %w", err)
	}
	log.Printf("[Auth] Token 刷新成功！新过期时间: %s", time.Unix(sa.Auth.ExpiresAt, 0).Format("2006-01-02 15:04:05"))
	return nil
}

// doRefreshTokenFor 刷新指定账号的令牌并保存回其凭据文件（多账号版）。
func doRefreshTokenFor(acc *Account) error {
	return refreshAccountToken(acc, "按需续期")
}

// refreshAccountToken 刷新指定账号的登录凭据并原子写回。
//
// 失败分两档处理：命中上游「登录态失效」标志（如 12153 / invalid_grant）或连续失败
// 达到 refreshDisableThreshold 次，才禁用账号；普通网络错误、5xx 以及未命中标志的
// 401/403 只累计计数并保留原凭据，避免一次抖动就删掉用户凭据。
func refreshAccountToken(acc *Account, reason string) error {
	if acc == nil {
		return fmt.Errorf("无法刷新：账号缺少 RefreshToken 或已失效")
	}

	// 与该账号的上游请求串行，避免刷新过程中请求读到一半更新的 Token。
	acc.lock.Lock()
	defer acc.lock.Unlock()

	accountMu.Lock()
	if acc.Disabled || acc.Auth == nil || acc.Auth.Auth.RefreshToken == "" {
		accountMu.Unlock()
		return fmt.Errorf("无法刷新：账号缺少 RefreshToken 或已失效")
	}
	refreshed := *acc.Auth
	oldTokens := refreshed.Auth // 覆盖前校验要比对旧凭据，先留存副本。
	path := acc.Path
	oldExpiresAt := refreshed.Auth.ExpiresAt
	accountMu.Unlock()

	log.Printf("[Auth] 账号 %s 开始刷新 Token，触发方式=%s，站点=%s，刷新前访问令牌过期时间=%s",
		path, reason, profileForEdition(refreshed.Edition).Label, time.Unix(oldExpiresAt, 0).Format("2006-01-02 15:04:05"))
	status, err := refreshTokenPayload(&refreshed, clientForAccount(acc))
	if err != nil {
		// WAF 拦截 refresh 端点：IP 级风控，账号凭据有效，只软冷却、不计入登录态失败次数。
		if isWafBlocked(status, err.Error()) {
			markCooldown(acc, time.Now().Add(wafCooldownBase), "WAF 拦截 (HTTP 403, refresh)")
			log.Printf("[Auth] 账号 %s Token 刷新被 WAF 拦截，软冷却 %v（未禁用账号，凭据保留）",
				path, wafCooldownBase)
			return err
		}
		kind := classifyRefreshFailure(status, err.Error())
		summary := refreshFailureSummary(kind, status, err)
		count := recordRefreshFailure(acc, summary)
		if count >= refreshDisableThreshold {
			log.Printf("[Auth] 账号 %s 判定登录态失效，已停止调度并写入失效标记；连续失败=%d 判定依据=%s", path, count, summary)
			disableAccount(acc, fmt.Sprintf("令牌刷新连续失败 %d 次: %s", count, summary))
		} else if kind == refreshFailureSessionDead {
			// 上游明确说登录态没了，但先不删凭据：保留原凭据继续重试，达到阈值才停止调度。
			log.Printf("[Auth] 账号 %s 续期被上游判定登录态失效（连续 %d/%d 次）：%s；暂时保留凭据，达到阈值后才停止调度，建议尽快重新登录",
				path, count, refreshDisableThreshold, summary)
		} else {
			log.Printf("[Auth] 账号 %s Token 刷新失败，旧凭据未覆盖；连续失败=%d/%d 判定=%s 业务影响=本次未续期，账号仍可用到访问令牌过期，稍后自动重试",
				path, count, refreshDisableThreshold, summary)
		}
		return err
	}
	// 覆盖前校验：新凭据必须仍属于同一账号且确实可用，否则保留旧凭据不覆盖。
	if err := validateRefreshedCredential(path, &refreshed, oldTokens); err != nil {
		accountMu.Lock()
		acc.LastRefreshError = "新凭据未通过覆盖前校验: " + err.Error()
		accountMu.Unlock()
		log.Printf("[Auth] 账号 %s 新凭据未通过覆盖前校验，已放弃本次覆盖并保留原凭据；原因=%v 业务影响=账号继续使用原凭据，不销毁、不降级",
			path, err)
		return err
	}
	if err := saveAuthTo(path, &refreshed); err != nil {
		log.Printf("[Auth] 账号 %s Token 已从上游刷新，但写回凭据失败，内存与磁盘均保留旧凭据: %v", path, err)
		return fmt.Errorf("写回凭据失败: %w", err)
	}
	fingerprint := ""
	if st, statErr := os.Stat(path); statErr == nil {
		fingerprint = accountFingerprint(st)
	}
	accountMu.Lock()
	if !acc.Disabled {
		acc.Auth = &refreshed
		acc.Edition = profileForEdition(refreshed.Edition).Key
		acc.fingerprint = fingerprint
		acc.RefreshFailCount = 0
		acc.LastRefreshError = ""
	}
	accountMu.Unlock()
	newExpiresAt := refreshed.Auth.ExpiresAt
	if oldExpiresAt > 0 && newExpiresAt <= oldExpiresAt {
		// 刷新成功但访问令牌期限没有顺延：该账号可能已接近上游的登录态上限。
		log.Printf("[Auth] 账号 %s 刷新成功但访问令牌期限未顺延（%s -> %s），该账号可能已接近登录态上限，建议留意是否需要重新登录",
			path, time.Unix(oldExpiresAt, 0).Format("2006-01-02 15:04:05"), time.Unix(newExpiresAt, 0).Format("2006-01-02 15:04:05"))
	}
	log.Printf("[Auth] 账号 %s Token 刷新成功，访问令牌过期时间由 %s 更新为 %s，触发方式=%s，凭据已安全写回",
		path, time.Unix(oldExpiresAt, 0).Format("2006-01-02 15:04:05"), time.Unix(newExpiresAt, 0).Format("2006-01-02 15:04:05"), reason)
	writeStatusSnapshot()
	return nil
}

// recordRefreshFailure 累计连续刷新失败次数并返回当前次数。
func recordRefreshFailure(acc *Account, summary string) int {
	accountMu.Lock()
	defer accountMu.Unlock()
	acc.RefreshFailCount++
	acc.LastRefreshError = summary
	return acc.RefreshFailCount
}

// refreshTokenPayload 调用上游刷新接口并更新内存中的令牌字段（不落盘）。
// 按凭据文件中的 edition 路由到对应站点（国内站/国际站）的刷新接口。
// 返回上游 HTTP 状态码（成功或失败时均为实际状态；网络错误为 0）。
func refreshTokenPayload(sa *StoredAuth, client *http.Client) (int, error) {
	if client == nil {
		client = cfg.HttpClient
	}
	registerCredentialSecrets(sa)
	prof := profileForEdition(sa.Edition)
	headers := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("X-Refresh-Token", sa.Auth.RefreshToken)
		if sa.Account.EnterpriseID != "" {
			r.Header.Set("X-Enterprise-Id", sa.Account.EnterpriseID)
		}
		r.Header.Set("X-Auth-Refresh-Source", "workbuddy")
	}

	data, status, err := doJSON(client, http.MethodPost, prof.tokenRefreshURL(), headers, nil)
	if err != nil {
		return status, fmt.Errorf("上游刷新拒绝 (HTTP %d): %w", status, err)
	}
	var tok tokenData
	if err := json.Unmarshal(data, &tok); err != nil || tok.AccessToken == "" {
		return status, fmt.Errorf("解析新 Token 失败: %w", err)
	}
	registerSecrets(tok.AccessToken, tok.RefreshToken)

	now := time.Now()
	sa.Auth.AccessToken = tok.AccessToken
	if tok.RefreshToken != "" {
		sa.Auth.RefreshToken = tok.RefreshToken
	}
	if tok.Domain != "" {
		sa.Auth.Domain = tok.Domain
	}
	// 接口未返回 expiresIn 时保留旧过期时间（preserveExpiry），避免刷新风暴。
	if tok.ExpiresIn > 0 {
		sa.Auth.ExpiresAt = now.Add(time.Duration(tok.ExpiresIn) * time.Second).Unix()
	}
	// 刷新令牌到期时间：优先用接口返回的 refreshExpiresIn；接口没给时，
	// 从当前刷新令牌未验签的令牌声明读取，避免在界面上显示成未知。
	if tok.RefreshExpiresIn > 0 {
		sa.Auth.RefreshExpiresAt = now.Add(time.Duration(tok.RefreshExpiresIn) * time.Second).Unix()
	} else if exp, ok := jwtExpiry(sa.Auth.RefreshToken); ok {
		sa.Auth.RefreshExpiresAt = exp.Unix()
	}
	sa.Auth.LastRefreshTime = now.Unix()
	return status, nil
}

func parseQuotaCapacity(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	return strconv.ParseFloat(value, 64)
}

// summary 只有额度与历史标记，不能独立判定有效套餐。
// 由完整权益查询成功后替换“待确认”，禁止据试用标记猜成Pro试用。
func planLabelFromSummary(summary quotaSummaryData) string {
	return planUnknown
}

func parseQuotaSummary(data []byte) (total, used, remaining float64, paid bool, plan string, err error) {
	var summary quotaSummaryData
	if err = json.Unmarshal(data, &summary); err != nil {
		return 0, 0, 0, false, "", fmt.Errorf("解析额度响应失败: %w", err)
	}
	for _, pkg := range summary.Packages {
		pkgTotal, parseErr := parseQuotaCapacity(pkg.CycleTotalCapacity)
		if parseErr != nil {
			return 0, 0, 0, false, "", fmt.Errorf("解析总额度 %q 失败: %w", pkg.CycleTotalCapacity, parseErr)
		}
		pkgUsed, parseErr := parseQuotaCapacity(pkg.CycleUsedCapacity)
		if parseErr != nil {
			return 0, 0, 0, false, "", fmt.Errorf("解析已用额度 %q 失败: %w", pkg.CycleUsedCapacity, parseErr)
		}
		pkgRemaining, parseErr := parseQuotaCapacity(pkg.CycleRemainCapacity)
		if parseErr != nil {
			return 0, 0, 0, false, "", fmt.Errorf("解析剩余额度 %q 失败: %w", pkg.CycleRemainCapacity, parseErr)
		}
		total += pkgTotal
		used += pkgUsed
		remaining += pkgRemaining
	}
	return total, used, remaining, summary.IsPaidUser, planLabelFromSummary(summary), nil
}

// parseEnterpriseUsage 把企业分配额度换成网关的总额/已用/剩余。
// credit（或 used_num）是本周期已用，和个人口径的「剩余」相反；
// limitNum（或 limit_num）是分给这个成员的额度。limitNum=-1 表示不限量。
func parseEnterpriseUsage(data []byte) (total, used, remaining float64, unlimited bool, err error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return 0, 0, 0, false, fmt.Errorf("企业额度响应为空")
	}
	var raw map[string]json.RawMessage
	if err = json.Unmarshal(trimmed, &raw); err != nil {
		return 0, 0, 0, false, fmt.Errorf("解析企业额度响应失败: %w", err)
	}
	limit, hasLimit, err := firstEnterpriseNumber(raw, "limitNum", "limit_num")
	if err != nil {
		return 0, 0, 0, false, err
	}
	if !hasLimit {
		return 0, 0, 0, false, fmt.Errorf("企业额度响应缺少 limitNum")
	}
	used, hasUsed, err := firstEnterpriseNumber(raw, "credit", "used_num", "usedNum")
	if err != nil {
		return 0, 0, 0, false, err
	}
	if !hasUsed {
		return 0, 0, 0, false, fmt.Errorf("企业额度响应缺少 credit/used_num")
	}
	if used < 0 || (limit < 0 && limit != -1) {
		return 0, 0, 0, false, fmt.Errorf("企业额度响应包含无效负数")
	}
	if limit == -1 {
		// 不限量没有「剩余<=0」这回事。总额/剩余用 -1 作哨兵，展示层显示「不限」。
		return -1, used, -1, true, nil
	}
	remaining = limit - used
	if remaining < 0 {
		remaining = 0
	}
	return limit, used, remaining, false, nil
}

func firstEnterpriseNumber(raw map[string]json.RawMessage, keys ...string) (float64, bool, error) {
	var value float64
	var present bool
	for _, key := range keys {
		val, ok := raw[key]
		if !ok || string(bytes.TrimSpace(val)) == "null" {
			continue
		}
		num, err := parseEnterpriseJSONNumber(val)
		if err != nil {
			return 0, true, fmt.Errorf("企业额度字段 %s 无法解析", key)
		}
		if present && value != num {
			return 0, true, fmt.Errorf("企业额度同义字段数值冲突")
		}
		value, present = num, true
	}
	return value, present, nil
}

func parseEnterpriseJSONNumber(raw json.RawMessage) (float64, error) {
	raw = bytes.TrimSpace(raw)
	var num float64
	if err := json.Unmarshal(raw, &num); err == nil && !math.IsNaN(num) && !math.IsInf(num, 0) {
		return num, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		num, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
		if err == nil && !math.IsNaN(num) && !math.IsInf(num, 0) {
			return num, nil
		}
	}
	return 0, fmt.Errorf("not a number")
}

// applyQuotaObservation 写入额度并在余额恢复或不限量时解除模型额度阻断。
// 调用方可以持有账号锁；这里只取账号池锁。
func applyQuotaObservation(acc *Account, total, used, remaining float64, unlimited, paid bool, plan string, planOK bool, now time.Time) (wasExhausted, exhausted bool, display string) {
	accountMu.Lock()
	defer accountMu.Unlock()
	wasExhausted = acc.QuotaExhausted
	acc.QuotaTotal = total
	acc.QuotaUsed = used
	acc.QuotaRemaining = remaining
	acc.QuotaUnlimited = unlimited
	acc.IsPaidUser = paid
	if planOK {
		acc.PlanLabel = plan
		acc.PlanCheckedAt = now.Unix()
		acc.PlanStale = false
	} else {
		if acc.PlanCheckedAt == 0 {
			acc.PlanLabel = planUnknown
		}
		acc.PlanStale = true
	}
	display = planDisplayLabel(acc.PlanLabel, acc.PlanStale)
	acc.QuotaKnown = true
	exhausted = !unlimited && remaining <= 0
	acc.QuotaExhausted = exhausted
	if unlimited || remaining > 0 {
		for _, state := range acc.ModelStates {
			if state == nil {
				continue
			}
			state.QuotaBlocked = false
			state.NextProbeAt = time.Time{}
		}
	}
	return wasExhausted, exhausted, display
}

func formatQuota(value float64) string {
	if value > -0.005 && value < 0.005 {
		value = 0
	}
	return strings.TrimRight(strings.TrimRight(strconv.FormatFloat(value, 'f', 2, 64), "0"), ".")
}

func formatQuotaAmount(unlimited bool, kind string, value float64) string {
	if unlimited && (kind == "total" || kind == "remaining") {
		return "不限"
	}
	return formatQuota(value)
}

func usageCredit(usage map[string]any) (float64, bool) {
	if usage == nil {
		return 0, false
	}
	value, exists := usage["credit"]
	if !exists {
		return 0, false
	}
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		n, err := v.Float64()
		return n, err == nil
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func usageFromCompletion(data []byte) map[string]any {
	var completion map[string]any
	if json.Unmarshal(data, &completion) != nil {
		return nil
	}
	usage, _ := completion["usage"].(map[string]any)
	return usage
}

func usageNumber(usage map[string]any, key string) (int64, bool) {
	if usage == nil {
		return 0, false
	}
	switch v := usage[key].(type) {
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case int:
		return int64(v), true
	case int64:
		return v, true
	case json.Number:
		n, err := v.Int64()
		if err == nil {
			return n, true
		}
		f, err := v.Float64()
		return int64(f), err == nil
	case string:
		s := strings.TrimSpace(v)
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return n, true
		}
		f, err := strconv.ParseFloat(s, 64)
		return int64(f), err == nil
	default:
		return 0, false
	}
}

func usageTotalTokens(usage map[string]any) (int64, bool) {
	if n, ok := usageNumber(usage, "total_tokens"); ok {
		return n, true
	}
	prompt, hasPrompt := usageNumber(usage, "prompt_tokens")
	completion, hasCompletion := usageNumber(usage, "completion_tokens")
	if hasPrompt || hasCompletion {
		return prompt + completion, true
	}
	return 0, false
}

func observeModelCredit(acc *Account, model string, usage map[string]any, reqID uint64) {
	credit, ok := usageCredit(usage)
	if !ok || acc == nil {
		return
	}
	// credit=0 只有样本足够大时才判定为免费：极小请求（例如探针）上游也可能记 0，
	// 那不代表该模型真的免费，不能据此让零余额账号去请求收费模型。
	if credit <= 0 {
		total, hasTotal := usageTotalTokens(usage)
		if !hasTotal || total < modelFreeMinTokens {
			log.Printf("[ModelCost] requestId=%d 账号 %s 模型 %s credit=0 但样本过小（total_tokens=%d < %d），忽略本次免费判定",
				reqID, acc.Path, model, total, modelFreeMinTokens)
			return
		}
	}
	// 每千 token 单价（EMA 平滑，wb2api 同口径）：tokens 无效时不动单价账本。
	per1k, hasPer1k := 0.0, false
	if tokens, ok := usageTotalTokens(usage); ok && tokens > 0 {
		per1k = credit / float64(tokens) * 1000
		if per1k < 0 {
			per1k = 0
		}
		hasPer1k = true
	}
	accountMu.Lock()
	state := modelStateLocked(acc, model)
	oldClass := state.CostClass
	if credit <= 0 {
		state.CostClass = modelCostFree
		state.QuotaBlocked = false
	} else {
		state.CostClass = modelCostPaid
	}
	if hasPer1k {
		if state.CostSamples == 0 {
			state.CostPer1k = per1k
		} else {
			state.CostPer1k = state.CostPer1k*(1-modelCostEMAAlpha) + per1k*modelCostEMAAlpha
		}
		state.CostSamples++
	}
	state.ObservedAt = time.Now()
	quotaExhausted := acc.QuotaExhausted
	path := acc.Path
	site := acc.Profile().Key
	accountMu.Unlock()
	recordModelCostClass(model, site, credit <= 0)

	if credit <= 0 {
		if oldClass != modelCostFree {
			log.Printf("[ModelCost] requestId=%d 账号 %s 站点=%s 模型 %s 实测 usage.credit=0，已学习为免费模型", reqID, path, site, model)
		}
		if quotaExhausted {
			log.Printf("[FreeModel] requestId=%d 站点=%s 请求的是免费模型 %s，已明确使用付费余额耗尽账号 %s 完成请求，usage.credit=0", reqID, site, model, path)
		}
	} else if oldClass != modelCostPaid {
		log.Printf("[ModelCost] requestId=%d 账号 %s 站点=%s 模型 %s 实测 usage.credit=%s，已学习为收费模型", reqID, path, site, model, formatQuota(credit))
	}
	writeStatusSnapshot()
}

// refreshAccountQuota 通过用户中心只读计费接口刷新账号额度，不记录或回传 Token。
func refreshAccountQuota(ctx context.Context, acc *Account) error {
	if acc == nil {
		return nil
	}
	if !lockAccountWithContext(ctx, &acc.lock) {
		return ctx.Err()
	}
	defer acc.lock.Unlock()

	accountMu.Lock()
	if acc.Disabled || acc.Auth == nil || acc.Auth.Auth.AccessToken == "" {
		accountMu.Unlock()
		return nil
	}
	auth := *acc.Auth
	path := acc.Path
	prof := acc.Profile()
	accountMu.Unlock()

	traceID := uuid.NewString()
	enterpriseID := strings.TrimSpace(auth.Account.EnterpriseID)
	if enterpriseID != "" {
		return refreshEnterpriseAccountQuota(ctx, acc, auth, path, prof, traceID, enterpriseID)
	}
	log.Printf("[Quota] traceId=%s 账号 %s 开始查询额度，站点=%s，接口=%s", traceID, path, prof.Label, prof.quotaSummaryURL())
	headers := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("Authorization", "Bearer "+auth.Auth.AccessToken)
		r.Header.Set("X-Client-Platform", "web")
		r.Header.Set("X-Trace-ID", traceID)
	}
	data, status, err := doJSONContext(ctx, clientForAccount(acc), http.MethodPost, prof.quotaSummaryURL(), headers, strings.NewReader("{}"))
	if err != nil {
		markAccountPlanStale(acc)
		log.Printf("[Quota] traceId=%s 账号 %s 查询额度失败，HTTP=%d，保留上一次额度数据，套餐标记待刷新", traceID, path, status)
		return err
	}
	total, used, remaining, paid, plan, err := parseQuotaSummary(data)
	if err != nil {
		markAccountPlanStale(acc)
		log.Printf("[Quota] traceId=%s 账号 %s 查询额度响应无法解析，保留上一次额度数据，套餐标记待刷新", traceID, path)
		return err
	}
	var summary quotaSummaryData
	_ = json.Unmarshal(data, &summary) // parseQuotaSummary 已校验。
	now := time.Now()
	resources, planErr := fetchPlanResources(ctx, prof, headers, traceID, path, now)
	if planErr == nil {
		plan, planErr = identifyPlan(prof.Key, summary, resources, now)
	}
	wasExhausted, exhausted, plan := applyQuotaObservation(acc, total, used, remaining, false, paid, plan, planErr == nil, now)
	if planErr != nil {
		log.Printf("[套餐判断] traceId=%s 账号=%s 结果=待确认 原因=%v 显示=%s 业务影响=额度仍更新，不把接口失败误报为免费或试用", traceID, path, planErr, plan)
	} else {
		log.Printf("[套餐判断] traceId=%s 账号=%s 有效期校验时间=%s 资源数=%d 显示=%s 结果=完整权益校验成功", traceID, path, now.Format(time.RFC3339), len(resources), plan)
	}
	switch {
	case !wasExhausted && exhausted:
		log.Printf("[Quota] traceId=%s 账号 %s 额度查询成功，总额度=%s，已用=%s，剩余=%s，付费用户=%t，套餐=%s；账号已冻结调度，等待额度恢复", traceID, path, formatQuota(total), formatQuota(used), formatQuota(remaining), paid, plan)
	case wasExhausted && !exhausted:
		log.Printf("[Quota] traceId=%s 账号 %s 额度已恢复，总额度=%s，已用=%s，剩余=%s，付费用户=%t，套餐=%s；账号已自动解除冻结并恢复调度", traceID, path, formatQuota(total), formatQuota(used), formatQuota(remaining), paid, plan)
	default:
		log.Printf("[Quota] traceId=%s 账号 %s 额度查询成功，总额度=%s，已用=%s，剩余=%s，付费用户=%t，套餐=%s", traceID, path, formatQuota(total), formatQuota(used), formatQuota(remaining), paid, plan)
	}
	return nil
}

// refreshEnterpriseAccountQuota 只查询企业成员被分配的额度。
// 外部项目实测个人 get-user-resource* 对企业号为空，不能据此把账号判成剩余 0。
// 调用方已持有 acc.lock。
func refreshEnterpriseAccountQuota(ctx context.Context, acc *Account, auth StoredAuth, path string, prof *upstreamProfile, traceID, enterpriseID string) error {
	endpoint := prof.enterpriseUsageURL()
	log.Printf("[Quota] traceId=%s 账号 %s 开始查询企业分配额度，站点=%s，接口=%s，企业身份=已识别，说明=不请求个人资源包 get-user-resource，避免空包被当成剩余 0", traceID, path, prof.Label, endpoint)
	headers := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("Authorization", "Bearer "+auth.Auth.AccessToken)
		r.Header.Set("X-Client-Platform", "web")
		r.Header.Set("X-Trace-ID", traceID)
		r.Header.Set("X-Enterprise-Id", enterpriseID)
		r.Header.Set("X-Tenant-Id", enterpriseID)
		if auth.Account.UID != "" {
			r.Header.Set("X-User-Id", strings.TrimSpace(auth.Account.UID))
		}
		if auth.Auth.Domain != "" {
			r.Header.Set("X-Domain", auth.Auth.Domain)
		}
	}
	data, status, err := doJSONContext(ctx, cfg.HttpClient, http.MethodPost, endpoint, headers, strings.NewReader("{}"))
	if err != nil {
		markAccountPlanStale(acc)
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度查询失败，HTTP=%d，保留上一次额度，业务影响=不把失败当成剩余 0，本轮不解除也不新增强制阻断", traceID, path, status)
		return err
	}
	total, used, remaining, unlimited, err := parseEnterpriseUsage(data)
	if err != nil {
		markAccountPlanStale(acc)
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度响应无法解析，保留上一次额度，原因=%v，业务影响=不把坏数据当成剩余 0", traceID, path, err)
		return err
	}
	now := time.Now()
	wasExhausted, exhausted, plan := applyQuotaObservation(acc, total, used, remaining, unlimited, false, planEnterprise, true, now)
	log.Printf("[套餐判断] traceId=%s 账号=%s 结果=%s 说明=企业号没有个人套餐资源包，不调用 get-user-resource，显示=%s", traceID, path, planEnterprise, plan)
	totalText := formatQuotaAmount(unlimited, "total", total)
	remainText := formatQuotaAmount(unlimited, "remaining", remaining)
	switch {
	case !wasExhausted && exhausted:
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度计算=limitNum %s - credit %s，剩余=%s，不限量=%t，套餐=%s；分配额度已用完，收费模型暂停调度", traceID, path, totalText, formatQuota(used), remainText, unlimited, plan)
	case wasExhausted && !exhausted:
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度已恢复，计算=limitNum %s - credit %s，剩余=%s，不限量=%t，套餐=%s；已解除此前因个人空包或 14018 造成的额度阻断", traceID, path, totalText, formatQuota(used), remainText, unlimited, plan)
	default:
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度查询成功，计算=limitNum %s - credit %s，剩余=%s，不限量=%t，套餐=%s", traceID, path, totalText, formatQuota(used), remainText, unlimited, plan)
	}
	return nil
}

func lockAccountWithContext(ctx context.Context, mu *sync.Mutex) bool {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if mu.TryLock() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func refreshAllAccountQuotas(ctx context.Context, done chan<- struct{}, schedule ...time.Time) {
	defer close(done)
	next := time.Time{}
	if len(schedule) > 0 {
		next = schedule[0]
	}
	trace, started := startScheduledTask("quota_scan", next)
	accountMu.Lock()
	accs := append([]*Account(nil), accounts...)
	accountMu.Unlock()
	log.Printf("[Quota] 开始批量更新额度，账号数=%d", len(accs))
	processed, failed := 0, 0
	for _, acc := range accs {
		if err := ctx.Err(); err != nil {
			log.Printf("[Quota] 本轮额度更新被取消，尚未扫描的账号将等待下一轮: %v", err)
			break
		}
		accountCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := refreshAccountQuota(accountCtx, acc)
		processed++
		cancel()
		if err != nil {
			failed++
			log.Printf("[Quota] 账号 %s 本轮额度更新未完成: %v", acc.Path, err)
		} else {
			accountMu.Lock()
			if acc != nil && !acc.Disabled && acc.PlanStale {
				failed++
			}
			accountMu.Unlock()
		}
	}
	snapshotErr := writeStatusSnapshot()
	log.Printf("[Quota] 本轮额度更新完成，状态快照已写入")
	status := "success"
	if ctx.Err() != nil {
		status = "cancelled"
	} else if len(accs) == 0 {
		status = "skipped"
	} else if failed == len(accs) {
		status = "failed"
	} else if failed > 0 || snapshotErr != nil {
		status = "partial"
	}
	logScheduledTask("quota_scan", "completed", status, trace, started, next, fmt.Sprintf("账号=%d，已检查=%d，额度/套餐异常=%d，快照失败=%t", len(accs), processed, failed, snapshotErr != nil))
}

func backgroundQuotaRefresher() {
	const interval = 300 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	next := time.Now().Add(interval)
	var cancel context.CancelFunc
	var done chan struct{}
	start := func() {
		if cancel != nil && done != nil {
			select {
			case <-done:
			default:
				log.Printf("[Quota] 新一轮额度扫描到达，正在强制取消上一轮未完成的请求")
				cancel()
			}
		}
		ctx, nextCancel := context.WithCancel(context.Background())
		cancel = nextCancel
		done = make(chan struct{})
		go refreshAllAccountQuotas(ctx, done, next)
	}
	start()
	for {
		select {
		case tick := <-ticker.C:
			next = tick.Add(interval)
			start()
		case <-quotaScanTrigger:
			log.Printf("[Quota] 凭据池发生变化，立即触发一轮额度扫描")
			start()
		}
	}
}

func requestQuotaScan() {
	select {
	case quotaScanTrigger <- struct{}{}:
	default:
	}
}

func isAlreadyCheckedIn(status int, message string) bool {
	if status == 0 && message == "" {
		return false
	}
	low := strings.ToLower(message)
	return strings.Contains(message, `"code":10001`) || strings.Contains(message, `"code":14001`) ||
		strings.Contains(message, "已签到") || strings.Contains(low, "already checked in")
}

// checkinAccount 执行国内站每日签到。国际站没有已确认可用的签到体系，明确跳过。
func checkinAccount(ctx context.Context, acc *Account) (string, error) {
	if acc == nil {
		return "skipped", nil
	}
	acc.lock.Lock()
	defer acc.lock.Unlock()

	accountMu.Lock()
	if acc.Disabled || acc.Auth == nil || acc.Auth.Auth.AccessToken == "" {
		accountMu.Unlock()
		return "skipped", nil
	}
	if acc.Profile().Key != profileCN.Key {
		accountMu.Unlock()
		return "global_skipped", nil
	}
	if strings.TrimSpace(acc.Auth.Account.EnterpriseID) != "" {
		accountPath := acc.Path
		accountMu.Unlock()
		log.Printf("[Checkin] traceId=%s 账号 %s 是企业版，不发起个人签到请求；额度仍由企业分配额度接口更新", newTraceID(), accountPath)
		return "enterprise_skipped", nil
	}
	auth := *acc.Auth
	path := acc.Path
	prof := acc.Profile()
	accountMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	log.Printf("[Checkin] 账号 %s 开始每日签到，站点=%s，接口=%s", path, prof.Label, prof.dailyCheckinURL())
	headers := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("Authorization", "Bearer "+auth.Auth.AccessToken)
		r.Header.Set("X-Client-Platform", "web")
		if auth.Account.UID != "" {
			r.Header.Set("X-User-Id", auth.Account.UID)
		}
		if auth.Account.EnterpriseID != "" {
			r.Header.Set("X-Enterprise-Id", auth.Account.EnterpriseID)
			r.Header.Set("X-Tenant-Id", auth.Account.EnterpriseID)
		}
		if auth.Auth.Domain != "" {
			r.Header.Set("X-Domain", auth.Auth.Domain)
		}
	}
	_, status, err := doJSONContext(ctx, clientForAccount(acc), http.MethodPost, prof.dailyCheckinURL(), headers, strings.NewReader("{}"))
	if err == nil {
		log.Printf("[Checkin] 账号 %s 每日签到成功", path)
		return "ok", nil
	}
	if isAlreadyCheckedIn(status, err.Error()) {
		log.Printf("[Checkin] 账号 %s 今天已经签到，本次按幂等成功处理", path)
		return "already", nil
	}
	log.Printf("[Checkin] 账号 %s 每日签到失败，HTTP=%d，原因=%v；不改变账号调度状态", path, status, err)
	return "failed", err
}

func checkinAllAccounts(ctx context.Context) {
	if !dailyCheckinMu.TryLock() {
		log.Printf("[Checkin] 已有一轮签到正在执行，本次重复触发已跳过")
		return
	}
	defer dailyCheckinMu.Unlock()
	next := nextDailyCheckin(time.Now())
	trace, started := startScheduledTask("daily_checkin", next)

	accountMu.Lock()
	accs := append([]*Account(nil), accounts...)
	accountMu.Unlock()
	log.Printf("[Checkin] 开始每日签到，账号总数=%d，国际站账号将跳过", len(accs))
	ok, already, failed, skipped := 0, 0, 0, 0
	for _, acc := range accs {
		if err := ctx.Err(); err != nil {
			log.Printf("[Checkin] 本轮签到被取消，未处理账号等待下一次触发: %v", err)
			break
		}
		result, err := checkinAccount(ctx, acc)
		switch result {
		case "ok":
			ok++
		case "already":
			already++
		case "failed":
			failed++
			_ = err
		default:
			skipped++
		}
	}
	log.Printf("[Checkin] 本轮签到完成，总数=%d，成功=%d，今日已签=%d，失败=%d，跳过=%d", len(accs), ok, already, failed, skipped)
	status := "success"
	if ctx.Err() != nil {
		status = "cancelled"
	} else if failed > 0 && ok+already == 0 {
		status = "failed"
	} else if failed > 0 {
		status = "partial"
	} else if ok+already == 0 {
		status = "skipped"
	}
	logScheduledTask("daily_checkin", "completed", status, trace, started, next, fmt.Sprintf("成功=%d，今日已签=%d，失败=%d，跳过=%d", ok, already, failed, skipped))
	requestQuotaScan()
}

func nextDailyCheckin(now time.Time) time.Time {
	loc := time.FixedZone("UTC+8", 8*60*60)
	localNow := now.In(loc)
	next := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 9, 0, 0, 0, loc)
	if !next.After(localNow) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

func requestCheckin() {
	select {
	case checkinTrigger <- struct{}{}:
	default:
	}
}

func backgroundDailyCheckin() {
	go checkinAllAccounts(context.Background())
	for {
		next := nextDailyCheckin(time.Now())
		logScheduledTask("daily_checkin", "scheduled", "", newTraceID(), time.Time{}, next, "固定北京时间09:00；账号池变化可能提前触发")
		log.Printf("[Checkin] 下一次定时签到时间=%s", next.Format("2006-01-02 15:04:05 MST"))
		timer := time.NewTimer(time.Until(next))
		select {
		case <-timer.C:
			go checkinAllAccounts(context.Background())
		case <-checkinTrigger:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			log.Printf("[Checkin] 凭据池发生变化，立即触发一轮签到")
			go checkinAllAccounts(context.Background())
		}
	}
}

// -----------------------------------------------------------------------------
// CLI 子命令实现: login, status, refresh
// -----------------------------------------------------------------------------

// formatAccountStatus 生成单个账号的完整状态文本（status 命令与 serve 启动横幅共用）。
// idx 从 1 开始的账号序号；now 为当前时间（用于冷却/过期判定）。
func formatAccountStatus(acc *Account, idx int, now time.Time) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n--- 账号 #%d ---\n", idx))
	sb.WriteString(fmt.Sprintf("凭据文件:     %s\n", acc.Path))
	prof := acc.Profile()
	sb.WriteString(fmt.Sprintf("站点:         %s (%s)\n", prof.Label, strings.TrimPrefix(prof.Base, "https://")))

	if acc.Disabled {
		// 失效账号（Auth 可能为 nil：凭据文件已删除，信息来自失效标记）
		nickname := acc.Nickname
		uid := acc.UID
		if acc.Auth != nil {
			nickname = acc.Auth.Account.Nickname
			uid = acc.Auth.Account.UID
		}
		sb.WriteString(fmt.Sprintf("用户昵称:     %s\n", ifEmpty(nickname, "(未知)")))
		sb.WriteString(fmt.Sprintf("用户 UID:     %s\n", ifEmpty(uid, "(未知)")))
		sb.WriteString("账号状态:     授权失效（禁止调度）\n")
		sb.WriteString(fmt.Sprintf("失效原因:     %s\n", truncate(acc.DisabledReason, 200)))
		sb.WriteString(fmt.Sprintf("处理建议:     凭据文件已删除，请重新执行: workbuddy-gateway login -auth %s\n", acc.Path))
		return sb.String()
	}

	if acc.Auth == nil {
		return sb.String()
	}

	expTime := time.Unix(acc.Auth.Auth.ExpiresAt, 0)
	remaining := time.Until(expTime)
	statusStr := "有效"
	if remaining <= 0 {
		statusStr = "已过期"
	}

	sb.WriteString(fmt.Sprintf("用户昵称:     %s\n", acc.Auth.Account.Nickname))
	sb.WriteString(fmt.Sprintf("用户 UID:     %s\n", acc.Auth.Account.UID))
	sb.WriteString(fmt.Sprintf("企业 ID:      %s\n", ifEmpty(acc.Auth.Account.EnterpriseID, "(个人账号)")))
	sb.WriteString(fmt.Sprintf("认证域名:     %s\n", ifEmpty(acc.Auth.Auth.Domain, "www.codebuddy.cn")))
	sb.WriteString(formatRenewalStatus(acc))
	sb.WriteString(fmt.Sprintf("套餐:         %s\n", planDisplayLabel(acc.PlanLabel, acc.PlanStale)))
	if acc.PlanStale {
		sb.WriteString("              * 权益查询失败，待刷新；名称可能是上次成功结果。\n")
	}
	if acc.QuotaKnown {
		sb.WriteString(fmt.Sprintf("额度:         总额度 %s / 已用 %s / 剩余 %s\n",
			formatQuotaAmount(acc.QuotaUnlimited, "total", acc.QuotaTotal),
			formatQuotaAmount(acc.QuotaUnlimited, "used", acc.QuotaUsed),
			formatQuotaAmount(acc.QuotaUnlimited, "remaining", acc.QuotaRemaining)))
	}
	if acc.CooldownUntil.After(now) {
		sb.WriteString(fmt.Sprintf("冷却状态:     冷却中 (解封: %s, 剩余 %v)\n",
			acc.CooldownUntil.Format("2006-01-02 15:04:05"),
			time.Until(acc.CooldownUntil).Round(time.Minute)))
		sb.WriteString(fmt.Sprintf("冷却原因:     %s\n", truncate(acc.CooldownMsg, 120)))
	} else {
		sb.WriteString("冷却状态:     可用\n")
	}
	sb.WriteString(fmt.Sprintf("Token 状态:   %s\n", statusStr))
	sb.WriteString(fmt.Sprintf("过期时间:     %s (剩余 %v)\n", expTime.Format("2006-01-02 15:04:05"), remaining.Round(time.Minute)))
	if ledger := formatModelCostLedger(acc, now); ledger != "" {
		sb.WriteString(ledger)
	}
	return sb.String()
}

// formatModelCostLedger 生成账号的模型成本账本摘要（status/启动横幅用）。
// 只展示有有效观测的模型：免费（tier 0）在前，收费按单价升序；最多 8 行。
func formatModelCostLedger(acc *Account, now time.Time) string {
	type row struct {
		model   string
		free    bool
		per1k   float64
		samples int
	}
	var rows []row
	for model, state := range acc.ModelStates {
		if state.CostSamples == 0 {
			continue
		}
		if !state.ObservedAt.IsZero() && now.Sub(state.ObservedAt) > modelCostTTL {
			continue // 观测过期：不再展示（重新学习后恢复）
		}
		rows = append(rows, row{model: model, free: state.CostClass == modelCostFree, per1k: state.CostPer1k, samples: state.CostSamples})
	}
	if len(rows) == 0 {
		return ""
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].free != rows[j].free {
			return rows[i].free
		}
		if rows[i].per1k != rows[j].per1k {
			return rows[i].per1k < rows[j].per1k
		}
		return rows[i].model < rows[j].model
	})
	freeCount := 0
	for _, r := range rows {
		if r.free {
			freeCount++
		}
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("模型账本:     %d 个模型（免费 %d / 收费 %d）\n", len(rows), freeCount, len(rows)-freeCount))
	limit := min(len(rows), 8)
	for _, r := range rows[:limit] {
		if r.free {
			sb.WriteString(fmt.Sprintf("  - %-28s 免费 (样本 %d)\n", r.model, r.samples))
		} else {
			sb.WriteString(fmt.Sprintf("  - %-28s %.4f/1k (样本 %d)\n", r.model, r.per1k, r.samples))
		}
	}
	if len(rows) > limit {
		sb.WriteString(fmt.Sprintf("  ... 其余 %d 个见 workbuddy-status.json\n", len(rows)-limit))
	}
	return sb.String()
}

func runStatus() {
	if err := loadAccounts(); err != nil {
		fmt.Printf("未找到有效凭据: %v\n请先执行: workbuddy-gateway login 扫码登录。\n", err)
		return
	}

	accountMu.Lock()
	defer accountMu.Unlock()

	fmt.Println("================== WorkBuddy 账号池状态 ==================")
	fmt.Printf("账号总数: %d\n", len(accounts))
	now := time.Now()
	for i, acc := range accounts {
		fmt.Print(formatAccountStatus(acc, i+1, now))
	}
	fmt.Println("\n=======================================================")
}

func runRefresh() {
	if err := loadAccounts(); err != nil {
		fmt.Printf("读取凭据失败: %v\n", err)
		return
	}
	accountMu.Lock()
	accs := append([]*Account(nil), accounts...)
	accountMu.Unlock()
	if len(accs) == 0 {
		fmt.Println("账号池为空")
		return
	}
	ok := 0
	fail := 0
	skipped := 0
	for i, acc := range accs {
		fmt.Printf("正在刷新账号 #%d (%s)... ", i+1, acc.Path)
		accountMu.Lock()
		disabled := acc.Disabled || acc.Auth == nil
		accountMu.Unlock()
		if disabled {
			fmt.Printf("跳过（授权失效，请重新登录）\n")
			skipped++
			continue
		}
		if err := doRefreshTokenFor(acc); err != nil {
			fmt.Printf("失败: %v\n", err)
			fail++
		} else {
			fmt.Println("成功")
			ok++
		}
	}
	fmt.Printf("\n刷新完成: 成功 %d 个，失败 %d 个，跳过 %d 个（授权失效）\n", ok, fail, skipped)
}

// -----------------------------------------------------------------------------
// 状态快照与前台实时监控 (monitor)
// -----------------------------------------------------------------------------

// accountSnapshot 是写入状态快照文件的单个账号状态。
type accountSnapshot struct {
	Alerts            []accountAlertMark `json:"alerts,omitempty"`
	Path              string             `json:"path"`
	SchedulingEnabled *bool              `json:"schedulingEnabled,omitempty"`
	SchedulingReason  string             `json:"schedulingReason,omitempty"`
	Edition           string             `json:"edition,omitempty"` // 站点标识（cn/intl）
	Nickname          string             `json:"nickname"`
	UID               string             `json:"uid"`
	State             string             `json:"state"` // active | cooldown | paid_exhausted | expired | disabled
	CooldownUntil     int64              `json:"cooldownUntil,omitempty"`
	CooldownMsg       string             `json:"cooldownMsg,omitempty"`
	DisabledReason    string             `json:"disabledReason,omitempty"`
	TokenExpiresAt    int64              `json:"tokenExpiresAt,omitempty"`
	QuotaTotal        float64            `json:"quotaTotal,omitempty"`
	QuotaUsed         float64            `json:"quotaUsed,omitempty"`
	QuotaRemaining    float64            `json:"quotaRemaining"`
	IsPaidUser        bool               `json:"isPaidUser"`
	PlanLabel         string             `json:"planLabel,omitempty"` // 官方套餐名称或已知短名称
	PlanCheckedAt     int64              `json:"planCheckedAt,omitempty"`
	PlanStale         bool               `json:"planStale,omitempty"`
	// 登录续期状态：刷新令牌到期时间与最近成功续期时间，便于提前发现需要重新登录的账号。
	RefreshExpiresAt int64                         `json:"refreshExpiresAt,omitempty"`
	LastRefreshTime  int64                         `json:"lastRefreshTime,omitempty"`
	RefreshFailCount int                           `json:"refreshFailCount,omitempty"`
	QuotaKnown       bool                          `json:"quotaKnown,omitempty"`
	QuotaExhausted   bool                          `json:"quotaExhausted,omitempty"`
	QuotaUnlimited   bool                          `json:"quotaUnlimited,omitempty"`
	ModelStates      map[string]modelStateSnapshot `json:"modelStates,omitempty"`
	FreeModels       int                           `json:"freeModels,omitempty"` // 展示值：所属站点模型统计中 0.00x 的模型数，不参与调度
	ModelCooldowns   int                           `json:"modelCooldowns,omitempty"`
}

type modelStateSnapshot struct {
	CostClass     string  `json:"costClass,omitempty"`
	CooldownUntil int64   `json:"cooldownUntil,omitempty"`
	QuotaBlocked  bool    `json:"quotaBlocked,omitempty"`
	NextProbeAt   int64   `json:"nextProbeAt,omitempty"`
	LastReason    string  `json:"lastReason,omitempty"`
	ObservedAt    int64   `json:"observedAt,omitempty"`
	CostPer1k     float64 `json:"costPer1k,omitempty"`
	CostSamples   int     `json:"costSamples,omitempty"`
}

// statusSnapshot 是写入 workbuddy-status.json 的完整快照。
type statusSnapshot struct {
	AlertSource       string              `json:"alertSource,omitempty"`
	UpdatedAt         int64               `json:"updatedAt"`
	Accounts          []accountSnapshot   `json:"accounts"`
	Models            []modelStatSnapshot `json:"models,omitempty"`
	ModelCatalog      []modelStatSnapshot `json:"modelCatalog,omitempty"`      // 管理页完整模型目录，含停用项。
	CostExploreEvents int64               `json:"costExploreEvents,omitempty"` // 累计 costTier 条件探索次数
}

// writeStatusSnapshot 将账号池实时状态（含冷却/失效）原子写入状态快照文件。
// serve 后台周期调用；monitor 命令前台读取展示。
func writeStatusSnapshot() error {
	statusSnapshotWriteMu.Lock()
	defer statusSnapshotWriteMu.Unlock()
	accountMu.Lock()
	snap := statusSnapshot{UpdatedAt: time.Now().Unix()}
	now := time.Now()
	for _, acc := range accounts {
		as := accountSnapshot{Path: acc.Path, Edition: acc.Profile().Key}
		as.QuotaTotal = acc.QuotaTotal
		as.QuotaUsed = acc.QuotaUsed
		as.QuotaRemaining = acc.QuotaRemaining
		as.IsPaidUser = acc.IsPaidUser
		as.PlanLabel = acc.PlanLabel
		as.PlanCheckedAt = acc.PlanCheckedAt
		as.PlanStale = acc.PlanStale
		as.RefreshFailCount = acc.RefreshFailCount
		if acc.Auth != nil {
			as.RefreshExpiresAt = acc.Auth.Auth.RefreshExpiresAt
			as.LastRefreshTime = acc.Auth.Auth.LastRefreshTime
			if as.RefreshExpiresAt == 0 {
				// 旧凭据或手工导入的凭据没有该字段，从未验签的令牌声明补一个展示值。
				if exp, ok := jwtExpiry(acc.Auth.Auth.RefreshToken); ok {
					as.RefreshExpiresAt = exp.Unix()
				}
			}
		}
		as.QuotaKnown = acc.QuotaKnown
		as.QuotaExhausted = acc.QuotaExhausted
		as.QuotaUnlimited = acc.QuotaUnlimited
		if len(acc.ModelStates) > 0 {
			as.ModelStates = make(map[string]modelStateSnapshot, len(acc.ModelStates))
			for model, state := range acc.ModelStates {
				if state.CooldownUntil.After(now) {
					as.ModelCooldowns++
				}
				as.ModelStates[model] = modelStateSnapshot{
					CostClass: state.CostClass, CooldownUntil: unixOrZero(state.CooldownUntil),
					QuotaBlocked: state.QuotaBlocked, NextProbeAt: unixOrZero(state.NextProbeAt),
					LastReason: state.LastReason, ObservedAt: unixOrZero(state.ObservedAt),
					CostPer1k: state.CostPer1k, CostSamples: state.CostSamples,
				}
			}
		}
		switch {
		case acc.Disabled:
			as.State = "disabled"
			as.DisabledReason = acc.DisabledReason
			if acc.Auth != nil {
				as.Nickname = acc.Auth.Account.Nickname
				as.UID = acc.Auth.Account.UID
			} else {
				as.Nickname = acc.Nickname
				as.UID = acc.UID
			}
		case acc.CooldownUntil.After(now):
			as.State = "cooldown"
			as.CooldownUntil = acc.CooldownUntil.Unix()
			as.CooldownMsg = acc.CooldownMsg
			if acc.Auth != nil {
				as.Nickname = acc.Auth.Account.Nickname
				as.UID = acc.Auth.Account.UID
				as.TokenExpiresAt = acc.Auth.Auth.ExpiresAt
			}
		case acc.Auth != nil && acc.Auth.Auth.ExpiresAt > 0 && acc.Auth.Auth.ExpiresAt <= now.Unix():
			as.State = "expired"
			as.Nickname = acc.Auth.Account.Nickname
			as.UID = acc.Auth.Account.UID
			as.TokenExpiresAt = acc.Auth.Auth.ExpiresAt
		case acc.QuotaExhausted:
			as.State = "paid_exhausted"
			if acc.Auth != nil {
				as.Nickname = acc.Auth.Account.Nickname
				as.UID = acc.Auth.Account.UID
				as.TokenExpiresAt = acc.Auth.Auth.ExpiresAt
			}
		default:
			as.State = "active"
			if acc.Auth != nil {
				as.Nickname = acc.Auth.Account.Nickname
				as.UID = acc.Auth.Account.UID
				as.TokenExpiresAt = acc.Auth.Auth.ExpiresAt
			}
		}
		snap.Accounts = append(snap.Accounts, as)
	}
	annotateAccountPolicies(snap.Accounts)
	snap.ModelCatalog = buildAllModelStatSnapshots(now, accounts)
	captureAccountStatsLocked()
	for _, row := range snap.ModelCatalog {
		if disabled, _ := modelDisabled(row.ID); !disabled {
			snap.Models = append(snap.Models, row)
		}
	}
	cnFree, intlFree := freeModelDisplayCounts(snap.Models)
	for i := range snap.Accounts {
		if profileForEdition(snap.Accounts[i].Edition).Key == "intl" {
			snap.Accounts[i].FreeModels = intlFree
		} else {
			snap.Accounts[i].FreeModels = cnFree
		}
	}
	if !lastFreeModelDisplay.initialized || lastFreeModelDisplay.cn != cnFree || lastFreeModelDisplay.intl != intlFree {
		lastFreeModelDisplay.initialized = true
		lastFreeModelDisplay.cn, lastFreeModelDisplay.intl = cnFree, intlFree
		log.Printf("[免费模型展示] traceId=%s 来源=模型统计站点倍率 国内=%d 国际=%d 模型行数=%d 规则=只计0.00x 业务影响=仅更新账号表展示，不改实测账本、调度或计费判断",
			newTraceID(), cnFree, intlFree, len(snap.Models))
	}
	snap.CostExploreEvents = costExploreEvents
	accountMu.Unlock()

	// 告警标记与快照同一次写入，避免出现「快照已异常、日志还没扫到」的时间差。
	marks := recordAccountAlertMarks(snap.Accounts, snap.UpdatedAt)
	for i := range snap.Accounts {
		snap.Accounts[i].Alerts = marks[snap.Accounts[i].Path]
	}
	snap.AlertSource = alertSourceSnapshot

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	// 原子写：先写临时文件再改名，避免 monitor 读到半截内容
	tmp := statusSnapshotFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, statusSnapshotFile); err != nil {
		return err
	}
	return nil
}

func displayWidth(s string) int {
	width := 0
	for _, r := range s {
		if r >= 0x1100 && (r <= 0x115f || r == 0x2329 || r == 0x232a ||
			(r >= 0x2e80 && r <= 0xa4cf) || (r >= 0xac00 && r <= 0xd7a3) ||
			(r >= 0xf900 && r <= 0xfaff) || (r >= 0xfe10 && r <= 0xfe6f) ||
			(r >= 0xff00 && r <= 0xff60) || (r >= 0xffe0 && r <= 0xffe6)) {
			width += 2
		} else {
			width++
		}
	}
	return width
}

func fitCell(s string, width int) string {
	s = strings.ReplaceAll(strings.ReplaceAll(s, "\r", " "), "\n", " ")
	if displayWidth(s) <= width {
		return s + strings.Repeat(" ", width-displayWidth(s))
	}
	limit := width - 3
	var b strings.Builder
	used := 0
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		rw := displayWidth(string(r))
		if used+rw > limit {
			break
		}
		b.WriteRune(r)
		used += rw
		s = s[size:]
	}
	return b.String() + "..." + strings.Repeat(" ", width-used-3)
}

func renderAccountTable(accs []accountSnapshot) string {
	widths := []int{4, 20, 24, 8, 10, 19, 10, 10, 10, 10, 10, 10}
	for _, a := range accs {
		widths[9] = min(36, max(widths[9], displayWidth(planDisplayLabel(a.PlanLabel, a.PlanStale))))
	}
	headers := []string{"序号", "凭据文件", "账号", "站点", "状态", "Token 有效期", "总额度", "已用", "剩余", "套餐", "免费模型", "模型冷却"}
	border := func() string {
		var b strings.Builder
		b.WriteByte('+')
		for _, width := range widths {
			b.WriteString(strings.Repeat("-", width+2))
			b.WriteByte('+')
		}
		return b.String()
	}
	row := func(cells []string) string {
		var b strings.Builder
		b.WriteByte('|')
		for i, cell := range cells {
			b.WriteByte(' ')
			b.WriteString(fitCell(cell, widths[i]))
			b.WriteString(" |")
		}
		return b.String()
	}

	var b strings.Builder
	b.WriteString(border() + "\n")
	b.WriteString(row(headers) + "\n")
	b.WriteString(border() + "\n")
	for i, a := range accs {
		status := "可用"
		expires := "-"
		total, used, remaining, plan := "-", "-", "-", "-"
		if a.QuotaKnown {
			total = formatQuotaAmount(a.QuotaUnlimited, "total", a.QuotaTotal)
			used = formatQuotaAmount(a.QuotaUnlimited, "used", a.QuotaUsed)
			remaining = formatQuotaAmount(a.QuotaUnlimited, "remaining", a.QuotaRemaining)
			plan = planDisplayLabel(a.PlanLabel, a.PlanStale)
		}
		if a.TokenExpiresAt > 0 {
			expires = time.Unix(a.TokenExpiresAt, 0).Format("2006-01-02 15:04:05")
		}
		switch a.State {
		case "cooldown":
			status = "冷却"
		case "expired":
			status = "已过期"
		case "quota_exhausted", "paid_exhausted":
			status = "付费耗尽"
		case "disabled":
			status = "失效"
		}
		b.WriteString(row([]string{
			strconv.Itoa(i + 1), filepath.Base(a.Path), ifEmpty(a.Nickname, "-"),
			profileForEdition(a.Edition).Label, status, expires, total, used, remaining, plan,
			strconv.Itoa(a.FreeModels), strconv.Itoa(a.ModelCooldowns),
		}) + "\n")
	}
	b.WriteString(border())
	for _, a := range accs {
		if a.PlanStale {
			b.WriteString("\n* 套餐权益查询失败，待刷新；名称可能是上次成功结果。")
			break
		}
	}
	now := time.Now()
	for _, a := range accs {
		if a.RefreshExpiresAt <= 0 {
			continue
		}
		exp := time.Unix(a.RefreshExpiresAt, 0)
		switch {
		case !exp.After(now):
			b.WriteString(fmt.Sprintf("\n! %s 刷新令牌已过期，需要重新登录。", filepath.Base(a.Path)))
		case exp.Sub(now) < 7*24*time.Hour:
			b.WriteString(fmt.Sprintf("\n! %s 刷新令牌 %s 到期（剩余 %s），建议尽快重新登录。",
				filepath.Base(a.Path), exp.Format("2006-01-02 15:04:05"), humanDuration(time.Until(exp))))
		}
	}
	for _, a := range accs {
		if a.RefreshFailCount > 0 {
			b.WriteString(fmt.Sprintf("\n! %s 续期连续失败 %d 次，可能需要重新登录。", filepath.Base(a.Path), a.RefreshFailCount))
		}
	}
	return b.String()
}

// statusSnapshotLoop serve 后台每 3 秒刷新一次状态快照。
func statusSnapshotLoop() {
	var lastLogged time.Time
	var previousStatus string
	write := func(next time.Time) {
		started := time.Now()
		err := writeStatusSnapshot()
		status, detail := "success", "账号与模型快照已写入"
		if err != nil {
			status, detail = "failed", safeDebugError(err)
		}
		logFrequentTask("status_snapshot", status, detail, started, next, &lastLogged, &previousStatus, err != nil)
	}
	write(time.Now().Add(3 * time.Second))
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for tick := range ticker.C {
		write(tick.Add(3 * time.Second))
	}
}

// tailLines 读取文件末尾 n 行（用于 monitor 展示最近日志）。
func tailLines(path string, n int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	lines, _, err := readTailLines(f, n, 0)
	return lines, err
}

// journalLines 获取 systemd 服务最近 n 行日志（Linux journalctl）。
func journalLines(service string, n int) ([]string, error) {
	cmd := exec.Command("journalctl", "-u", service, "-n", strconv.Itoa(n), "--no-pager", "-o", "short")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	s := strings.TrimSpace(string(out))
	if s == "" {
		return nil, nil
	}
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines, nil
}

// runMonitor 前台实时监控：周期刷新展示账号池状态 + 最近日志（Ctrl+C 退出）。
func runMonitor() {
	interval := time.Duration(cfg.MonitorInterval) * time.Second
	if interval <= 0 {
		interval = 3 * time.Second
	}
	logN := cfg.LogLines
	if logN <= 0 {
		logN = 15
	}

	// 判断 stdout 是否为终端（是则用 ANSI 清屏重绘，否则滚动输出）
	isTTY := false
	if fi, err := os.Stdout.Stat(); err == nil {
		isTTY = fi.Mode()&os.ModeCharDevice != 0
	}

	fmt.Println("================ WorkBuddy 实时监控 ================")
	fmt.Println("按 Ctrl+C 退出 | 状态文件: " + statusSnapshotFile)
	fmt.Println("---------------------------------------------------------------")

	for {
		if isTTY {
			fmt.Print("\033[H\033[2J") // 清屏
		} else {
			fmt.Println()
		}
		fmt.Printf("更新时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))

		data, err := os.ReadFile(statusSnapshotFile)
		if err != nil {
			fmt.Printf("未找到状态文件 %s（服务是否在运行？请确认已在服务工作目录执行 monitor）\n", statusSnapshotFile)
		} else {
			var snap statusSnapshot
			if err := json.Unmarshal(data, &snap); err == nil {
				active, cooldown, exhausted, expired, disabled := 0, 0, 0, 0, 0
				for _, a := range snap.Accounts {
					switch a.State {
					case "active":
						active++
					case "cooldown":
						cooldown++
					case "expired":
						expired++
					case "quota_exhausted", "paid_exhausted":
						exhausted++
					case "disabled":
						disabled++
					}
				}
				fmt.Printf("账号池: 共 %d 个 | 可用 %d | 冷却 %d | 付费耗尽 %d | 过期 %d | 失效 %d\n",
					len(snap.Accounts), active, cooldown, exhausted, expired, disabled)
				fmt.Println(renderAccountTable(snap.Accounts))
				if len(snap.Models) > 0 {
					fmt.Printf("\n模型统计 (来源 %s):\n", modelSourceLabel(modelSourceFromSnapshots(snap.Models)))
					fmt.Println(renderModelTable(snap.Models))
				}
			} else {
				fmt.Println("状态文件解析失败")
			}
		}

		// 最近日志
		if cfg.JournalService != "" {
			if lines, err := journalLines(cfg.JournalService, logN); err == nil && len(lines) > 0 {
				fmt.Printf("\n最近日志 (journalctl -u %s):\n", cfg.JournalService)
				for _, l := range lines {
					fmt.Println("  " + l)
				}
			}
		} else if cfg.LogFile != "" {
			if lines, err := tailLines(cfg.LogFile, logN); err == nil && len(lines) > 0 {
				fmt.Printf("\n最近日志 (%s):\n", cfg.LogFile)
				for _, l := range lines {
					fmt.Println("  " + l)
				}
			}
		}

		fmt.Println("---------------------------------------------------------------")
		time.Sleep(interval)
	}
}

func runLogin() {
	site := "cn"
	if cfg.LoginIntl {
		site = "intl"
	}
	prof := profileForEdition(site)

	fmt.Println("================ WorkBuddy 登录 ================")
	fmt.Printf("目标站点:     %s (%s)\n", prof.Label, strings.TrimPrefix(prof.Base, "https://"))
	if cfg.LoginIntl {
		fmt.Println("国际站登录将在浏览器中完成（邮箱 / 验证码 / SSO 等），凭据由网关自动接管。")
	}
	fmt.Println("正在生成登录凭据与二维码...")

	// 与网页「添加账号」共用同一条登录流程，避免 CLI 与网页两套实现漂移。
	s, err := beginAccountLogin(site, cfg.AuthFile)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	if qr, qrErr := qrcode.New(s.AuthURL, qrcode.Medium); qrErr == nil {
		if cfg.LoginIntl {
			fmt.Println("\n请用手机扫描下方二维码，并在浏览器中完成登录（邮箱 / 验证码 / SSO 等）：")
		} else {
			fmt.Println("\n请使用 微信 或 企业微信 扫描下方二维码登录：")
		}
		fmt.Println(qr.ToSmallString(false))
	}
	fmt.Println("如无法扫码，也可在浏览器中直接打开以下链接：")
	fmt.Printf("%s\n\n", s.AuthURL)
	fmt.Println("等待登录授权完成 (按 Ctrl+C 可取消)...")

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		snap := s.snapshot()
		switch snap["status"] {
		case "done":
			fmt.Println("\n登录成功")
			fmt.Printf("站点:         %s (%s)\n", prof.Label, strings.TrimPrefix(prof.Base, "https://"))
			if acct, ok := snap["account"].(map[string]any); ok {
				fmt.Printf("欢迎，%v (UID: %v)\n", acct["nickname"], acct["uid"])
				if exp, ok := acct["tokenExpiresAt"].(int64); ok && exp > 0 {
					fmt.Printf("访问令牌有效期至: %s\n", time.Unix(exp, 0).Format("2006-01-02 15:04:05"))
				}
			}
			fmt.Printf("凭据已成功保存到: %s\n", cfg.AuthFile)
			fmt.Println("\n现在可以启动网关服务：")
			fmt.Println("  workbuddy-gateway serve")
			return
		case "failed":
			fmt.Printf("\n%v\n", snap["error"])
			return
		case "expired":
			fmt.Println("\n登录已超时，请重新执行 login 命令。")
			return
		case "cancelled":
			fmt.Println("\n登录已取消。")
			return
		}
	}
}

// -----------------------------------------------------------------------------
// 本地 HTTP API 网关服务 (OpenAI 协议兼容)
// -----------------------------------------------------------------------------

func runServe() {
	// 先绑定全部所需端口；任一失败就释放已经绑定的端口，不启动半套服务。
	servers, listeners, err := prepareGatewayServers()
	if err != nil {
		fmt.Fprintf(os.Stderr, "启动失败: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	configCtx, stopConfigReload := context.WithCancel(context.Background())
	defer stopConfigReload()
	loadModelsCache()
	loadOptionalStats()
	if err := loadAccounts(); err != nil {
		fmt.Printf("警告: 未检测到有效凭据 (%v)。\n请先执行: workbuddy-gateway login 扫码登录，或确保凭据文件存在。\n\n", err)
	} else {
		accountMu.Lock()
		accs := append([]*Account(nil), accounts...)
		accountMu.Unlock()
		for _, acc := range accs {
			if acc.Disabled || acc.Auth == nil {
				continue
			}
			_ = ensureValidTokenFor(acc)
		}
	}

	// 启动后台自动刷新协程
	go backgroundTokenRefresher()
	// 主动续期（保活）：到点刷新全部账号登录凭据，不等访问令牌临近过期
	go keepaliveLoop()
	go backgroundQuotaRefresher()
	go backgroundDailyCheckin()
	if cfg.ModelsRefresh > 0 {
		go modelsRefreshLoop(time.Duration(cfg.ModelsRefresh) * time.Minute)
	} else {
		logScheduledTask("model_catalog", "disabled", "disabled", newTraceID(), time.Time{}, time.Time{}, "模型目录自动刷新已关闭")
	}
	// 价格探测独立于目录刷新：即使关闭目录刷新，也仍可基于缓存探测价格。
	go modelPriceProbeLoop()

	// 启动状态快照协程（monitor 命令实时读取展示）
	go statusSnapshotLoop()
	statsCtx, stopStats := context.WithCancel(context.Background())
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		statsPersistenceLoop(statsCtx)
	}()
	defer func() {
		stopStats()
		<-statsDone
	}()

	// 启动凭据热加载协程（新增/更新/删除凭据文件免重启生效）
	if cfg.ReloadInterval > 0 {
		go accountReloaderLoop()
	} else {
		logScheduledTask("credential_reload", "disabled", "disabled", newTraceID(), time.Time{}, time.Time{}, "凭据自动扫描已关闭")
	}

	listenAddr := servers[0].Addr

	fmt.Println("================================================================")
	fmt.Printf("WorkBuddy 本地网关已启动\n")
	fmt.Printf("   服务监听地址:  http://%s\n", listenAddr)
	fmt.Printf("   Chat 接口地址: http://%s/v1/chat/completions\n", listenAddr)
	fmt.Printf("   Models 接口:   http://%s/v1/models\n", listenAddr)
	fmt.Printf("   模型转发策略:  【完全透传】客户端请求的任意 model 原样中继至上游\n")
	_, modelSource := mergedModelIDs()
	fmt.Printf("   模型列表来源:  %s\n", modelSourceLabel(modelSource))
	if cfg.ReloadInterval > 0 {
		fmt.Printf("   凭据热加载:    每 %ds 自动扫描，新增/更新/删除凭据免重启生效\n", cfg.ReloadInterval)
	} else {
		fmt.Printf("   凭据热加载:    已关闭 (-reload-interval 0)\n")
	}
	if currentAPIKey() != "" {
		printAPIAuthBanner(os.Stdout)
	} else {
		fmt.Printf("   API 鉴权:      未启用 (任何客户端均可直连)\n")
	}
	if cfg.WebUI {
		fmt.Printf("   网页控制台:    已启用 http://%s/ui/ (独立管理 Key)\n", servers[1].Addr)
		if currentAdminKey() == "" {
			fmt.Println("   管理初始化:    请在本机或 SSH 隧道打开网页生成管理 Key，保存成功后仅弹窗展示一次")
		}
	} else {
		fmt.Println("   网页控制台:    未启用 (-webui 可开启)")
	}
	if cfg.ProxyURL != "" {
		fmt.Printf("   上游出口代理:  %s\n", cfg.ProxyURL)
	}
	if len(cfg.ProxyURLs) > 0 {
		fmt.Printf("   多代理池:      %d 个出口（账号按凭据文件名稳定绑定）\n", len(cfg.ProxyURLs))
		for i, p := range cfg.ProxyURLs {
			fmt.Printf("     [%d] %s\n", i+1, p)
		}
	}
	if cfg.DebugEnabled {
		fmt.Printf("   JSON 调试日志: 已开启 (%s)\n", filepath.Join(logDir, "debug-YYYY-MM-DD.jsonl"))
	} else {
		fmt.Printf("   JSON 调试日志: 已关闭 (%s 中 debug.enabled=false)\n", runtimeConfigFile)
	}
	fmt.Printf("   上游超时:      响应头等待 %v / 流空闲 %v (%s 可覆盖)\n",
		upstreamHeaderTimeout, upstreamIdleTimeout, runtimeConfigFile)
	fmt.Printf("   网络容错:      瞬时错误重试 %d 次 / 间隔 %v（仅未输出前重试，%s 可覆盖）\n",
		upstreamTransientRetries, upstreamRetryBackoff, runtimeConfigFile)
	if modelFilterConfigured() {
		blocked, allowed := modelFilterSummary()
		fmt.Printf("   模型黑白名单:  已启用 (黑名单 %d 个 / 白名单 %d 个，被禁模型已从列表隐藏并拒绝请求)\n", blocked, allowed)
	} else {
		fmt.Printf("   模型黑白名单:  未启用 (%s 中 models 段可配置)\n", runtimeConfigFile)
	}
	fmt.Printf("   模型账号名单:  %d 个模型配置了凭据文件规则（%s）\n", modelAccountRuleCount(), runtimeConfigFile)
	fmt.Println("   配置热生效:    每 1s 扫描 config.json；端口变更需重启")

	// 启动时展示所有账号状态（与 status 命令一致）
	accountMu.Lock()
	accCount := len(accounts)
	activeCount := 0
	disabledCount := 0
	now := time.Now()
	for _, acc := range accounts {
		if acc.Disabled || acc.Auth == nil {
			disabledCount++
		} else {
			activeCount++
		}
	}
	fmt.Printf("   账号池:        %d 个账号 (有效 %d, 失效 %d)\n", accCount, activeCount, disabledCount)
	if accCount > 0 {
		fmt.Println("   ----------------------------------------------------------")
		for i, acc := range accounts {
			fmt.Print(formatAccountStatus(acc, i+1, now))
		}
		fmt.Println("   ----------------------------------------------------------")
	}
	// 末尾汇总：加载到的凭据文件清单（即使上方日志被截断也能确认）
	fileList := make([]string, 0, len(accounts))
	for _, acc := range accounts {
		fileList = append(fileList, acc.Path)
	}
	if len(fileList) > 0 {
		fmt.Printf("   已加载凭据文件 (%d): %s\n", len(fileList), strings.Join(fileList, ", "))
	} else {
		fmt.Println("   未加载到任何凭据文件，请先执行 login 命令扫码登录")
	}
	accountMu.Unlock()

	fmt.Println("================================================================")
	fmt.Println("等待客户端请求中 (按 Ctrl+C 安全停止)...")
	go runtimeConfigLoop(configCtx)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stopChan)
	serveErrors := make(chan error, len(servers))
	for i := range servers {
		go func(i int) {
			if err := servers[i].Serve(listeners[i]); err != nil && err != http.ErrServerClosed {
				serveErrors <- err
			}
		}(i)
	}
	select {
	case <-stopChan:
	case err := <-serveErrors:
		log.Printf("[服务异常] traceId=%s 原因=%v 说明=同时关闭API与管理监听，避免半可用状态", newTraceID(), err)
	}
	fmt.Println("\n正在关闭网关服务...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var shutdown sync.WaitGroup
	for _, server := range servers {
		shutdown.Go(func() {
			if err := server.Shutdown(ctx); err != nil {
				_ = server.Close()
			}
		})
	}
	shutdown.Wait()
	fmt.Println("网关已安全停止。")
}

func printAPIAuthBanner(w io.Writer) {
	fmt.Fprintln(w, "   API 鉴权:      已启用 (密钥已隐藏)")
}

func backgroundTokenRefresher() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	logScheduledTask("token_check", "scheduled", "", newTraceID(), time.Time{}, time.Now().Add(5*time.Minute), "")
	for tick := range ticker.C {
		next := tick.Add(5 * time.Minute)
		trace, started := startScheduledTask("token_check", next)
		accountMu.Lock()
		accs := make([]*Account, len(accounts))
		copy(accs, accounts)
		accountMu.Unlock()
		failed := 0
		for _, acc := range accs {
			if err := ensureValidTokenFor(acc); err != nil {
				failed++
				log.Printf("[BackgroundAuth] 账号 %s 自动检查/续期令牌异常: %v", acc.Path, err)
			}
		}
		status := "success"
		if len(accs) == 0 {
			status = "skipped"
		} else if failed == len(accs) {
			status = "failed"
		} else if failed > 0 {
			status = "partial"
		}
		logScheduledTask("token_check", "completed", status, trace, started, next, fmt.Sprintf("检查账号=%d，异常=%d；仅临近过期时续期", len(accs), failed))
	}
}

type auditResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *auditResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *auditResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

func (w *auditResponseWriter) Flush() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func requestAuditMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceID := r.Header.Get("X-Trace-ID")
		if traceID == "" {
			traceID = uuid.NewString()
		}
		w.Header().Set("X-Trace-ID", traceID)
		// 普通落盘审计也需要稳定的 TraceID / requestID；不能依赖可选 JSON 日志。
		ensureDebugRequestContext(r, traceID, start)
		r = r.WithContext(context.WithValue(r.Context(), statsRequestContextKey{}, &statsAttempt{}))
		if debugLoggingEnabled() {
			debugEvent(r, "info", "request_received", map[string]any{
				"message": "请求已进入网关，准备执行中间件与路由处理",
			})
		}
		debugFullIncomingHeaders(r)
		aw := &auditResponseWriter{ResponseWriter: w}
		log.Printf("[请求到达] traceId=%s 方法=%s 路径=%s 来源=%s 说明=请求已进入网关", traceID, r.Method, r.URL.Path, r.RemoteAddr)
		next.ServeHTTP(aw, r)
		if aw.status == 0 {
			aw.status = http.StatusOK
		}
		statsFinishAttempt(r, aw.status)
		debugEvent(r, "info", "response_returned", map[string]any{
			"status_code": aw.status,
			"result":      http.StatusText(aw.status),
		})
		log.Printf("[响应返回] traceId=%s 状态码=%d 耗时=%v 结果=%s", traceID, aw.status, time.Since(start), http.StatusText(aw.status))
	})
}

// -----------------------------------------------------------------------------
// 中间件: CORS 与可选 APIKey 校验
// -----------------------------------------------------------------------------

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		if r.Method == http.MethodOptions {
			debugEvent(r, "info", "cors_preflight_completed", map[string]any{
				"result":  "allowed",
				"message": "CORS 预检已直接返回，请求未进入业务逻辑",
			})
			log.Printf("[中间件-CORS] traceId=%s 结果=通过并直接响应 说明=预检请求未进入业务方法", w.Header().Get("X-Trace-ID"))
			w.WriteHeader(http.StatusOK)
			return
		}
		debugEvent(r, "debug", "cors_check_passed", map[string]any{"result": "allowed"})
		log.Printf("[中间件-CORS] traceId=%s 结果=通过", w.Header().Get("X-Trace-ID"))
		next.ServeHTTP(w, r)
	})
}

// API 端口只保留原有健康与根路径免鉴权；管理端口使用独立鉴权链。
func isAuthExemptPath(p string) bool {
	return p == "/health" || p == "/ping" || p == "/"
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := currentAPIKey()
		if apiKey != "" && !isAuthExemptPath(r.URL.Path) {
			authHeader := r.Header.Get("Authorization")
			token := strings.TrimPrefix(authHeader, "Bearer ")
			// 兼容部分使用 x-api-key 头携带密钥的客户端。
			if token == "" {
				token = r.Header.Get("x-api-key")
			}
			if subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) != 1 {
				debugEvent(r, "warn", "authentication_rejected", map[string]any{
					"status_code":     http.StatusUnauthorized,
					"reason":          "invalid_api_key",
					"business_impact": "请求未进入业务逻辑",
				})
				log.Printf("[请求被拦截] traceId=%s 拦截层=API鉴权 结果=拒绝 原因=未提供有效API密钥 返回状态码=401 业务影响=请求未进入业务方法", w.Header().Get("X-Trace-ID"))
				writeUpstreamError(w, r, http.StatusUnauthorized, "invalid_api_key", "未提供有效 API 密钥")
				return
			}
		}
		debugEvent(r, "debug", "authentication_passed", map[string]any{"result": "allowed"})
		log.Printf("[中间件-鉴权] traceId=%s 结果=通过", w.Header().Get("X-Trace-ID"))
		next.ServeHTTP(w, r)
	})
}

// -----------------------------------------------------------------------------
// 路由处理: /v1/chat/completions (支持任意 model 透传)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// 轮转退避与抖动
//
// 来源：移植自 Sliverkiss/workbuddy2api 的 internal/server/backoff.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。
//
// 目的：换号重试前先歇一下，让上游频控窗口滑过。立即连环重试会以固定节奏
// 持续撞击 WAF 的密度判罚；抖动则打散多请求的同相位重试（齐步走的退避会
// 以固定周期再次聚团）。
// -----------------------------------------------------------------------------

var (
	// rotateBackoffBase 轮转退避基数（对齐官方 CLI 的 500ms 形态）。
	// 测试可置 0 跳过等待。
	rotateBackoffBase = 500 * time.Millisecond
	// rotateBackoffJitter 抖动比例（±25%，对齐官方 CLI delay×(1±0.25) 形态）。
	rotateBackoffJitter = 0.25
)

const (
	// rotateBackoffCap 轮转退避封顶：轮转上限 = 池大小，封顶只约束大池的极端等待。
	rotateBackoffCap = 8 * time.Second
)

// jitterDur 给时长施加 ±rotateBackoffJitter 的均匀抖动。
// d<=0 原样返回（零等待不抖动）。
func jitterDur(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	f := 1 + (rand.Float64()*2-1)*rotateBackoffJitter
	out := time.Duration(float64(d) * f)
	if out < 0 {
		return 0
	}
	return out
}

// rotateBackoffDelay 返回第 n 次轮转（0 基：首次失败换号前 n=0）应等待的时长：
// base·2^n 封顶 rotateBackoffCap，再施加 ±25% 抖动。base 置 0（测试）时恒 0。
// 用逐次翻倍而非位移：base 调整后无需同步维护移位上限，溢出由封顶比较兜底。
func rotateBackoffDelay(n int) time.Duration {
	d := rotateBackoffBase
	if d <= 0 {
		return 0
	}
	for k := 0; k < n && d < rotateBackoffCap; k++ {
		d *= 2
		if d <= 0 { // 翻倍溢出成非正数：直接按封顶处理
			return jitterDur(rotateBackoffCap)
		}
	}
	if d > rotateBackoffCap {
		d = rotateBackoffCap
	}
	return jitterDur(d)
}

// sleepCtx 可取消的等待：ctx 取消立即返回 false（客户端断连/优雅停机不必等退避
// 睡醒），等满返回 true。d<=0 立即放行。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// upstreamChat 完成「多账号轮询 + 429 冷却代偿 + 授权失效禁用 + 单账号串行」的上游调度。
// 成功时返回 200 响应（调用方负责关闭 Body）与命中的账号/站点；失败时函数内部已写回
// 错误响应并返回 ok=false。Chat Completions 与 Responses 两个入口共用此逻辑。
//
// conversationID 是从请求体提取的会话键，仅用于 prompt_cache_key；会话头族由
// prepareConversationContext/backendHeaders 统一按官方客户端语义生成。
func upstreamChat(w http.ResponseWriter, r *http.Request, reqID uint64, modelName string, upstreamBytes []byte, startTime time.Time, conversationID string, streaming bool) (*http.Response, *Account, *upstreamProfile, bool) {
	prepareConversationContext(r, nil)
	traceID := w.Header().Get("X-Trace-ID")
	log.Printf("[业务入口] traceId=%s requestId=%d 业务=上游模型调用 请求体字节数=%d", traceID, reqID, len(upstreamBytes))
	recordModelRequest(modelName)
	accountMu.Lock()
	poolSize := len(accounts)
	accountMu.Unlock()
	if poolSize == 0 {
		debugEvent(r, "warn", "request_rejected_no_account", map[string]any{
			"status_code":     http.StatusUnauthorized,
			"reason":          "no_auth",
			"business_impact": "没有可用登录凭据，未调用上游模型",
		})
		recordModelFailure(modelName, "no_auth")
		writeUpstreamError(w, r, http.StatusUnauthorized, "no_auth", "未找到有效登录凭据，请先执行 login 命令扫码登录")
		return nil, nil, nil, false
	}
	var lastRateErr string
	var lastAuthErr string
	var lastErr string
	// 最后一次上游非 200 响应的状态码与分类：轮转耗尽的兜底用它把真实原因
	// 透传给客户端（而不是笼统的「所有账号均处于冷却状态」——那会让客户端
	// 在「模型名错了」「工具定义无效」这类可自纠的问题上陷入无意义重试）。
	lastErrStatus := 0
	var lastKind errKind
	// 降级重试状态（请求级）：degradeApplied 保证单请求内只降级一次；
	// currentBody 为当前生效的请求体（降级重试时会被重写）。
	degradeApplied := false
	currentBody := upstreamBytes
	attempted := make(map[*Account]bool, poolSize)
	for attempt := 0; attempt < poolSize; attempt++ {
		// 轮转退避（第 2 次尝试起）：换号前先歇一下让上游频控窗口滑过。
		// 首次尝试不等待（正常单号请求零开销）；ctx 取消（客户端断连/优雅停机）
		// 立即终止轮转——客户端已走，换号重试无意义。
		if attempt > 0 {
			if d := rotateBackoffDelay(attempt - 1); d > 0 {
				if !sleepCtx(r.Context(), d) {
					log.Printf("[#%d] 轮转退避被中断（客户端断连或服务停机），终止换号重试", reqID)
					return nil, nil, nil, false
				}
			}
		}
		acc, selection, err := nextAccountForModel(modelName, attempted)
		if err != nil {
			if errors.Is(err, errModelAccountPolicy) {
				log.Printf("[请求被拒绝] traceId=%s requestId=%d 模型=%s 原因=账号黑白名单没有匹配的凭据 返回状态码=403 业务影响=未进入上游", traceID, reqID, modelName)
				debugEvent(r, "warn", "model_account_policy_rejected", map[string]any{
					"status_code": http.StatusForbidden, "reason": "没有符合模型账号名单的凭据", "business_impact": "未进入上游调用",
				})
				recordModelFailure(modelName, "账号名单拒绝")
				writeUpstreamError(w, r, http.StatusForbidden, "model_account_disabled", fmt.Sprintf("模型 %s 没有可用的凭据文件：已被账号黑白名单禁用", modelName))
				return nil, nil, nil, false
			}
			// 所有账号均不可用（冷却或失效）
			msg := fmt.Sprintf("无可用账号: %v", err)
			if lastAuthErr != "" {
				msg += " | 最近一次授权失效: " + truncate(lastAuthErr, 200)
			}
			if lastRateErr != "" {
				msg += " | 最近一次频率限制: " + truncate(lastRateErr, 200)
			}
			if lastErr != "" {
				msg += " | 最近一次上游错误: " + truncate(lastErr, 200)
			}
			log.Printf("[#%d] %s", reqID, msg)
			debugEvent(r, "warn", "request_rejected_no_available_account", map[string]any{
				"status_code":     http.StatusServiceUnavailable,
				"reason":          "no_available_account",
				"business_impact": "未调用上游模型",
			})
			recordModelFailure(modelName, "无可用账号")
			writeUpstreamError(w, r, http.StatusServiceUnavailable, "no_available_account", msg)
			return nil, nil, nil, false
		}
		attempted[acc] = true
		debugSetAccount(r, acc.Path)
		debugEvent(r, "debug", "account_selected", map[string]any{
			"selection": string(selection),
			"attempt":   attempt + 1,
		})
		if modelAccountRuleConfigured(modelName) {
			// 仅在实际命中账号的日志里附一个短标记，不再逐账号刷名单日志。
			log.Printf("[#%d] 账号名单命中: 模型=%s 凭据=%s（已按 config.json 账号黑白名单筛选）", reqID, modelName, filepath.Base(acc.Path))
			debugEvent(r, "debug", "model_account_policy_hit", map[string]any{
				"model": modelName, "account_file": filepath.Base(acc.Path),
			})
		}
		switch selection {
		case selectionFreeExhausted:
			log.Printf("[FreeModel] requestId=%d 请求的是已知免费模型 %s，选择付费余额耗尽账号 %s 发起请求", reqID, modelName, acc.Path)
		case selectionProbeExhausted:
			log.Printf("[ModelProbe] requestId=%d 账号 %s 付费余额已耗尽，但模型 %s 收费属性未知；执行一次受控探测，5分钟内不重复探测", reqID, acc.Path, modelName)
		}
		if err := ensureValidTokenFor(acc); err != nil {
			lastAuthErr = err.Error()
			continue
		}
		// 令牌刷新时若发现授权失效会禁用账号；若被禁用则跳过换下一个
		accountMu.Lock()
		disabled := acc.Disabled
		accountMu.Unlock()
		if disabled {
			lastAuthErr = acc.DisabledReason
			continue
		}

		// 按账号所属站点（国内站/国际站）路由上游与指纹 Header
		prof := acc.Profile()

		// prompt_cache_key 注入（费用优化，费用降约 17 倍）：必须在轮转循环内按
		// **当前账号**计算——键里带账号 UID 隔离段，跨账号复用会命中他人前缀缓存
		// 并泄露对话内容。同一账号内的换号重试复用同键（uid 相同、会话相同 → 同键），
		// 因此只在首次尝试后缓存：本循环每轮都要为不同账号重算。
		reqBody := currentBody
		if acc.Auth != nil {
			reqBody = injectPromptCacheKey(currentBody, acc.Auth.Account.UID, conversationID)
		}

		debugEvent(r, "info", "upstream_request_started", map[string]any{
			"upstream_host": prof.Base,
			"attempt":       attempt + 1,
			"payload_bytes": len(reqBody),
		})
		// 注入 CodeBuddy 凭据与指纹 Header
		// 限制同一账号向腾讯上游的请求严格单并发串行排队，防止并发双发触发腾讯风控
		acc.lock.Lock()
		statsSelectAccount(r, acc, modelName)
		resp, upstreamCancel, err := doUpstreamRequest(r, acc, prof, reqBody, reqID, traceID)
		acc.lock.Unlock()
		if err != nil {
			debugEvent(r, "error", "upstream_request_failed", map[string]any{
				"upstream_host": prof.Base,
				"error_type":    debugErrorType(err),
				"error":         safeDebugError(err),
			})
			log.Printf("[异常] traceId=%s requestId=%d 发生阶段=上游网络调用 账号=%s 异常=%v 业务影响=本次模型请求失败 是否已处理=是", traceID, reqID, acc.Path, err)
			log.Printf("[#%d] 账号 %s [%s] 上游请求失败: %v", reqID, acc.Path, prof.Label, err)
			recordModelFailure(modelName, "网络错误")
			writeUpstreamError(w, r, http.StatusBadGateway, "upstream_network_error", fmt.Sprintf("网络转发失败: %v", err))
			return nil, nil, nil, false
		}

		// 上游非 200 响应处理
		if resp.StatusCode >= 400 {
			statsFinishAttempt(r, resp.StatusCode)
			errBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			upstreamCancel()
			errStr := string(errBody)
			debugFullUpstreamError(r, resp, errBody)
			debugEvent(r, "warn", "upstream_response_rejected", map[string]any{
				"upstream_host": prof.Base,
				"status_code":   resp.StatusCode,
				"error_code":    modelStatusFromError(resp.StatusCode, errStr),
			})
			log.Printf("[#%d] 账号 %s [%s] 上游返回 HTTP %d: %s (耗时 %v)", reqID, acc.Path, prof.Label, resp.StatusCode, errStr, time.Since(startTime))
			log.Printf("[外部接口] traceId=%s requestId=%d 上游=%s 状态码=%d 结果=失败 账号=%s", traceID, reqID, prof.Base, resp.StatusCode, acc.Path)

			kind := classifyUpstream(resp.StatusCode, errStr)

			// 记录最后一次上游失败（原文/状态码/分类，见 lastErrStatus 声明处的
			// 说明）。统一在分类后记录，不依赖各分支自行赋值。
			lastErr, lastErrStatus, lastKind = errStr, resp.StatusCode, kind

			// 请求级错误（上下文超限）：换任何账号都会得到同样结果，
			// 轮转纯属浪费健康号配额，直接透传上游原文。
			if !kind.rotatesAccount() {
				recordModelFailure(modelName, kind.String())
				writeOpenAIErrorHint(w, resp.StatusCode, "upstream_error",
					fmt.Sprintf("upstream %d: %s", resp.StatusCode, errStr), kind)
				return nil, nil, nil, false
			}

			switch kind {
			case errHardCredit:
				markModelQuotaBlocked(acc, modelName, errStr)
				if selection == selectionProbeExhausted {
					msg := fmt.Sprintf("当前模型 %s 已在余额耗尽账号 %s 上完成受控探测并确认需要付费额度，本次不再探测其他耗尽账号", modelName, acc.Path)
					log.Printf("[#%d] %s", reqID, msg)
					recordModelFailure(modelName, modelStatusFromError(resp.StatusCode, errStr))
					writeUpstreamError(w, r, http.StatusServiceUnavailable, "model_requires_quota", msg)
					return nil, nil, nil, false
				}
				if selection == selectionExploreProbe {
					log.Printf("[CostExplore] 账号 %s 搭车学习失败（模型 %s 需付费额度），回退常规选号继续本次请求", acc.Path, modelName)
				}
				lastRateErr = errStr
				continue

			case errModelBlocked:
				until, ok := parseResetTime(errStr)
				if !ok {
					until = time.Now().Add(60 * time.Second)
				}
				markModelCooldown(acc, modelName, until, errStr)
				if selection == selectionProbeExhausted {
					msg := fmt.Sprintf("当前模型 %s 在余额耗尽账号 %s 的受控探测中触发模型级限流，本次不再探测其他耗尽账号", modelName, acc.Path)
					recordModelFailure(modelName, modelStatusFromError(resp.StatusCode, errStr))
					writeUpstreamError(w, r, http.StatusServiceUnavailable, "model_rate_limited", msg)
					return nil, nil, nil, false
				}
				if selection == selectionExploreProbe {
					log.Printf("[CostExplore] 账号 %s 搭车学习失败（模型 %s 触发模型级限流），回退常规选号继续本次请求", acc.Path, modelName)
				}
				lastRateErr = errStr
				continue

			case errSoftRate:
				// 429 频率限制：解析重置时间并屏蔽该账号，交由其他账号代偿
				until, ok := parseResetTime(errStr)
				if !ok {
					until = time.Now().Add(60 * time.Second) // 无法解析时默认冷却 60 秒
				}
				markCooldown(acc, until, errStr)
				lastRateErr = errStr
				continue // 尝试下一个账号

			case errWafBlock:
				// WAF 拦截（403 + 无业务信封：HTML 拦截页/空体）：拦的是出口 IP
				// 而非账号，账号本身健康。软冷却让本账号避让，轮换到其他账号继续，
				// **绝不 disableAccount**——那会删除凭据文件（WAF 误判为授权失效）。
				markCooldown(acc, time.Now().Add(wafCooldownBase), "WAF 拦截 (HTTP 403)")
				log.Printf("[#%d] 账号 %s [%s] 命中 WAF 拦截，软冷却 %v 后重试（未禁用账号）",
					reqID, acc.Path, prof.Label, wafCooldownBase)
				lastRateErr = errStr
				continue // 尝试下一个账号

			case errAccountFault:
				// 账号级授权/配额故障（11140 request illegal / 14017 trial 未激活）：
				// 由账号自身状态决定，短冷却后仍会复现，需重新登录才能恢复。
				// 与 session_dead 的区别：这里不删凭据（可能是临时风控），只冷却轮换。
				markCooldown(acc, time.Now().Add(accountFaultCooldown), "账号级故障: "+truncate(errStr, 80))
				log.Printf("[#%d] 账号 %s [%s] 账号级故障 (%s)，冷却 %v 后轮换（凭据保留）",
					reqID, acc.Path, prof.Label, kind, accountFaultCooldown)
				lastAuthErr = errStr
				continue

			case errSessionDead:
				// 会话失效（401 / 12153）：真授权失效，需重新登录
				disableAccount(acc, fmt.Sprintf("上游鉴权失败 (HTTP %d): %s", resp.StatusCode, truncate(errStr, 200)))
				lastAuthErr = errStr
				continue // 尝试下一个账号

			case errContentBlocked:
				// 内容拦截误报处理：passthrough/append 模式首遇时触发降级
				//（到次日 00:00 CST），换中性提示词同请求内重试一次。
				// append 降级重试退化为 replace——原文在场只会确定性再撞 400。
				// custom 模式已是网关提示词，再拦说明非指纹误报，不进入降级。
				// 第二次仍被拦（用户内容本身触发审核）→ 不轮转（换号结果相同），
				// 透传错误。内容问题非账号问题：不罚账号。
				if cfg.PromptMode != "custom" && !degradeApplied {
					degrade.Trigger()
					currentBody = rewriteSystemTo(currentBody, degradedPrompt)
					degradeApplied = true
					delete(attempted, acc) // 释放本账号，降级重试可再命中
					log.Printf("[#%d] 账号 %s [%s] 内容拦截（疑似指纹误报），降级至 %s，中性提示词重试",
						reqID, acc.Path, prof.Label, degrade.until.Format("2006-01-02 15:04"))
					continue
				}
				log.Printf("[#%d] 账号 %s [%s] 内容拦截（%s），不罚账号不轮转，透传错误",
					reqID, acc.Path, prof.Label, kind)
				recordModelFailure(modelName, kind.String())
				writeOpenAIError(w, http.StatusBadRequest, "content_blocked",
					"content blocked by upstream content firewall")
				return nil, nil, nil, false

			case errBadParams:
				// 请求体畸形（11101）：账号健康，不冷却，但仍轮转——
				// 不同账号可能有不同的模型权限，值得再试一次。
				log.Printf("[#%d] 账号 %s [%s] 请求级错误 (%s)，不罚账号但轮转重试",
					reqID, acc.Path, prof.Label, kind)
				lastErr = errStr
				continue

			case errNotFound, errServer, errClient:
				// 上游偶发 / 服务端故障 / 其他 4xx：换号重试（默认行为）
				recordModelFailure(modelName, modelStatusFromError(resp.StatusCode, errStr))
				lastErr = errStr
				continue

			default:
				recordModelFailure(modelName, modelStatusFromError(resp.StatusCode, errStr))
				writeOpenAIErrorHint(w, resp.StatusCode, "upstream_error",
					fmt.Sprintf("upstream %d: %s", resp.StatusCode, errStr), kind)
				return nil, nil, nil, false
			}
		}

		debugEvent(r, "info", "upstream_response_received", map[string]any{
			"upstream_host": prof.Base,
			"status_code":   resp.StatusCode,
		})
		log.Printf("[外部接口] traceId=%s requestId=%d 上游=%s 状态码=%d 结果=成功 账号=%s", traceID, reqID, prof.Base, resp.StatusCode, acc.Path)
		recordModelSuccess(modelName)
		// 用空闲看门狗包装响应体：持续有数据就永不超时，超过 upstreamIdleTimeout 无数据才中断。
		// 关闭响应体时会一并取消上游 context，完成清理。
		resp.Body = newIdleReadCloser(resp.Body, currentIdleTimeout(), upstreamCancel)
		return resp, acc, prof, true
	}

	// 轮转耗尽（poolSize 次尝试均未成功）。区分两种情形：
	//
	//  - 全程没有任何上游非 200 响应（账号全部 token 失效 / 被禁用，循环内
	//    continue 掉）：确实没有可透传的上游信息，保持原语义 429
	//    all_accounts_cooldown。
	//  - 每次尝试都收到上游 4xx/5xx：透传**最后一次**的真实状态码与原文，并附
	//    gateway_hint。笼统的 429 会掩盖根因——例如全部账号都返回 11102（模型
	//    在该站点不存在）时，客户端需要的是「模型名错了」而不是「账号都在冷却」：
	//    前者改一次模型名即可自纠，后者只会引发无意义的重试循环。生产实测
	//    （2026-09-18）中 11129「工具定义无效」×4 账号轮转后报 429，排查成本
	//    远高于读一条正确的错误。
	//
	//  WAF 拦截是唯一特例：拦截页是 HTML，透传给 OpenAI 客户端无意义，改发
	//  503 + 专用错误类型，语义是「服务暂时被风控，稍后重试」。
	if lastErrStatus > 0 {
		recordModelFailure(modelName, lastKind.String())
		if lastKind == errWafBlock {
			writeOpenAIErrorHint(w, http.StatusServiceUnavailable, "upstream_waf_blocked",
				"all accounts were blocked by upstream WAF; retry after the block window", lastKind)
			return nil, nil, nil, false
		}
		writeOpenAIErrorHint(w, lastErrStatus, "upstream_error",
			fmt.Sprintf("upstream %d: %s", lastErrStatus, lastErr), lastKind)
		return nil, nil, nil, false
	}
	recordModelFailure(modelName, "all_cooldown")
	debugEvent(r, "warn", "request_rejected_all_cooldown", map[string]any{
		"status_code":     http.StatusTooManyRequests,
		"reason":          "all_accounts_cooldown",
		"business_impact": "未调用上游模型",
	})
	writeUpstreamError(w, r, http.StatusTooManyRequests, "all_accounts_cooldown", "所有账号均处于冷却状态")
	return nil, nil, nil, false
}

func handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeOpenAIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "仅支持 POST 请求")
		return
	}

	reqID := requestIDFor(r)
	startTime := requestStartFor(r)

	readStarted := debugBodyReadStarted(r)
	bodyBytes, err := io.ReadAll(r.Body)
	readDuration := debugElapsedSince(readStarted)
	if err != nil {
		debugBodyReadFailed(r, bodyBytes, readStarted, err)
		writeOpenAIError(w, http.StatusBadRequest, "read_error", "读取请求体失败")
		return
	}
	defer r.Body.Close()

	var reqObj map[string]any
	decodeStarted := time.Now()
	decodeErr := json.Unmarshal(bodyBytes, &reqObj)
	decodeDuration := time.Since(decodeStarted)
	if decodeErr != nil {
		debugBodyReadCompleted(r, bodyBytes, readDuration, decodeDuration, false, decodeErr)
		writeOpenAIError(w, http.StatusBadRequest, "invalid_json", "无效的 JSON 请求体")
		return
	}

	// 核心特性：完全透传 model 字段
	// 客户端传什么 model，我们就透传什么 model 给上游，不做任何硬编码限制！
	modelName, _ := reqObj["model"].(string)
	if modelName == "" {
		modelName = "hy4-preview" // 默认保底
		reqObj["model"] = modelName
	}

	isStream, _ := reqObj["stream"].(bool)
	debugSetModelAndStream(r, modelName, isStream)
	debugBodyReadCompleted(r, bodyBytes, readDuration, decodeDuration, true, nil)
	prepareConversationContext(r, reqObj)

	// 模型黑白名单拦截：命中即拒绝，不消耗任何上游账号额度。
	if disabled, reason := modelDisabled(modelName); disabled {
		log.Printf("[请求被拒绝] traceId=%s requestId=%d 拦截层=模型黑白名单 模型=%s 结果=拒绝 原因=%s 返回状态码=403 业务影响=请求未进入上游调用",
			w.Header().Get("X-Trace-ID"), reqID, modelName, reason)
		debugEvent(r, "warn", "model_blocked_by_config", map[string]any{
			"status_code":     http.StatusForbidden,
			"reason":          reason,
			"business_impact": "模型被网关配置禁用，请求未进入上游调用",
		})
		writeOpenAIError(w, http.StatusForbidden, "model_disabled", reason)
		return
	}

	// 腾讯上游强制要求 stream 必须为 true，非流式会被拦截 (code 11101)
	reqObj["stream"] = true

	// OpenAI 新别名翻译：上游只认 max_tokens，直接透传 max_completion_tokens 会 400
	translateMaxCompletionTokens(reqObj)

	// 深度思考 (Thinking) 自动适配：混元系列如果未关闭思考，自动赋予 high 档位保证深度思考输出
	applyThinkingRules(reqObj, modelName)

	// reasoning_effort 档位降级：客户端传的档位模型不支持时（如对只支持到 high 的
	// 模型传 "max"）上游会 400。降级到 ≤请求档位的最高支持档。档位表来自实时目录
	// （reasoning.supportedEfforts），未收录的模型一律透传（不猜测）。
	normalizeReasoningEffort(reqObj, supportedEffortsFor(modelName))

	// DeepSeek 多轮思维链一致性：会话历史里带过 reasoning 痕迹时，上游要求所有
	// assistant 消息都带 reasoning_content 字段，否则多轮请求被判不一致。
	// 必须排在 sanitizeMessages 之前——新生成的 reasoning_content 同样要过脱敏。
	backfillReasoningContent(reqObj)

	// tool_choice 归一化：上游把该字段定义为 string，OpenAI 官方 SDK 默认发对象形式
	// （{"type":"function",...}），直接透传会 400 code=11101。
	normalizeToolChoice(reqObj)

	// 系统提示词三模式（prompt-mode）：
	//   passthrough（缺省）：透传客户端原始 system
	//   custom：网关提示词替换所有 system/developer
	//   append：在开头连续 system 块之后插入网关提示词（客户端规范与网关提示词并用）
	// 降级期（degrade.Active）内 passthrough/append 退化为 replace 语义：
	// 原文在场只会确定性再撞内容审核（见 upstreamChat 的降级重试）。
	switch cfg.PromptMode {
	case "custom":
		promptReplaceSystem(reqObj, effectivePromptText())
	case "append":
		if degrade.Active() {
			promptReplaceSystem(reqObj, degradedPrompt)
		} else {
			promptAppendSystem(reqObj, effectivePromptText())
		}
	default: // passthrough
		if degrade.Active() {
			promptReplaceSystem(reqObj, degradedPrompt)
		}
	}

	// 模板净化：改写 Claude Code 等框架被腾讯官方逐字拉黑的固定 prompt 语句
	if modified := sanitizeMessages(reqObj); modified > 0 {
		logSystemPrefixSanitized(r, reqID, modified)
	}

	// 会话结构归一化：保证首条消息为 system，修复部分非 harness 客户端
	//（以 assistant / tool 续写或回传工具结果）触发的上游 11128 错误。
	// prepareSystemPromptForUpstream 内部先调用 ensureLeadingSystemMessage，再套用
	// config.json 的 fallback/force；此顺序必须早于配对归一化。
	prepareSystemPromptForUpstream(reqObj, r, reqID, w.Header().Get("X-Trace-ID"))
	// 输出预算归一化：客户端未声明 max_tokens 时补模型输出上限，
	// 避免上游默认预留 384000 导致长会话在远未到窗口上限时被 11133 拒绝
	ensureUpstreamMaxTokens(reqObj, r, reqID, w.Header().Get("X-Trace-ID"), modelName)
	repairReport := repairToolMessageSequence(reqObj)
	logToolSequenceRepair(r, w.Header().Get("X-Trace-ID"), reqID, modelName, repairReport)
	// 11155 防护：与 Responses 入口共用出站推理历史回填，不伪造思维链正文。
	logReasoningHistoryRepair(r, reqID, modelName, repairReasoningHistory(reqObj))

	// tool 配对归一化：先重排（结果中间夹的非 tool 消息挪后）、再清理（删孤儿
	// tool_call / tool 结果）。工具执行失败的会话历史会被上游判 11148 顶死整条
	// 会话，这里是最后一道防线——宁可丢一轮工具上下文，也好过会话报废。
	normalizeToolPairing(reqObj)

	upstreamBytes, err := json.Marshal(reqObj)
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "encode_error", "序列化请求失败")
		return
	}

	if cfg.Verbose {
		log.Printf("[#%d][Req] Model: %s | Stream: %v | BodyLen: %d", reqID, modelName, isStream, len(upstreamBytes))
	} else {
		log.Printf("[#%d] POST /v1/chat/completions -> Upstream [Model: %s, Stream: %v]", reqID, modelName, isStream)
	}

	resp, acc, prof, ok := upstreamChat(w, r, reqID, modelName, upstreamBytes, startTime, resolveConversationID(reqObj), isStream)
	if !ok {
		return
	}
	if isStream {
		streamChatResponse(w, r, resp, modelName, reqID, acc, prof, startTime)
	} else {
		writeChatAggregate(w, r, resp, modelName, reqID, acc, prof, startTime)
	}
}

// streamChatResponse 将上游 SSE 逐行透传为 OpenAI Chat Completions 流式响应。
func streamChatResponse(w http.ResponseWriter, r *http.Request, resp *http.Response, modelName string, reqID uint64, acc *Account, prof *upstreamProfile, startTime time.Time) {
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeOpenAIError(w, http.StatusInternalServerError, "streaming_unsupported", "服务器不支持流式响应 Flush")
		return
	}

	body := newTTFTReader(resp.Body, startTime)
	defer finishFirstToken(r, modelName, reqID, body)
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var usage map[string]any
	// toolCallSeen 跨帧记录 delta.tool_calls 里已发过首片的 index，供逐 chunk 透传时
	// 收敛 name 为「每 index 一次」（对齐 OpenAI 官方流，防累加型客户端把工具名拼成
	// Bash×帧数）。
	toolCallSeen := map[int]bool{}
	finishReason := ""
	sawDone := false
	for scanner.Scan() {
		cleanData := stripDataPrefix(scanner.Text())
		if cleanData == "" {
			continue
		}
		if cleanData == "[DONE]" {
			// 先记住，等确认有真实 finish_reason 再转发。
			// 没有结束原因时转发 [DONE]，等于告诉客户端这次是正常结束。
			sawDone = true
			break
		}
		var chunk map[string]any
		if json.Unmarshal([]byte(cleanData), &chunk) == nil {
			observeFirstToken(body, chunk)
			if u, ok := chunk["usage"].(map[string]any); ok {
				usage = u
			}
			// 上游 error 帧（带 error 键）原样透传，但附加网关视角的 gateway_hint
			// 补充说明——message 原文一律不动，hint 并列添加（见 attachHintToErrorFrame）。
			if _, hasErr := chunk["error"]; hasErr {
				cleanData = attachHintToErrorFrame(cleanData, frameGatewayHint(cleanData))
			} else {
				stripToolCallNames(chunk, toolCallSeen)
				if raw, err := json.Marshal(chunk); err == nil {
					cleanData = string(raw)
				}
			}
			finishReason = noteFinishReason(finishReason, chunk)
		}
		if cleanedChunk := cleanChunkJSON(cleanData); cleanedChunk != "" {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", cleanedChunk)
			flusher.Flush()
		}
	}
	scanErr := scanner.Err()
	completeTokenStream(body, scanErr == nil && finishReason != "")
	if scanErr != nil {
		// 流被中断（上游卡死 / 超时 / 客户端取消）时，绝不补发 [DONE]：
		// 补发等于告诉下游「正常结束」，会让残缺的工具调用被当成完整结果执行。
		// 改为下发一条标准错误事件，让 CPA / 客户端明确知道这次响应不完整。
		debugEvent(r, "error", "stream_response_failed", map[string]any{
			"error_type": debugErrorType(scanErr),
			"error":      safeDebugError(scanErr),
		})
		reason := "上游流式响应中断，本次回复不完整"
		if errors.Is(scanErr, context.DeadlineExceeded) {
			reason = fmt.Sprintf("上游超过 %v 无数据，判定连接卡死并中断，本次回复不完整", currentIdleTimeout())
		}
		errPayload, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"message": reason,
				"type":    "upstream_stream_interrupted",
				"code":    "stream_interrupted",
			},
		})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", errPayload)
		flusher.Flush()
		log.Printf("[异常] traceId=%s requestId=%d 发生阶段=上游流式读取 账号=%s 异常=%v 业务影响=本次响应不完整，已向客户端下发中断错误而非伪造结束", debugTraceID(r), reqID, acc.Path, scanErr)
	} else if finishReason == "" {
		// 连接是正常关掉的（没有读错误），但整段流没有非空 finish_reason。
		// 这不是成功：不能补 [DONE]，也不能记成 stream_response_completed。
		debugEvent(r, "error", "stream_closed_without_finish", map[string]any{
			"error_type":      "stream_closed_without_finish",
			"finish_reason":   "",
			"saw_done":        sawDone,
			"business_impact": "上游干净结束但没有 finish_reason，已拒绝当成成功，未补发 [DONE]",
		})
		errPayload, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"message": errStreamClosedWithoutFinish.Error(),
				"type":    "upstream_stream_incomplete",
				"code":    "stream_closed_without_finish",
			},
		})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", errPayload)
		flusher.Flush()
		log.Printf("[异常] traceId=%s requestId=%d 发生阶段=上游流结束 账号=%s 结果=拒绝当成成功 原因=连接正常关闭但没有非空 finish_reason 是否看到DONE=%t 业务影响=不补发 [DONE]，客户端收到 stream_closed_without_finish 是否已处理=是",
			debugTraceID(r), reqID, acc.Path, sawDone)
	} else {
		if sawDone {
			_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
			flusher.Flush()
		}
		debugEvent(r, "info", "stream_response_completed", map[string]any{
			"status_code":   http.StatusOK,
			"finish_reason": finishReason,
			"saw_done":      sawDone,
		})
	}
	observeModelCredit(acc, modelName, usage, reqID)
	statsObserveUsage(r, usage, scanErr == nil && finishReason != "")
	recordModelTokens(modelName, usage, reqID)
	recordModelLatency(modelName, time.Since(startTime))
	if scanErr == nil && finishReason != "" {
		log.Printf("[#%d] 流式输出完成 (账号 %s [%s], 结束原因=%s, 耗时 %v, 首Token(v2) %s)", reqID, acc.Path, prof.Label, finishReason, time.Since(startTime), body.text())
	}
}

// writeChatAggregate 聚合上游 SSE 为完整 Chat Completions JSON 响应。
func writeChatAggregate(w http.ResponseWriter, r *http.Request, resp *http.Response, modelName string, reqID uint64, acc *Account, prof *upstreamProfile, startTime time.Time) {
	defer resp.Body.Close()
	body := newTTFTReader(resp.Body, startTime)
	defer finishFirstToken(r, modelName, reqID, body)
	completionJSON, err := aggregateCompletion(body, modelName)
	if err != nil {
		debugEvent(r, "error", "aggregate_response_failed", map[string]any{
			"error_type": debugErrorType(err),
			"error":      safeDebugError(err),
		})
		logAggregateFailure(r, reqID, err)
		writeOpenAIError(w, http.StatusInternalServerError, "aggregate_error", "聚合上游流式响应失败: "+err.Error())
		return
	}
	usage := usageFromCompletion(completionJSON)
	statsObserveUsage(r, usage, true)
	observeModelCredit(acc, modelName, usage, reqID)
	recordModelTokens(modelName, usage, reqID)
	recordModelLatency(modelName, time.Since(startTime))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(completionJSON)
	debugEvent(r, "info", "aggregate_response_completed", map[string]any{
		"status_code":    http.StatusOK,
		"response_bytes": len(completionJSON),
	})
	log.Printf("[#%d] 非流式响应完成 (账号 %s [%s], 耗时 %v, 首Token(v2) %s)", reqID, acc.Path, prof.Label, time.Since(startTime), body.text())
}

// -----------------------------------------------------------------------------
// 路由处理: /v1/models (列出常见模型供客户端自动补全)
// -----------------------------------------------------------------------------

func handleModels(w http.ResponseWriter, r *http.Request) {
	modelIDs, source := mergedModelIDs()
	modelsList := make([]map[string]any, 0, len(modelIDs))
	for _, id := range modelIDs {
		entry := map[string]any{
			"id": id, "object": "model", "owned_by": "workbuddy", "permission": []any{},
		}
		// 能力扩展字段：只透传目录里真实存在的值，未知一律省略，不做 1M 之类兜底。
		// context_length 是模型能力上限，default_context_length 是客户端默认预算，
		// 两者语义不同，必须分开给，不能合并成一个值。
		// 这里只补充描述信息，不注入上游请求参数、不截断消息，也不改变输出预算逻辑。
		if mc, ok := modelCapabilityFor(id); ok {
			if mc.Ceiling > 0 {
				entry["context_length"] = mc.Ceiling
			}
			if mc.MaxOutput > 0 {
				entry["max_output_tokens"] = mc.MaxOutput
			}
			if mc.Default > 0 {
				entry["default_context_length"] = mc.Default
			}
			if len(mc.Options) >= 2 {
				entry["context_length_options"] = mc.Options
			}
			if len(mc.BySite) > 1 {
				// 站点之间能力不一致：顶层已留空，明细在此，避免下游按较大值预留。
				entry["capabilities_by_site"] = mc.BySite
			}
			// fork 的推理能力字段：实时目录与 npm 目录声明，客户端可据此避免盲传非法档位。
			if m, ok := firstCatalogEntry(id); ok {
				if m.SupportsImages {
					entry["supports_images"] = true
				}
				if m.SupportsToolCall {
					entry["supports_tool_call"] = true
				}
				if len(m.SupportedEfforts) > 0 {
					entry["reasoning_supported_efforts"] = m.SupportedEfforts
					if m.DefaultEffort != "" {
						entry["reasoning_default_effort"] = m.DefaultEffort
					}
				}
			}
		}
		modelsList = append(modelsList, entry)
	}
	resp := map[string]any{
		"object": "list",
		"data":   modelsList,
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Model-Source", modelSourceLabel(source))
	_ = json.NewEncoder(w).Encode(resp)
}

// firstCatalogEntry 按站点顺序取模型目录条目（首个命中）；未收录返回 false。
// 站点间对同一模型的窗口声明可能不同，此处不合并、不取极值——取声明方原值，
// 避免把两站的窗口拼成一个上游从未声明的数字。
func firstCatalogEntry(modelID string) (catalogModel, bool) {
	for _, site := range catalogSites {
		if m, ok := modelEntry(site, modelID); ok {
			return m, true
		}
	}
	return catalogModel{}, false
}

// supportedEffortsFor 汇总模型在各站点的推理档位（站点间可能不同，取并集）。
//
// 并集口径的理由：档位降级只用于「客户端传了上游不认的档位」这一种情形，
// 而请求最终落到哪个站点由选号决定、此时尚未可知。取并集意味着跨站请求不会被
// 误降级（某站支持的档位在另一站不支持的极端情形下仍可能 400，但那属于上游
// 自身不一致，网关不猜测）。未收录任何档位 → 返回 nil，调用方一律透传。
func supportedEffortsFor(modelID string) map[string][]string {
	var union []string
	seen := map[string]bool{}
	for _, site := range catalogSites {
		m, ok := modelEntry(site, modelID)
		if !ok {
			continue
		}
		for _, e := range m.SupportedEfforts {
			e = strings.TrimSpace(strings.ToLower(e))
			if e == "" || seen[e] {
				continue
			}
			seen[e] = true
			union = append(union, e)
		}
	}
	if len(union) == 0 {
		return nil
	}
	return map[string][]string{modelID: union}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	modelIDs, source := mergedModelIDs()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":       "healthy",
		"timestamp":    time.Now().Unix(),
		"version":      version,
		"model_count":  len(modelIDs),
		"model_source": modelSourceLabel(source),
	})
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprintf(w, "WorkBuddy Local Gateway v%s is running.\n\nEndpoints:\n- POST /v1/chat/completions\n- POST /v1/responses\n- GET  /v1/models\n- GET  /health\n", version)
}

// -----------------------------------------------------------------------------
// 请求转译与净化工具函数
// -----------------------------------------------------------------------------

func applyThinkingRules(obj map[string]any, modelName string) {
	// 遵循 CodeBuddy 规范：仅当客户端显式设置了 reasoning_effort 时才传递与规范化
	// 绝不可强行对普通请求注入 reasoning_effort，否则极易触发腾讯内容与安全策略拦截 (code 11-128)
	//
	// 双字段兼容：OpenAI 生态同时存在 snake_case（官方 SDK）与 camelCase
	// （部分 JS 客户端）两种拼写。只认其一会让另一种绕过本函数直抵上游，
	// 既可能触发上述拦截，也会在关闭思考时留下一个上游不认的字段。
	for _, key := range []string{"reasoning_effort", "reasoningEffort"} {
		currEff, exists := obj[key].(string)
		if !exists {
			continue
		}
		if currEff == "" || strings.EqualFold(strings.TrimSpace(currEff), "off") ||
			strings.EqualFold(strings.TrimSpace(currEff), "none") {
			delete(obj, key)
		}
	}
	if _, hasSnake := obj["reasoning_effort"]; !hasSnake {
		if _, hasCamel := obj["reasoningEffort"]; !hasCamel {
			delete(obj, "reasoning_summary")
			return
		}
	}
	// 客户端显式请求思考时，设置 auto
	obj["reasoning_summary"] = "auto"
}

// -----------------------------------------------------------------------------
// 会话头族：聚合主键与消息级 ID
//
// 背景：同一次用户操作内的多次上游尝试（换号重试 / 降级重发）此前各自生成
// 独立的 X-Request-ID，上游后台把它们记成碎片化的多次调用。会话头族让这些
// 尝试共享同一个 X-Conversation-Request-ID 聚合主键，后台按对话轮聚合。
// -----------------------------------------------------------------------------

// newMessageID 生成消息级 ID：32 位 hex（UUID v4 去横线的长度形态），对齐官方
// X-Request-ID / X-Conversation-Message-ID。crypto/rand 失败时回落 uuid（同为
// crypto 随机源）——恒 32 hex、恒合法，可直接用作 B3 TraceId。
func newMessageID() string {
	b := make([]byte, 16)
	if _, err := cryptorand.Read(b); err == nil {
		return hex.EncodeToString(b)
	}
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

// resolveConversationID 从已解析的请求体提取会话键（conversation 维度）。
// 四个键按序尝试：metadata.conversation_id → metadata.conversationId →
// conversation_id → conversationId。缺失返回 ""（不伪造：透传客户端原值优先，
// 客户端没给就不发 X-Conversation-ID）。
//
// 注意不含 user 维度：user 的粒度远粗于上游对话级缓存的边界，
// 一个 user 的全部并行对话会被钉到同一聚合键上。
func resolveConversationID(obj map[string]any) string {
	if meta, ok := obj["metadata"].(map[string]any); ok {
		if v, _ := meta["conversation_id"].(string); v != "" {
			return v
		}
		if v, _ := meta["conversationId"].(string); v != "" {
			return v
		}
	}
	if v, _ := obj["conversation_id"].(string); v != "" {
		return v
	}
	v, _ := obj["conversationId"].(string)
	return v
}

// validB3TraceID 判断 B3 TraceId 是否合法：16 或 32 位 hex（大小写均可）。
// 官方客户端生成的 conversationRequestId 是 32 位 hex（UUID 去横线），
// 入站透传值可能是任意形状（含横线/超长/非 hex），直接塞进 B3 头会破坏链路关联。
func validB3TraceID(s string) bool {
	if len(s) != 16 && len(s) != 32 {
		return false
	}
	for i := range s {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// 内容拦截降级重试（prompt.mode 三模式 + degradeGate）
//
// 误报处理哲学（与指纹脱敏同源）：内容拦截多为 system 来源的指纹误报，
// 换最小中性提示词即可绕开。custom 模式已用网关提示词替换，再撞审核说明
// 不是指纹误报（大概率是用户内容本身），不进入降级路径。
// -----------------------------------------------------------------------------

// degradedPrompt 降级提示词：误报处理用，刻意极简中性。
const degradedPrompt = "You are a helpful assistant. Respond in the user's language, follow the user's instructions, and be direct and concise."

// degradeGate 降级状态机：passthrough/append 模式下请求被内容策略拦截时，
// 切换到 degradedPrompt 直到次日 00:00 CST 重置。进程内存、重启清零。
type degradeGate struct {
	mu    sync.Mutex
	until time.Time
}

// degrade 全局降级门（进程级：任一请求触发，其后所有请求直达降级态）。
var degrade degradeGate

// Active 当前是否处于降级期。
func (g *degradeGate) Active() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Now().Before(g.until)
}

// Trigger 触发降级，直到次日 00:00 CST。已在降级期内则不续期
// （保持最早触发点的 00:00 重置语义）。
func (g *degradeGate) Trigger() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !time.Now().Before(g.until) {
		g.until = nextMidnightCST(time.Now())
	}
}

// nextMidnightCST 返回 now 之后最近的 Asia/Shanghai 00:00 时刻。
// 用固定 +08:00 偏移计算，避免依赖系统时区配置（容器/宿主机时区不确定）。
// 边界：23:59 → 次日 00:00；00:00 → 次日 00:00（刚过零点，下个零点是次日）。
//
// 来源：移植自 Sliverkiss/workbuddy2api 的 internal/server/degrade.go
// （MIT License, Copyright (c) 2026 Sliverkiss），逐字保留。
func nextMidnightCST(now time.Time) time.Time {
	cst := time.FixedZone("CST", 8*60*60)
	y, m, d := now.In(cst).Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, cst)
	for !midnight.After(now) {
		midnight = midnight.Add(24 * time.Hour)
	}
	return midnight
}

// effectivePromptText 返回 custom/append 模式实际使用的提示词文本。
func effectivePromptText() string {
	if cfg.PromptText != "" {
		return cfg.PromptText
	}
	return degradedPrompt
}

// promptReplaceSystem 用网关提示词替换 messages 中所有 system/developer 块
// （custom 模式与降级重试用）。保底：无 messages 字段时注入单条 system。
func promptReplaceSystem(obj map[string]any, systemPrompt string) {
	msgs, ok := obj["messages"].([]any)
	if !ok {
		obj["messages"] = []any{map[string]any{"role": "system", "content": systemPrompt}}
		return
	}
	kept := make([]any, 0, len(msgs)+1)
	kept = append(kept, map[string]any{"role": "system", "content": systemPrompt})
	for _, m := range msgs {
		mm, ok := m.(map[string]any)
		if !ok {
			kept = append(kept, m)
			continue
		}
		if roleOfMessage(mm) == "system" || roleOfMessage(mm) == "developer" {
			continue
		}
		kept = append(kept, m)
	}
	obj["messages"] = kept
}

// rewriteSystemTo 对序列化后的请求体执行 system 替换（降级重试用）。
func rewriteSystemTo(body []byte, systemPrompt string) []byte {
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	promptReplaceSystem(obj, systemPrompt)
	if out, err := json.Marshal(obj); err == nil {
		return out
	}
	return body
}

// promptAppendSystem 在 messages 的「开头连续 system/developer 块」之后插入
// 一条网关提示词（append 模式）。既有消息逐字不动——客户端项目规范与网关
// 提示词并用。
func promptAppendSystem(obj map[string]any, systemPrompt string) {
	msgs, ok := obj["messages"].([]any)
	if !ok {
		obj["messages"] = []any{map[string]any{"role": "system", "content": systemPrompt}}
		return
	}
	// 开头连续块 = 从 messages[0] 起向后 role 为 system/developer 的消息；
	// 遇第一条非 system/developer 消息即停。
	blockEnd := 0
	for blockEnd < len(msgs) {
		role := roleOfMessage(msgs[blockEnd])
		if role != "system" && role != "developer" {
			break
		}
		blockEnd++
	}
	gwMsg := map[string]any{"role": "system", "content": systemPrompt}
	out := make([]any, 0, len(msgs)+1)
	out = append(out, msgs[:blockEnd]...)
	out = append(out, gwMsg)
	out = append(out, msgs[blockEnd:]...)
	obj["messages"] = out
}

// sanitizeMessages 净化 messages 的 content / reasoning_content / tool_calls。
//
// content 与 tool_calls 各自独立判断：content 可以为 null（工具调用轮），
// 若在 content 缺失时直接跳过，这类消息的 tool_calls 完全不被净化。
func sanitizeMessages(obj map[string]any) int {
	messages, ok := obj["messages"].([]any)
	if !ok {
		return 0
	}
	modified := 0
	for _, m := range messages {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}
		// OpenAI 新版 developer 角色（GPT-5 系客户端/Codex）不被腾讯上游接受，
		// 会返回 11128 "Illegal API invocation from an unapproved channel"，
		// 统一归一化为 system（语义等价）。
		if roleOfMessage(msg) == "developer" {
			msg["role"] = "system"
		}
		// system 开头若是上游黑名单身份句（如 DeepSeek Harness），等义改写前缀；
		// 只处理偏移 0，正文逐字保留。
		if roleOfMessage(msg) == "system" {
			if content, changed := sanitizeSystemMessageContent(msg["content"]); changed {
				msg["content"] = content
				modified++
			}
		}
		if c, ok := msg["content"]; ok {
			if nc, changed := sanitizeContent(c); changed {
				msg["content"] = nc
				modified++
			}
		}
		// reasoning_content（思维链回填字段）实测同样携带指纹，与 content 同等净化。
		if rc, ok := msg["reasoning_content"].(string); ok {
			if s := sanitizeText(rc); s != rc {
				msg["reasoning_content"] = s
				modified++
			}
		}
		if tc, ok := msg["tool_calls"]; ok {
			if sanitizeToolCalls(tc) {
				modified++
			}
		}
	}
	return modified
}

// injectPromptCacheKey 注入上游 prompt_cache_key 字段（前缀缓存复用，费用优化）。
//
// 来源：移植自 Sliverkiss/workbuddy2api 的 internal/upstream/cache_key.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。逆向实测：同一段 8k token 前缀，
// 不带该字段时 prompt_cache_hit_tokens=0、credit≈0.34；带上后
// prompt_cache_hit_tokens=7808、credit≈0.02（费用降约 17 倍）。
//
// 键格式 `wbgw-<uid8>-<convHex>`：
//   - uid8 是账号 UID 前 8 字符，提供**跨账号硬隔离**——跨账号复用同一 cache key
//     会让上游命中他人前缀缓存、泄露对方对话内容，故 uid 是不可省略的隔离因子；
//   - convHex = sha256(uid + "|" + 会话标识) 前 16 字节的 hex，同账号同会话稳定、
//     不同会话不同。会话源为空时仍由 uid 单独哈希：跨账号绝不碰撞，但空会话不复用
//     （空会话 = 新会话语义，本就不该命中旧前缀）。
//
// 优先级：客户端已显式携带 prompt_cache_key → 原值保留，绝不覆盖（客户端自知
// 复用哪个键）；否则 body 里的 conversation_id / conversationId 优先于入站参数。
//
// 与本仓库的会话头族配套：conversationID 由 resolveConversationID 从请求体提取，
// 与 X-Conversation-ID 同源，保证「同一对话轮」在头与体两处口径一致。
// body 不可解析时原样返回（坏 body 不二次错误化，交由后续上游分类处理）。
func injectPromptCacheKey(body []byte, uid, conversationID string) []byte {
	if len(body) == 0 {
		return body
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	// 优先级 1：客户端已显式带 key → 绝不覆盖。
	if existing, ok := obj["prompt_cache_key"].(string); ok && existing != "" {
		return body
	}
	// 优先级 2：body 内的会话标识优先于入站参数。
	conv := conversationID
	if v, ok := obj["conversation_id"].(string); ok && strings.TrimSpace(v) != "" {
		conv = strings.TrimSpace(v)
	} else if v, ok := obj["conversationId"].(string); ok && strings.TrimSpace(v) != "" {
		conv = strings.TrimSpace(v)
	}
	obj["prompt_cache_key"] = buildPromptCacheKey(uid, conv)
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

// buildPromptCacheKey 生成 `wbgw-<uid8>-<convHex>` 格式的稳定 cache key。
// 见 injectPromptCacheKey 的隔离语义说明。
func buildPromptCacheKey(uid, conversation string) string {
	uid8 := uid
	if len(uid8) > 8 {
		uid8 = uid8[:8]
	}
	if uid8 == "" {
		uid8 = "-"
	}
	sum := sha256.Sum256([]byte(uid + "|" + conversation))
	return "wbgw-" + uid8 + "-" + hex.EncodeToString(sum[:16])
}

// -----------------------------------------------------------------------------
// 出站请求体的 tool_call ↔ tool 结果配对归一化
//
// 来源：逐字移植自 Sliverkiss/workbuddy2api 的 internal/upstream/tool_pairing.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。仅调用点适配本仓库的
// map[string]any 请求体形态（原实现直接作用于 []any 消息切片）。
//
// 背景：OpenAI 兼容协议要求带 tool_calls 的 assistant 消息，其每一个 tool_call id
// 都必须有对应的一条 role:tool 结果消息；反之 role:tool 消息也必须有对应的前置
// tool_call。缺任一侧，上游都会以 HTTP 400 拒绝整个请求。
//
// 工具执行失败时（参数非法、超时、工具不存在……）客户端会把 assistant 的 tool_calls
// 持久化进会话历史，却写不回结果消息。这条坏历史随后被每次请求原样重放——上游对之后
// 每一条用户消息都返回 400，整条会话报废。网关是最后一道防线：发出请求前剔除无法配对
// 的条目让会话自愈，宁可丢一轮工具上下文，也好过整条会话死亡。
// -----------------------------------------------------------------------------

// repackToolResultBlocks 把插在 assistant.tool_calls 与其 tool 结果之间的非 tool 消息
// 挪到整组之后，保证同一批 tool_call 的结果在 wire 上连续。
//
// 背景：Codex 的 image_resize_notice 特性会把 <image_resize_notice> 作为一条
// developer/system 消息插在 tool 输出后面。并行调用时它插在两份 tool 结果中间：
//
//	assistant tool_calls=[c00 c01]
//	tool c00
//	developer <image_resize_notice>   <- 插在中间
//	tool c01
//
// OpenAI 兼容协议要求 tool 结果紧跟 assistant，中间插任何消息都算配对断裂，上游判
// 11148（tool_call_sequence_broken）并顶死整条会话。这里只调顺序、不改内容：
//
//	assistant tool_calls=[c00 c01] | tool c00 | X | tool c01
//	→ assistant tool_calls=[c00 c01] | tool c00 | tool c01 | X
//
// 结果顺序保持不变（同批 tool_call 的原相对顺序 = 结果顺序），不引入新的顺序敏感
// 问题。无插入消息时零改动零分配（返回原 slice）。
func repackToolResultBlocks(messages []any) ([]any, bool) {
	if len(messages) < 3 {
		return messages, false
	}
	out := make([]any, 0, len(messages))
	changed := false
	i := 0
	for i < len(messages) {
		m, ok := messages[i].(map[string]any)
		if !ok || m["role"] != "assistant" {
			out = append(out, messages[i])
			i++
			continue
		}
		tcs, hasCalls := m["tool_calls"].([]any)
		if !hasCalls || len(tcs) == 0 {
			out = append(out, messages[i])
			i++
			continue
		}
		want := map[string]bool{}
		for _, tci := range tcs {
			if tc, ok := tci.(map[string]any); ok {
				if id, _ := tc["id"].(string); id != "" {
					want[id] = true
				}
			}
		}
		// 收集紧随其后（允许被其他消息打断）的同批 tool 结果，按原相对顺序。
		out = append(out, messages[i])
		i++
		var results []any
		var between []any
		sawNonTool := false
		for i < len(messages) {
			mm, ok := messages[i].(map[string]any)
			if !ok {
				break
			}
			role, _ := mm["role"].(string)
			if role == "tool" {
				id, _ := mm["tool_call_id"].(string)
				if !want[id] {
					break
				}
				results = append(results, messages[i])
				if sawNonTool {
					changed = true
				}
				i++
				continue
			}
			if len(results) == 0 {
				break // assistant 后没有结果：交由 cleanupOrphanToolCalls 处理
			}
			// 下一组 assistant.tool_calls 是新的组头，绝不能当插入物吞掉：一旦被收进
			// between，它永远不再被外层循环当作组头处理，它自己那批结果也就永远得不
			// 到重排。必须 break 交还外层循环。
			if role == "assistant" {
				if next, _ := mm["tool_calls"].([]any); len(next) > 0 {
					break
				}
			}
			// 同批结果尚未收齐时，中间消息视为插入物，暂存待后移。
			between = append(between, messages[i])
			sawNonTool = true
			i++
		}
		out = append(out, results...)
		out = append(out, between...)
	}
	if !changed {
		return messages, false
	}
	return out, true
}

// cleanupOrphanToolCalls 剔除无法配对的 tool_call 与 tool 结果（所有模型一律执行，
// 独立于指纹脱敏开关）。
//
//   - 收集全线 role:tool 消息的 tool_call_id（结果集）与 assistant.tool_calls[].id（调用集）；
//   - 一批 assistant.tool_calls 按 keepCalls 对称裁剪：只留有结果配对的调用（部分保留
//     不会留下无结果的 tool_call），过滤后为空才删掉整个 tool_calls 键；
//   - role:tool 只在对应 tool_call 被保留时才保留，否则删除整条消息；
//   - 无任何工具流量 → 原 slice 原样返回，changed=false（零分配零改动）。
//
// 这是「让请求通过」的安全网：只要存在合法配对就整段保留这些字段，绝不吞掉正确配对。
// 返回清理后的 slice（无改动时等于原 slice，勿依赖其是否新分配）及是否发生删除。
func cleanupOrphanToolCalls(messages []any) ([]any, bool) {
	if len(messages) == 0 {
		return messages, false
	}
	callIDs := map[string]bool{}
	resultIDs := map[string]bool{}
	hasTraffic := false
	for _, m := range messages {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}
		switch msg["role"] {
		case "tool":
			if id, ok := msg["tool_call_id"].(string); ok && id != "" {
				resultIDs[id] = true
				hasTraffic = true
			}
		case "assistant":
			if tcs, ok := msg["tool_calls"].([]any); ok {
				for _, tci := range tcs {
					tc, ok := tci.(map[string]any)
					if !ok {
						continue
					}
					if id, ok := tc["id"].(string); ok && id != "" {
						callIDs[id] = true
						hasTraffic = true
					}
				}
			}
		}
	}
	if !hasTraffic {
		return messages, false
	}
	// keepCalls：调用 id 是否双侧齐全（调用存在且结果存在）。重复 id 与乱序均按集合处理。
	keepCalls := map[string]bool{}
	for id := range callIDs {
		if resultIDs[id] {
			keepCalls[id] = true
		}
	}
	changed := false
	// 1) assistant.tool_calls：按 keepCalls 对称裁剪——只留有结果的调用，过滤后为空则删键。
	//
	// 不能「批内每个 id 都齐才整批保留，否则删掉整个 tool_calls 键」：那会留下无主结果——
	// 批 [c1,c2] 只回了 c1 时，调用侧整批被删，而 tool{c1} 仍按 id 命中 keepCalls 得以保留，
	// 出站载荷于是变成「无 tool_calls 的 assistant + 孤儿 tool」，上游判 11148
	// （tool calls and tool results do not match）并顶死整条会话。
	// 两侧必须共用同一份 keepCalls 按 id 对称裁剪，任何输入都不会再产生半截配对。
	for _, m := range messages {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}
		if role, _ := msg["role"].(string); role != "assistant" {
			continue
		}
		tcs, ok := msg["tool_calls"].([]any)
		if !ok || len(tcs) == 0 {
			continue
		}
		keptCalls := make([]any, 0, len(tcs))
		for _, tci := range tcs {
			tc, ok := tci.(map[string]any)
			if !ok {
				continue
			}
			if id, _ := tc["id"].(string); keepCalls[id] {
				keptCalls = append(keptCalls, tc)
			}
		}
		if len(keptCalls) == len(tcs) {
			continue // 整批齐全：零改动
		}
		changed = true
		if len(keptCalls) == 0 {
			delete(msg, "tool_calls")
			continue
		}
		msg["tool_calls"] = keptCalls
	}
	// 2) role:tool 结果：只有对应 tool_call 被保留才保留；孤儿结果整条删除。
	kept := make([]any, 0, len(messages))
	for _, m := range messages {
		msg, ok := m.(map[string]any)
		if !ok {
			kept = append(kept, m)
			continue
		}
		if role, _ := msg["role"].(string); role == "tool" {
			id, _ := msg["tool_call_id"].(string)
			if !keepCalls[id] {
				changed = true
				continue
			}
		}
		kept = append(kept, m)
	}
	if !changed {
		return messages, false
	}
	return kept, true
}

// normalizeToolPairing 对请求体的 messages 依次执行「重排 → 清理」两步 tool 配对归一化。
//
// 顺序不可颠倒：先 repack 把夹在结果中间的非 tool 消息挪后，再 cleanup 删孤儿，
// 两侧同口径（repack 判定「同批结果」与 cleanup 的 keepCalls 都按 tool_call id 匹配）。
// 任一步有改动即回写 obj——不能只在最后一步改动时回写，否则 repack 单独生效的结果
// 会被原 slice 覆盖丢失。
func normalizeToolPairing(obj map[string]any) {
	msgs, ok := obj["messages"].([]any)
	if !ok || len(msgs) == 0 {
		return
	}
	msgs, repacked := repackToolResultBlocks(msgs)
	msgs, cleaned := cleanupOrphanToolCalls(msgs)
	if repacked || cleaned {
		obj["messages"] = msgs
	}
}

// ensureLeadingSystemMessage 保证 messages 的首条消息符合腾讯上游的会话结构校验。
//
// 上游要求首条消息为 system prompt，否则返回 HTTP 400
// {"code":11128,"msg":"first message is not system prompt"}。实测国内站对 user 开头较宽容，
// 但国际站（workbuddy.ai）严格校验；账号池混挂时表现为约 50% 请求随机失败。部分非 harness
// 客户端在续写或仅回传工具结果时还会以 assistant / tool 作为首条消息。此处统一归一化为：
//  1. 首条已是 system：原样透传；
//  2. 首条是 developer（OpenAI 新版 system 别名）：重命名为 system；
//  3. 后续存在 system/developer：提升到首位（developer 归一化为 system），其余保持原序；
//  4. 其余情况（user / assistant / tool 开头且无 system）：在最前注入一条保底 system。
//
// 返回值只表示本次是否注入了保底 system，用于不记录提示词正文的审计日志。
func ensureLeadingSystemMessage(obj map[string]any) bool {
	messages, ok := obj["messages"].([]any)
	if !ok || len(messages) == 0 {
		obj["messages"] = []any{map[string]any{"role": "system", "content": configuredFallbackSystemPrompt()}}
		return true
	}

	switch roleOfMessage(messages[0]) {
	case "system":
		return false
	case "developer":
		if msg, ok := messages[0].(map[string]any); ok {
			msg["role"] = "system"
		}
		return false
	}

	// 后续存在 system/developer：提升到首位，其余保持原序
	for i := 1; i < len(messages); i++ {
		switch roleOfMessage(messages[i]) {
		case "system", "developer":
			if msg, ok := messages[i].(map[string]any); ok {
				msg["role"] = "system"
			}
			reordered := make([]any, 0, len(messages))
			reordered = append(reordered, messages[i])
			reordered = append(reordered, messages[:i]...)
			reordered = append(reordered, messages[i+1:]...)
			obj["messages"] = reordered
			return false
		}
	}

	// 无任何 system：在最前注入保底 system（兼容国内站/国际站）
	injected := make([]any, 0, len(messages)+1)
	injected = append(injected, map[string]any{"role": "system", "content": configuredFallbackSystemPrompt()})
	injected = append(injected, messages...)
	obj["messages"] = injected
	return true
}

// roleOfMessage 读取消息的 role 字段并归一化为小写去空格；非法结构返回空串。
func roleOfMessage(m any) string {
	msg, ok := m.(map[string]any)
	if !ok {
		return ""
	}
	role, _ := msg["role"].(string)
	return strings.ToLower(strings.TrimSpace(role))
}

// -----------------------------------------------------------------------------
// 出站请求体指纹脱敏
//
// 来源：逐字移植自 Sliverkiss/workbuddy2api 的 internal/upstream/sanitize.go
// （MIT License, Copyright (c) 2026 Sliverkiss），仅按本仓库的调用点做了
// 函数签名适配（sanitizeMessages 接收 map 而非 []any）。许可全文见 README
// 「代码来源与许可」章节。
//
// 背景：客户端（Claude Code / Codex 类 CLI）会在 system prompt 注入若干固定模板句，
// 上游内容审核按**逐字精确匹配**拦截（非语义审核），一字改动即可绕过。
//
// 策略分三层：
//   - 改写层：承载语义的模板句做最小改写（换一词），语义不变
//   - 剥离层：header 键值段整段删除（纯噪音，无语义损失）
//   - 兜底层：残留的裸键名做最小缩写，破坏逐字匹配但保留可读性
// -----------------------------------------------------------------------------

// sanitizeFeatures 特征预检词表：任一命中才进入净化。
// 普通请求全不中 → 原样返回，零分配。
var sanitizeFeatures = []string{
	"x-anthropic-billing-header",                      // header 键值段键名
	"cc_entrypoint=",                                  // 尾随裸键值（截断前缀即可命中）
	"You are Claude Code",                             // 身份句（截断前缀即可命中）
	"Main branch (",                                   // 注入指令句（截断前缀即可命中）
	"You are a coding agent running in the Codex CLI", // Codex instructions 首段
	"github.com/anthropics/",                          // 反馈句里的 Anthropic 仓库链接
	"11128",                                           // 上游反探测：裸数字错误码
}

// sanitizeHdrRe 剥离层：header 键名即触发（与值无关），整段删除。
var sanitizeHdrRe = regexp.MustCompile(`(?i)x-anthropic-billing-header:[^;\n]*;?\s*`)

// sanitizeBareHdrRe 兜底层：裸键名（无冒号无值）同样是指纹——assistant 消息里
// 反引号引用裸键名即触发 11128，而剥离层要求冒号、对裸串无效。
// 键值形态被整段删除后，残留的裸键名做最小缩写（header→hdr）：破坏逐字匹配、
// 语义不变、保留可读性。大小写不敏感，覆盖 X-Anthropic-... 变体。
//
// 该正则不要求冒号，是 sanitizeHdrRe 的超集——两者替换语义不同
// （整段删除 vs 最小缩写），不可合并为一个正则。
var sanitizeBareHdrRe = regexp.MustCompile(`(?i)x-anthropic-billing-header`)

// sanitizeKvRe 剥离层：尾随裸键值（cc_xxx=...;）循环清理。
var sanitizeKvRe = regexp.MustCompile(`(?i)\bcc_[a-z0-9_]+=[^;\n]*;?\s*`)

// sanitizeRewrites 改写层：全模板句逐字替换（每句只改一个词，语义不变）。
//
// 身份句的匹配串**不带结尾标点**（只到 "…for Claude" 为止）：
// CLI 版这句以句号收尾，桌面版（claude-desktop-3p / Agent SDK）以逗号接后继内容。
// 带句号的整句只匹配前者，桌面版会漏网、指纹原样发上游 → 400 code=11128。
// 去掉结尾标点后两种形态一并覆盖（替换串同样不带标点，让原有标点原样保留）。
var sanitizeRewrites = [][2]string{
	{
		"You are Claude Code, Anthropic's official CLI for Claude",
		"You are Claude Code, Anthropic's official CLI tool for Claude",
	},
	{
		"Default branch (you will usually use this for PRs)",
		"Default branch (you will usually use this for PRs)",
	},
	{
		"You are a coding agent running in the Codex CLI, a terminal-based coding assistant.",
		"You are a coding agent running in the Codex CLI tool, a terminal-based coding assistant.",
	},
	{
		// 反馈句：整句带 Anthropic 仓库链接，上游按整句拦截（只留链接或只留半边均不拦，
		// 实测需整句同时出现）。give→provide 一词之差即可绕过，语义不变。
		"To give feedback, users should report the issue at https://github.com/anthropics/claude-code/issues",
		"To provide feedback, users should report the issue at https://github.com/anthropics/claude-code/issues",
	},
	{
		// 上游反探测：只要请求体里出现裸数字 11128 就整单拦截（与该数字的上下文无关）。
		// 11128 正是本类拦截自身的错误码，上游据此识别"在讨论/回显其内部错误码"的请求。
		// 代价：用户对话中任何 11128 都会被改写——但这串数字出现在请求里本身就是拦截条件，
		// 不改写必然失败。插入连字符保留可读性与指代。
		"11128",
		"11-128",
	},
}

// sanitizeText 单段文本净化：预检不中 → 返回原串（零分配）。
func sanitizeText(text string) string {
	if !hasFingerprint(text) {
		return text
	}
	for _, rw := range sanitizeRewrites {
		text = strings.ReplaceAll(text, rw[0], rw[1])
	}
	if sanitizeHdrRe.MatchString(text) {
		text = sanitizeHdrRe.ReplaceAllString(text, "")
	}
	if strings.Contains(text, "cc_") {
		prev := ""
		for prev != text { // 清尾随裸 kv（cc_version=...; cc_entrypoint=...;）
			prev = text
			text = sanitizeKvRe.ReplaceAllString(text, "")
		}
	}
	// 兜底：键值形态已在上面整段删除，这里只剩裸键名（引用/示例文本形态）。
	text = sanitizeBareHdrRe.ReplaceAllString(text, "x-anthropic-billing-hdr")
	return strings.TrimSpace(text)
}

// hasFingerprint 特征预检：先走 strings.Contains 快速路径（零分配）；
// header 键名有大小写变体且可能以裸键名形态出现（无冒号），
// Contains 大小写敏感、sanitizeHdrRe 要求冒号——两者都会漏掉「混合大小写 + 裸键名」，
// 必须再用不要求冒号的正则兜底，否则整条净化被跳过。
func hasFingerprint(text string) bool {
	for _, f := range sanitizeFeatures {
		if strings.Contains(text, f) {
			return true
		}
	}
	return sanitizeBareHdrRe.MatchString(text)
}

// sanitizeContent 兼容字符串与多模态数组；只动 text part，image 等 part 不动。
// 返回净化后的值及是否发生变化。
func sanitizeContent(v any) (any, bool) {
	switch c := v.(type) {
	case string:
		s := sanitizeText(c)
		return s, s != c
	case []any:
		changed := false
		for _, p := range c {
			m, ok := p.(map[string]any)
			if !ok {
				continue
			}
			text, ok := m["text"].(string)
			if !ok {
				continue
			}
			if s := sanitizeText(text); s != text {
				m["text"] = s
				changed = true
			}
		}
		return c, changed
	}
	return v, false
}

// sanitizeToolCalls 净化 assistant.tool_calls[].function.arguments。
//
// arguments 是**字符串化的 JSON**（不是对象），因此按文本走 sanitizeText 即可。
// 这块长期是盲区：工具调用消息的 content 通常是 null，若在 content 缺失时直接跳过，
// 整条消息连 tool_calls 一起漏过——于是历史里任何写进工具参数的被拦字符串
// （文件名、命令、写入内容）都会原样漏出。
func sanitizeToolCalls(v any) bool {
	callList, ok := v.([]any)
	if !ok {
		return false
	}
	changed := false
	for _, c := range callList {
		call, ok := c.(map[string]any)
		if !ok {
			continue
		}
		fn, ok := call["function"].(map[string]any)
		if !ok {
			continue
		}
		args, ok := fn["arguments"].(string)
		if !ok {
			continue
		}
		if s := sanitizeText(args); s != args {
			fn["arguments"] = s
			changed = true
		}
	}
	return changed
}

// backendHeaders 设置 CodeBuddy 上游专用指纹与鉴权 Header（按站点 Profile 生成）。
//
// 会话/轮次/消息标识由 setConversationHeaders 统一生成，语义对齐官方 WorkBuddy
// 客户端：X-Conversation-ID 恒发，请求与消息 ID 按逻辑请求复用；不再额外发送
// B3 或 X-Root-Request-ID，保持与官方客户端一致的头部形态。
func backendHeaders(req *http.Request, sa *StoredAuth, prof *upstreamProfile) {
	commonHeaders(req, prof)
	reqID := uuid.New().String()
	req.Header.Set("X-Request-ID", reqID)
	req.Header.Set("X-Trace-ID", reqID)
	setConversationHeaders(req)
	req.Header.Set("X-Client-ID", prof.ClientID)
	req.Header.Set("X-Client-Version", prof.ClientVer)

	if sa != nil && sa.Auth.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+sa.Auth.AccessToken)
	} else {
		req.Header.Set("X-No-Authorization", "1")
	}
	if sa != nil && sa.Account.UID != "" {
		req.Header.Set("X-User-Id", sa.Account.UID)
	} else {
		req.Header.Set("X-No-User-Id", "1")
	}
	if sa != nil && sa.Account.EnterpriseID != "" {
		req.Header.Set("X-Enterprise-Id", sa.Account.EnterpriseID)
	} else {
		req.Header.Set("X-No-Enterprise-Id", "1")
	}
	if sa != nil && sa.Auth.RefreshToken != "" {
		req.Header.Set("X-Refresh-Token", sa.Auth.RefreshToken)
	}
	if sa != nil && sa.Auth.Domain != "" {
		req.Header.Set("X-Domain", sa.Auth.Domain)
	} else {
		req.Header.Set("X-No-Department-Info", "1")
	}
	req.Header.Set("X-Product", prof.Product)
}

// commonHeaders 按站点 Profile 设置通用伪装 Header（Origin/Referer/UA 因站点而异）。
func commonHeaders(req *http.Request, prof *upstreamProfile) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Origin", prof.Origin)
	req.Header.Set("Referer", prof.Origin+"/")
	req.Header.Set("User-Agent", prof.ClientUA)
}

func doJSON(client *http.Client, method, fullURL string, headers func(*http.Request), body io.Reader) (json.RawMessage, int, error) {
	// 控制类请求（令牌刷新、额度查询、模型目录等）必须有明确上限，避免卡死；
	// 上游对话流不经过这里，因此不会误伤长流。
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	return doJSONContext(ctx, client, method, fullURL, headers, body)
}

func doJSONContext(ctx context.Context, client *http.Client, method, fullURL string, headers func(*http.Request), body io.Reader) (json.RawMessage, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return nil, 0, err
	}
	if headers != nil {
		headers(req)
	} else {
		commonHeaders(req, &profileCN) // 兜底默认（当前所有调用方均显式传入站点 Profile）
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, resp.StatusCode, fmt.Errorf("http_error: upstream %d: %s", resp.StatusCode, string(raw))
	}
	var env apiEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("解析上游 JSON 失败: %w", err)
	}
	if env.Code != 0 {
		return nil, resp.StatusCode, fmt.Errorf("code=%d msg=%s", env.Code, env.Msg)
	}
	return env.Data, resp.StatusCode, nil
}

// -----------------------------------------------------------------------------
// 流式聚合与格式清理
// -----------------------------------------------------------------------------

// mergedToolCall 累积合并上游按 index 分片下发的工具调用增量。
type mergedToolCall struct {
	ID   string
	Type string
	Name string
	Args strings.Builder
}

// applyToolCallDelta 将上游 tool_calls 增量按 index 归并进 map；order 记录首次出现的
// index 顺序，保证最终输出顺序稳定。Chat Completions 聚合与 Responses 流式共用。
func applyToolCallDelta(toolCalls map[int]*mergedToolCall, order *[]int, tcs []any) {
	for _, tcAny := range tcs {
		tc, ok := tcAny.(map[string]any)
		if !ok {
			continue
		}
		idx := 0
		if v, ok := tc["index"].(float64); ok {
			idx = int(v)
		}
		st, exists := toolCalls[idx]
		if !exists {
			st = &mergedToolCall{Type: "function"}
			toolCalls[idx] = st
			*order = append(*order, idx)
		}
		if id, ok := tc["id"].(string); ok && id != "" {
			st.ID = id
		}
		if t, ok := tc["type"].(string); ok && t != "" {
			st.Type = t
		}
		if fn, ok := tc["function"].(map[string]any); ok {
			if n, ok := fn["name"].(string); ok && n != "" {
				st.Name = n
			}
			if a, ok := fn["arguments"].(string); ok && a != "" {
				st.Args.WriteString(a)
			}
		}
	}
}

// -----------------------------------------------------------------------------
// 工具调用的残缺参数检测
//
// 来源：逐字移植自 Sliverkiss/workbuddy2api 的 internal/upstream/truncation.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。
//
// 背景：SSE 流被截断（连接中断 / finish_reason==length）时，工具调用的 arguments
// 会只剩半截 JSON。此时网关若把脏参数原样交给客户端，客户端解析会报非法 JSON 并
// 卡死会话。处置是丢弃残缺调用，而非补成 {} 伪造合法外观。
//
// 关键区分：只把「非空但无法解析」视为截断。空串是合法的无参数工具；能解析但类型
// 不对（标量 / 数组）属于模型输出错误，交给客户端 schema 校验回传即可，不在此判定。
// -----------------------------------------------------------------------------

// isTruncatedArguments 判定工具参数字符串是否因分片丢失而残缺（区别于「该工具本就无参数」）。
//   - 空串 / 纯空白 → false（合法无参工具）；
//   - 非空但 JSON 解析失败 → true（截断）；
//   - 能解析（含 null/标量/数组等任何合法 JSON）→ false。
func isTruncatedArguments(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return false
	}
	var v any
	return json.Unmarshal([]byte(trimmed), &v) != nil
}

// dropTruncatedToolCalls 过滤出 arguments 完整的 tool_call（返回新 slice）。
// 只依据 isTruncatedArguments 判定，不改动任何保留的调用（正例零改动）。
func dropTruncatedToolCalls(calls []map[string]any) []map[string]any {
	kept := make([]map[string]any, 0, len(calls))
	for _, call := range calls {
		fn, _ := call["function"].(map[string]any)
		if fn == nil {
			kept = append(kept, call)
			continue
		}
		args, _ := fn["arguments"].(string)
		if isTruncatedArguments(args) {
			continue
		}
		kept = append(kept, call)
	}
	return kept
}

// logAggregateFailure 记录非流式聚合失败。没有 finish_reason 的干净结束单独说明，
// 避免和网络中断混在同一句「聚合失败」里。
func logAggregateFailure(r *http.Request, reqID uint64, err error) {
	if errors.Is(err, errStreamClosedWithoutFinish) {
		log.Printf("[异常] traceId=%s requestId=%d 发生阶段=聚合上游流 结果=拒绝当成成功 原因=上游连接正常结束但没有 finish_reason 返回状态码=500 业务影响=不把残缺输出伪装成完整回复 是否已处理=是",
			debugTraceID(r), reqID)
		return
	}
	log.Printf("[#%d] 聚合响应失败: %v", reqID, err)
}

// noteFinishReason 记下分片里的真实结束原因。
// 空串必须忽略：上游会在每一个中间分片上都带 finish_reason:""，那不是结束。
func noteFinishReason(current string, chunk map[string]any) string {
	if chunk == nil {
		return current
	}
	choices, _ := chunk["choices"].([]any)
	for _, c := range choices {
		choice, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if v, ok := choice["finish_reason"].(string); ok && v != "" {
			return v
		}
	}
	return current
}

func aggregateCompletion(r io.Reader, model string) ([]byte, error) {
	var content, reasoning, role, respModel, respID, finish string
	var created int64
	var usage map[string]any
	toolCalls := map[int]*mergedToolCall{}
	var toolOrder []int
	// sawDone 记录是否收到 data: [DONE] 终止帧。EOF 收尾但未见 [DONE] 说明上游连接
	// 中断，此时残留的 tool_call 分片是半截 JSON，必须丢弃（见下方 dropTruncatedToolCalls）。
	sawDone := false

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		data := stripDataPrefix(scanner.Text())
		if data == "" {
			continue
		}
		if data == "[DONE]" {
			sawDone = true
			continue
		}
		var chunk map[string]any
		if json.Unmarshal([]byte(data), &chunk) != nil {
			continue
		}
		observeFirstToken(r, chunk)
		if v, ok := chunk["id"].(string); ok && v != "" {
			respID = v
		}
		if v, ok := chunk["model"].(string); ok && v != "" {
			respModel = v
		}
		if v, ok := chunk["created"].(float64); ok {
			created = int64(v)
		}
		if v, ok := chunk["usage"].(map[string]any); ok {
			usage = v
		}
		choices, _ := chunk["choices"].([]any)
		for _, c := range choices {
			choice, _ := c.(map[string]any)
			if delta, ok := choice["delta"].(map[string]any); ok {
				if v, ok := delta["role"].(string); ok && v != "" {
					role = v
				}
				if v, ok := delta["content"].(string); ok {
					content += v
				}
				if v, ok := delta["reasoning_content"].(string); ok {
					reasoning += v
				}
				if tcs, ok := delta["tool_calls"].([]any); ok {
					applyToolCallDelta(toolCalls, &toolOrder, tcs)
				}
			}
			finish = noteFinishReason(finish, map[string]any{"choices": []any{choice}})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取上游流失败: %w", err)
	}
	if finish == "" {
		return nil, errStreamClosedWithoutFinish
	}
	completeTokenStream(r, true)

	message := map[string]any{"role": ifEmpty(role, "assistant"), "content": content}
	if reasoning != "" {
		message["reasoning_content"] = reasoning
	}
	if len(toolOrder) > 0 {
		calls := make([]map[string]any, 0, len(toolOrder))
		for _, idx := range toolOrder {
			st := toolCalls[idx]
			id := st.ID
			if id == "" {
				id = fmt.Sprintf("call_%d_%d", time.Now().UnixNano(), idx)
			}
			calls = append(calls, map[string]any{
				"id":    id,
				"type":  ifEmpty(st.Type, "function"),
				"index": idx,
				"function": map[string]any{
					"name":      st.Name,
					"arguments": st.Args.String(),
				},
			})
		}
		// 流被截断时 tool_call 的 arguments 是残缺 JSON（解析失败），不把脏参数交给
		// 客户端——残留分片会被客户端解析成非法 JSON 卡死会话。截断的两个来源：
		//   - finish_reason=="length"（模型因 max_tokens 提前中止）；
		//   - 上游连接中断（EOF 收尾但未发 data: [DONE]，sawDone=false）。
		// 完整参数原样保留（正例零改动）；空参数（无参工具）不是截断，同样保留。
		if finish == "length" || !sawDone {
			calls = dropTruncatedToolCalls(calls)
		}
		if len(calls) > 0 {
			message["tool_calls"] = calls
		}
	}
	if created == 0 {
		created = time.Now().Unix()
	}
	result := map[string]any{
		"id":      ifEmpty(respID, fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())),
		"object":  "chat.completion",
		"created": created,
		"model":   ifEmpty(respModel, model),
		"choices": []map[string]any{{
			"index":         0,
			"message":       message,
			"finish_reason": finish,
		}},
	}
	if usage != nil {
		result["usage"] = usage
	}
	return json.Marshal(result)
}

// cleanChunkJSON 清洗单个上游 chat 分片，使其符合 OpenAI 流式分片规范后再透传。
//
// 上游是「类 OpenAI」实现，分片里带有若干偏离规范的噪声。普通 OpenAI 客户端能忍，
// 但部分客户端/协议翻译层会据此误判流状态，导致工具不执行：
//
//  1. finish_reason:"" → null（本 bug 的直接原因）。规范要求中间分片为 null、只有终止分片
//     给出真实原因；上游却在整条流的每一个分片上都下发 finish_reason:""。
//     翻译层会取流中「第一个非 null 的 finish_reason」作为最终 stop_reason，
//     于是首个分片就把 stop_reason 锁成 end_turn，真实终止分片的 "tool_calls" 不再被采纳。
//     Claude Code 只在 stop_reason=tool_use 时才执行工具，表现为「网关日志成功但客户端没结果」。
//  2. 旧版 function_call 空壳：上游在含工具调用的终止片追加
//     {"function_call":{"name":"","arguments":""}}，属同类非规范噪声，一并清除。
//  3. delta 中的空值字段（content:""、tool_calls:[] 等）。

// -----------------------------------------------------------------------------
// 流中空闲监控
//
// 来源：移植自 Sliverkiss/workbuddy2api 的 internal/upstream/idle.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。
//
// 背景：本仓库对上游只设了整体超时（180s）。若上游建立连接后吐了几个 token 就
// 静默挂住，该账号的 acc.lock 会一直被占——串行队列全堵，而整体超时要等满才释放。
// 活跃吐数据续命不掐，静默超过阈值才断流（释放账号锁）。
// -----------------------------------------------------------------------------

// idleMonitoringBody 包在聊天 SSE body 外层：每次读到底层数据（n>0）就刷新
// lastRead；后台 goroutine 周期检查，静默超过 idle 就 cancel 请求 context，
// 中断阻塞中的 Read。
type idleMonitoringBody struct {
	rc       io.ReadCloser
	mu       sync.Mutex
	lastRead time.Time
	stopOnce sync.Once
	stopCh   chan struct{}
	cancel   context.CancelFunc
}

func (b *idleMonitoringBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.mu.Lock()
		b.lastRead = time.Now()
		b.mu.Unlock()
	}
	return n, err
}

// Close 停掉后台 goroutine、取消请求 context、关闭底流，保证无泄漏。
func (b *idleMonitoringBody) Close() error {
	b.stopOnce.Do(func() { close(b.stopCh) })
	b.cancel()
	return b.rc.Close()
}

func (b *idleMonitoringBody) idleFor() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Since(b.lastRead)
}

// monitorBody 包装上游 body：每次读到数据续命，静默超过 idle 就 cancel 请求 context
// 中断阻塞中的 Read。idle<=0 时不启监控 goroutine（禁用空闲掐流），但 Close 仍
// 负责 cancel——调用方把 cancel 所有权完全交给返回的 body，无需关心分支。
func monitorBody(rc io.ReadCloser, idle time.Duration, cancel context.CancelFunc) io.ReadCloser {
	b := &idleMonitoringBody{
		rc:       rc,
		lastRead: time.Now(),
		stopCh:   make(chan struct{}),
		cancel:   cancel,
	}
	if idle <= 0 {
		return b
	}
	go func() {
		t := time.NewTicker(idleTick(idle))
		defer t.Stop()
		for {
			select {
			case <-b.stopCh:
				return
			case <-t.C:
				if b.idleFor() > idle {
					cancel()
					return
				}
			}
		}
	}()
	return b
}

// idleTick 返回监控周期：idle/4，钳在 [10ms, 1s]。小 idle 也能快速发现，大值避免空转。
func idleTick(idle time.Duration) time.Duration {
	d := idle / 4
	if d > time.Second {
		d = time.Second
	}
	if d < 10*time.Millisecond {
		d = 10 * time.Millisecond
	}
	return d
}

// -----------------------------------------------------------------------------
// reasoning_effort 档位降级
//
// 来源：移植自 Sliverkiss/workbuddy2api 的 internal/upstream/payload.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。
//
// 背景：客户端可能传模型不支持的档位（如对只支持到 high 的模型传 "max"），
// 上游会 400。降级到 ≤请求档位的最高支持档，语义偏离最小。
// -----------------------------------------------------------------------------

// effortRank 档位从低到高。
var effortRank = map[string]int{"off": 0, "minimal": 1, "low": 2, "medium": 3, "high": 4, "xhigh": 5, "max": 6}

// normalizeReasoningEffort 按模型 supportedEfforts 降级 reasoning_effort（snake/camel 双字段兼容）。
//   - 请求档位模型支持 → 原样透传
//   - 请求档位不支持 → 改为 ≤请求档位的最高支持档（降级）
//   - 支持档全部高于请求档 → 取最低支持档（偏离最小）
//   - 未知模型/未知档位/未携带字段/模型未缓存 → 一律透传
func normalizeReasoningEffort(obj map[string]any, efforts map[string][]string) {
	if len(efforts) == 0 {
		return
	}
	model, _ := obj["model"].(string)
	if model == "" {
		return
	}
	supported, ok := efforts[model]
	if !ok || len(supported) == 0 {
		return
	}
	key := ""
	if _, present := obj["reasoning_effort"]; present {
		key = "reasoning_effort"
	} else if _, present := obj["reasoningEffort"]; present {
		key = "reasoningEffort"
	} else {
		return
	}
	reqStr, ok := obj[key].(string)
	if !ok {
		return
	}
	reqStr = strings.TrimSpace(strings.ToLower(reqStr))
	reqIdx, known := effortRank[reqStr]
	if !known {
		return
	}
	// 已知档位：先把值归一化写回。客户端可能带空白/大小写变体（"  HIGH  "），
	// 上游按字面比较会判非法参数；归一化后的值才是档位表里的规范形式。
	if orig, ok := obj[key].(string); ok && orig != reqStr {
		obj[key] = reqStr
	}
	// off/none 的语义是「关闭思考」，rank 为 0（最低）。模型不支持该档位时
	// **不得上抬**到任何开启档位——那会把客户端明确的关闭意图反转成开启，
	// 比透传一个上游可能拒绝的值更糟。原值透传，由上游判定。
	if reqIdx == 0 {
		return
	}
	// 在 ≤请求档位的支持档里选最高档；命中且与请求不同才改写。
	best, bestIdx := "", -1
	for _, s := range supported {
		idx, k := effortRank[strings.TrimSpace(strings.ToLower(s))]
		if k && idx <= reqIdx && idx > bestIdx {
			best, bestIdx = s, idx
		}
	}
	if best != "" {
		if obj[key] != best {
			obj[key] = best
			log.Printf("[Effort] reasoning_effort 降级 model=%s %s -> %s", model, reqStr, best)
		}
		return
	}
	// 支持档全部高于请求档：取最低支持档。
	lowest, lowestIdx := "", 1<<30
	for _, s := range supported {
		idx, k := effortRank[strings.TrimSpace(strings.ToLower(s))]
		if k && idx < lowestIdx {
			lowest, lowestIdx = s, idx
		}
	}
	if lowest != "" {
		obj[key] = lowest
		log.Printf("[Effort] reasoning_effort 上抬 model=%s %s -> %s", model, reqStr, lowest)
	}
}

// isDeepSeekModel 模型名以 deepseek 为前缀（不区分大小写）。
// 覆盖 deepseek-v4.1-flash / deepseek-v4-pro / deepseek-r1 等变体；
// 前缀匹配对齐官方 thinkingFormat:"deepseek" 的判定口径，避免漏注。
func isDeepSeekModel(model string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "deepseek")
}

// backfillReasoningContent DeepSeek 多轮一致性：历史 assistant 消息带 reasoning 痕迹时，
// 上游要求后续请求所有 assistant 消息都带 reasoning_content 字段（string，可为空串）
// ——即 requiresReasoningContentOnAssistantMessages（官方客户端 matches 规则）。
//
// 来源：逐字移植自 Sliverkiss/workbuddy2api 的 internal/upstream/thinking.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。
//
// 规则（对齐官方客户端逻辑）：
//   - 会话内任一 assistant 消息带非空 reasoning（string）或已有 reasoning_content 字段
//     → 所有 assistant 消息确保有 reasoning_content（string）：
//   - reasoning 非空且无 reasoning_content → 复制 reasoning 值
//   - 已有 reasoning_content → 原样保留（不覆盖）
//   - 两者皆无 → 补空串 ""
//   - 任何 assistant 均无 reasoning 痕迹 → 零改动（不白白加字段）
//
// 仅 deepseek 模型生效（thinkingFormat:deepseek + requiresReasoningContent）。
func backfillReasoningContent(obj map[string]any) {
	model, _ := obj["model"].(string)
	if !isDeepSeekModel(model) {
		return
	}
	msgs, ok := obj["messages"].([]any)
	if !ok || len(msgs) == 0 {
		return
	}
	// 第一遍：检测是否有任何 reasoning 痕迹（非空 reasoning 或已有 reasoning_content）。
	hasTrace := false
	for _, mm := range msgs {
		msg, ok := mm.(map[string]any)
		if !ok {
			continue
		}
		if r, ok := msg["reasoning"].(string); ok && r != "" {
			hasTrace = true
			break
		}
		if _, ok := msg["reasoning_content"]; ok {
			hasTrace = true
			break
		}
	}
	if !hasTrace {
		return
	}
	// 第二遍：所有 assistant 消息补/复制 reasoning_content 字段。
	for _, mm := range msgs {
		msg, ok := mm.(map[string]any)
		if !ok {
			continue
		}
		role, _ := msg["role"].(string)
		if role != "assistant" {
			continue
		}
		if _, ok := msg["reasoning_content"]; ok {
			continue // 已有 → 不覆盖
		}
		if r, ok := msg["reasoning"].(string); ok {
			msg["reasoning_content"] = r
		} else {
			msg["reasoning_content"] = ""
		}
	}
}

// normalizeToolChoice 按上游 Go struct（string 类型）改写 OpenAI tool_choice。
//
// 来源：逐字移植自 Sliverkiss/workbuddy2api 的 internal/upstream/payload.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。
//
// 背景：上游把 tool_choice 定义为 string，而 OpenAI 官方 SDK 默认发对象形式
// （{"type":"function","function":{"name":"x"}}），直接透传会 400 code=11101。
//
//   - "none"            → 删 tool_choice + 删 tools/functions
//   - {"type":"none"}   → 同上
//   - {"type":"auto"/"required"} → 字符串 "auto"/"required"
//   - {"type":"function","function":{"name":"x"}} → 字符串 "x"
//   - 其他对象/非标量 → 删 tool_choice
func normalizeToolChoice(obj map[string]any) {
	suppress := func() {
		delete(obj, "tools")
		delete(obj, "functions")
	}
	tc, present := obj["tool_choice"]
	if !present {
		return
	}
	switch v := tc.(type) {
	case string:
		if strings.EqualFold(strings.TrimSpace(v), "none") {
			delete(obj, "tool_choice")
			suppress()
		}
	case map[string]any:
		typ, _ := v["type"].(string)
		typ = strings.ToLower(strings.TrimSpace(typ))
		switch typ {
		case "none":
			delete(obj, "tool_choice")
			suppress()
		case "auto", "required":
			obj["tool_choice"] = typ
		case "function":
			name := ""
			if fn, ok := v["function"].(map[string]any); ok {
				name, _ = fn["name"].(string)
			}
			if name == "" {
				name, _ = v["name"].(string)
			}
			if name = strings.TrimSpace(name); name != "" {
				obj["tool_choice"] = name
			} else {
				obj["tool_choice"] = "auto"
			}
		default:
			delete(obj, "tool_choice")
		}
	default:
		delete(obj, "tool_choice")
	}
}

// stripToolCallNames 收敛流式 tool_calls 的 name 语义为「每个 index 只出现一次」：
// 首片保留 function.name，同一 index 后续分片里的 name 键一律删除（无论上游是
// 空串还是重复非空串）。
//
// 来源：逐字移植自 Sliverkiss/workbuddy2api 的 internal/upstream/sse.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。
//
// 这是 OpenAI 官方流的真实形态——首帧带 name，后续帧只带 arguments 片段、不再出现
// name 键——因此是累加型与覆盖型客户端的共同祖先行为：
//   - 累加型（官方 WorkBuddy/CodeBuddy `name += tc_function?.name || ""`）：
//     后续分片 name 键缺失 → 追加空串，累积 name 保持唯一，不再拼成 Bash×帧数。
//   - 覆盖型（`name ?? state.name` 或 `if (name) state.name = name`）：
//     后续分片 name 键缺失 → 保留已建好的首帧 name，不被空串意外清空。
//     键缺失是比空串更安全的形态：`??` 与 truthy 守卫对缺失键必然保留旧值，
//     而对空串，`??` 会误判为重设并清空工具名。
//
// seen 记录每个 index 是否已发过首片（与 name 是否非空无关）；删除是幂等的。
// 只动 function.name 键，id/type/arguments 原样透传。
func stripToolCallNames(obj map[string]any, seen map[int]bool) {
	choices, _ := obj["choices"].([]any)
	for _, ci := range choices {
		c, _ := ci.(map[string]any)
		if c == nil {
			continue
		}
		delta, _ := c["delta"].(map[string]any)
		if delta == nil {
			continue
		}
		tcs, _ := delta["tool_calls"].([]any)
		for _, tci := range tcs {
			tc, _ := tci.(map[string]any)
			if tc == nil {
				continue
			}
			idx := 0
			if v, ok := tc["index"].(float64); ok {
				idx = int(v)
			}
			if seen[idx] {
				// 已发过首片：删除本分片的 name 键（存在即删，幂等）。
				if fn, _ := tc["function"].(map[string]any); fn != nil {
					delete(fn, "name")
				}
				continue
			}
			// 首现：保留 name 键原样（上游首片通常带非空 name；空 name 也照发，
			// 与 OpenAI 对「首帧无 name」的容忍一致），随后分片统一删除。
			seen[idx] = true
		}
	}
}

// translateMaxCompletionTokens 把 OpenAI 别名 max_completion_tokens 翻译为上游
// 认的 max_tokens（OpenAI 新别名，上游只认后者，直接透传会 400 code=11101）。
//
// 来源：移植自 Sliverkiss/workbuddy2api 的 internal/upstream/payload.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。
//
// 规则：
//   - 显式 max_tokens 优先——别名只删不译；
//   - 别名非正数值（0/null/负数）不翻译；
//   - 非数值别名（字符串等畸形）不翻译（原样透传由上游报 11101 参数错）；
//   - 无论是否翻译，别名一律删除（上游不认该字段）。
func translateMaxCompletionTokens(obj map[string]any) {
	alias, has := obj["max_completion_tokens"]
	delete(obj, "max_completion_tokens")
	if !has {
		return
	}
	if _, explicit := obj["max_tokens"]; explicit {
		return // 显式 max_tokens 优先：别名只删不译
	}
	// json.Unmarshal 数字 → float64（整数去整后回写，避免科学计数法/小数尾巴进上游 body）；
	// 其他数值类型防御性兼容（手构造 map 的调用方）。
	switch v := alias.(type) {
	case float64:
		if v > 0 && v == float64(int64(v)) {
			obj["max_tokens"] = int64(v)
		}
	case int64:
		if v > 0 {
			obj["max_tokens"] = v
		}
	case int:
		if v > 0 {
			obj["max_tokens"] = int64(v)
		}
	}
}

// -----------------------------------------------------------------------------
// 网关错误附加说明（error.gateway_hint）
//
// 来源：移植自 Sliverkiss/workbuddy2api 的 internal/upstream/hint.go
// （MIT License, Copyright (c) 2026 Sliverkiss）。
//
// 纪律：
//   - error.message 永远是上游 body 原文透传；gateway_hint 只做与 message **并列**的
//     网关视角补充说明，绝不替换 / 包装 message。
//   - 未覆盖形态返回空串 → 响应不带该字段（不编造）。
//   - 措辞是英文：错误响应面向客户端工具链，英文是通用口径。
// -----------------------------------------------------------------------------

// gatewayHint 按错误分类返回网关视角的补充说明（error.gateway_hint 字段值）。
// 返回空串 = 未覆盖形态，调用方不带该字段。
//
// 措辞面向「客户端下一步该做什么」，而非复述错误：换号无意义的形态提示等待，
// 请求问题的形态提示调整入参。
func gatewayHint(kind errKind) string {
	switch kind {
	case errPromptTooLong:
		return "request context exceeds the model's limit; reduce history/message size"
	case errWafBlock:
		return "upstream WAF blocked the gateway; retry after the block window"
	case errSoftRate:
		return "rate limited by upstream; retry after reset"
	case errAccountFault:
		return "account-level fault at upstream (auth/quota state); the gateway will rotate or disable this account"
	case errSessionDead:
		return "account session expired at upstream; the account is disabled until re-login"
	case errHardCredit:
		return "account credits exhausted at upstream; waiting for daily check-in to restore"
	case errModelBlocked:
		return "upstream has no such model on this backend; switch model or retry on another account"
	case errContentBlocked:
		// 措辞不含 "upstream"：content_blocked 响应有不含上游字样的既有口径。
		return "request content was rejected by content policy; adjust the prompt and retry"
	case errBadParams:
		return "upstream rejected the request parameters; check the request body and tool definitions"
	default:
		// errNone / errNotFound / errServer / errClient 等未覆盖形态：无 hint。
		return ""
	}
}

// writeOpenAIErrorHint 同 writeOpenAIError，但在 error 对象内并列附加 gateway_hint
// 字段（message 原文不动）。hint 为空串时不带该字段，响应与 writeOpenAIError 逐字节一致。
func writeOpenAIErrorHint(w http.ResponseWriter, statusCode int, errType, message string, kind errKind) {
	hint := gatewayHint(kind)
	if hint == "" {
		writeOpenAIError(w, statusCode, errType, message)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message":      message,
			"type":         errType,
			"code":         statusCode,
			"gateway_hint": hint,
		},
	})
}

// attachHintToErrorFrame 给上游 SSE error 帧的 error 对象附加 gateway_hint 字段。
// message / code / requestId 等原文一律不动；非 JSON 或结构不符时原样返回。
func attachHintToErrorFrame(payload, hint string) string {
	if hint == "" {
		return payload
	}
	var obj map[string]any
	if json.Unmarshal([]byte(payload), &obj) != nil {
		return payload
	}
	errObj, ok := obj["error"].(map[string]any)
	if !ok {
		return payload
	}
	errObj["gateway_hint"] = hint
	out, err := json.Marshal(obj)
	if err != nil {
		return payload
	}
	return string(out)
}

// frameGatewayHint 判定 SSE error 帧的 gateway_hint：帧内 error.message 走既有分类，
// 命中则返回 hint。非 error 帧 / 判不出 / 未覆盖形态返回空串（原样透传）。
func frameGatewayHint(payload string) string {
	if payload == "" || payload == "[DONE]" {
		return ""
	}
	var f struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(payload), &f) != nil || f.Error.Message == "" {
		return ""
	}
	return gatewayHint(classifyUpstream(http.StatusBadRequest, f.Error.Message))
}

func cleanChunkJSON(s string) string {
	var obj map[string]any
	if json.Unmarshal([]byte(s), &obj) != nil {
		return s
	}
	if choices, ok := obj["choices"].([]any); ok {
		for _, c := range choices {
			choice, ok := c.(map[string]any)
			if !ok {
				continue
			}
			// 空 finish_reason 归一化为 null：只有终止分片才应携带真实原因。
			if fr, ok := choice["finish_reason"].(string); ok && fr == "" {
				choice["finish_reason"] = nil
			}
			if delta, ok := choice["delta"].(map[string]any); ok {
				for k, v := range delta {
					if k == "function_call" && isEmptyFunctionCall(v) {
						delete(delta, k)
						continue
					}
					if isEmptyValue(v) {
						delete(delta, k)
					}
				}
			}
		}
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return s
	}
	return string(out)
}

// isEmptyFunctionCall 判断是否为旧版 function_call 空壳（name 与 arguments 均为空）。
// 真正的旧版函数调用会带 name 或 arguments，不应误删。
func isEmptyFunctionCall(v any) bool {
	fc, ok := v.(map[string]any)
	if !ok {
		return false
	}
	name, _ := fc["name"].(string)
	args, _ := fc["arguments"].(string)
	return strings.TrimSpace(name) == "" && strings.TrimSpace(args) == ""
}

func isEmptyValue(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case string:
		return x == ""
	case []any:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	}
	return false
}

func stripDataPrefix(s string) string {
	s = strings.TrimSpace(s)
	for strings.HasPrefix(s, "data:") {
		s = strings.TrimSpace(strings.TrimPrefix(s, "data:"))
	}
	return s
}

func writeOpenAIError(w http.ResponseWriter, statusCode int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    errType,
			"code":    statusCode,
		},
	})
}

func ifEmpty(val, fallback string) string {
	if strings.TrimSpace(val) == "" {
		return fallback
	}
	return val
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// accSiteLocked 返回账号所属站点 key；调用方需持有 accountMu。
func accSiteLocked(acc *Account) string {
	if acc == nil {
		return ""
	}
	if acc.Auth != nil {
		return profileForEdition(acc.Auth.Edition).Key
	}
	return profileForEdition(acc.Edition).Key
}
