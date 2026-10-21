# WorkBuddy Local Gateway

<img width="917" height="754" alt="image" src="https://github.com/user-attachments/assets/7dcfc461-1357-4991-9565-279047687898" />


基于腾讯 **CodeBuddy** 协议开发的**纯 Go、零 CGO 依赖、跨平台单二进制**本地 AI 代理网关。支持 CLI 与可选的 Web 控制台（`-webui`，管理 Key / 端口 / 模型 API 鉴权由 `config.json` 配置），登录、凭据续期、账号与模型管理均可在命令行或网页完成。

> **合并说明（2026-10-21）**：本仓库已合并上游 [CangShui/workbuddy-gateway](https://github.com/CangShui/workbuddy-gateway) **v1.14.21** 的全部功能，包括 Web 控制台、`config.json` 热加载、Anthropic Messages API（`/v1/messages`）、用量统计与账号告警、凭据保活与删除前校验、模型/账号黑白名单、完整请求诊断、官方会话头族与 DeepSeek Harness 前缀净化等。上游完整使用手册见 [`docs/user-guide.md`](docs/user-guide.md)；本文件保留本 fork 的串行调度、错误分类、成本账本与移植来源说明。

**同时支持两个上游站点**（同一套 `/v2/plugin/*` 协议，凭据按站点隔离，账号池可混挂轮询）：

| 站点 | 上游 | 登录方式 | 登录命令 |
|---|---|---|---|
| 国内站 | `copilot.tencent.com` / `www.codebuddy.cn` | 微信 / 企业微信扫码 | `login` |
| 国际站 | `www.workbuddy.ai` | 浏览器内登录（邮箱 / 验证码 / SSO） | `login -intl` |

---

## 代码来源与许可

本仓库是 [**CangShui/workbuddy-gateway**](https://github.com/CangShui/workbuddy-gateway) 的二次开发分支（fork），
并从 [**Sliverkiss/workbuddy2api**](https://github.com/Sliverkiss/workbuddy2api)（MIT 许可）移植了
错误分类、指纹脱敏与成本调度相关实现。两者面向同一套上游协议，实现路线不同。

### 与两个上游仓库的关系

| 仓库 | 角色 | 协议实现 | 许可 |
|---|---|---|---|
| [CangShui/workbuddy-gateway](https://github.com/CangShui/workbuddy-gateway) | **代码基线**（本仓库的 fork 来源） | 腾讯 CodeBuddy `/v2/plugin/*`（国内站 + 国际站） | 仓库未附许可文件 |
| [Sliverkiss/workbuddy2api](https://github.com/Sliverkiss/workbuddy2api) | **部分代码来源**（错误分类 / 脱敏 / 成本调度，见「许可与署名」） | 同一套 WorkBuddy 上游协议 | MIT |
| 本仓库 | 基线 + 二次开发 | 同基线，叠加下述改动 | 见「许可与署名」 |

### 基线与二次开发的边界

截至 `v1.12.0`（提交 `9eb5c65`）之前的全部代码、文档与提交历史来自 **CangShui/workbuddy-gateway**，
本仓库完整保留其 git 历史与原作者署名。此后的提交（`bd7b329` 起）为二次开发内容：

| 提交 | 内容 | 来源 |
|---|---|---|
| `bd7b329` | WAF 403 与授权失效分流，避免 WAF 拦截页误删凭据 | 修复基线缺陷；判定口径取自 wb2api |
| `680a944` | `errKind` 错误分类体系（13 个常量，区分账号故障与请求故障） | 移植 wb2api 的 `ErrKind` |
| `9cee7a6` | 指纹脱敏（修复基线中恒为 no-op 的实现） | **移植 wb2api 的 `sanitize.go`（MIT）** |
| `a9be76a` | 会话头族 + 内容拦截降级重试 | 移植 wb2api 的 `X-Conversation-Request-ID` 与降级重试思路 |
| `df4556c` | 成本账本分层选号 + 多代理池 | 移植 wb2api 的 costTier 与 issue #136 方案 a′ |
| `8abcaf3` | CI：推送 tag 自动发布 Linux 产物 | 本仓库新增 |
| `b58ecea` | 文档：代码来源与 MIT 署名 | 本仓库新增 |
| `1c5bea9` | `prompt_cache_key` 前缀缓存复用（费用降约 17 倍） | 移植 wb2api 的 `cache_key.go`（MIT） |
| `8d7e1d8` | tool 配对自愈（孤儿清理 + 结果块重排） | 移植 wb2api 的 `tool_pairing.go`（MIT） |
| `0dcefa0` | 残缺工具参数检测 + 轮转退避与抖动 | 移植 wb2api 的 `truncation.go` / `backoff.go`（MIT） |
| `aefbc39` | 流式工具名收敛 + 别名翻译 + `gateway_hint` | 移植 wb2api 的 `sse.go` / `payload.go` / `hint.go`（MIT） |
| `0d18103` | `tool_choice` 归一化 + 档位降级 + 思维链回填 + 空闲掐流 + `/v1/models` 能力透出 | 移植 wb2api 的 `payload.go` / `thinking.go` / `idle.go` / 目录能力字段（MIT） |
| `89781ca` | 修正超时分层：共享客户端默认安全、聊天路径显式 opt-out | 自研（修正 `0d18103` 引入的回归） |
| `546ca41` | 轮转耗尽兜底透传真实上游错误（`11102`/`11129` 不再被 429 掩盖）；`11129` 归入 errBadParams | 自研（修复基线既有缺陷） |

设计取舍记录在 [`FORK-PLAN.md`](FORK-PLAN.md)，其中包含 wb2api 的实测数据（卸载前日志累计 WAF 命中 680 次）与本仓库的抗 WAF 架构依据。

### 移植原则

二次开发**保留基线的调度架构**，并按 wb2api 的设计思路补齐错误处理与成本调度：

- **保留基线的串行调度**：单账号严格串行（`acc.lock` 包住整个上游调用）是抗 WAF 的核心，不可动。
- **裁剪移植范围**：wb2api 的并发调度、六类定时任务（含活跃地图连发）本身是风控特征，明确不移植。
- **保持依赖极简**：仅 `go-qrcode` + `google/uuid`，零 CGO。

### 许可与署名

| 来源 | 许可 | 本仓库的使用方式 |
|---|---|---|
| CangShui/workbuddy-gateway | 未附许可文件 | 本仓库的代码基线（fork 来源，含完整 git 历史） |
| **Sliverkiss/workbuddy2api** | **MIT** | 见下方「逐字移植清单」 |
| 本仓库 | 沿用基线分发方式 | 二次开发部分（提交 `bd7b329` 起） |

**逐字移植清单**（经与 wb2api 源码逐函数比对确认）：

| 位置 | 来源文件 | 比对结果 |
|---|---|---|
| `sanitizeFeatures` / `sanitizeHdrRe` / `sanitizeBareHdrRe` / `sanitizeKvRe` / `sanitizeText` / `hasFingerprint` / `sanitizeContent` / `sanitizeToolCalls` | `internal/upstream/sanitize.go` | 代码逐字相同（正则表、替换表、注释一致） |
| `sanitizeRewrites` | 同上 | 99.1%（仅注释措辞微调） |
| `hasBusinessEnvelope` | `internal/upstream/client.go` | 逐字相同 |
| `isWafBlocked` | 同上（原名 `IsWafBlocked`） | 95.9%（仅改名与注释裁剪） |
| `nextMidnightCST` | `internal/server/degrade.go` | 逐字相同（仅删去一行注释） |
| `injectPromptCacheKey` | `internal/upstream/cache_key.go`（原名 `InjectPromptCacheKey`） | 53.8%（同结构改写：内联 `strField` 为 TrimSpace、键前缀改 `wbgw-`） |
| `buildPromptCacheKey` | 同上（原名 `buildCacheKey`） | 84.0%（仅改键前缀与函数名） |
| `repackToolResultBlocks` / `cleanupOrphanToolCalls` | `internal/upstream/tool_pairing.go` | 逐字相同（含注释） |
| `isTruncatedArguments` / `dropTruncatedToolCalls` | `internal/upstream/truncation.go` | 逐字相同（含注释） |
| `jitterDur` | `internal/server/backoff.go` | 94.0%（常量改可注入变量以便测试） |
| `rotateBackoffDelay` | 同上（原名 `backoffAfter`） | 94.4%（改名 + 常量改可注入变量） |
| `sleepCtx` | 同上 | 逐字相同 |
| `stripToolCallNames` | `internal/upstream/sse.go` | 逐字相同（含注释） |
| `translateMaxCompletionTokens` | `internal/upstream/payload.go` | 逐字相同（含注释） |
| `gatewayHint` / `attachHintToErrorFrame` / `frameGatewayHint` | `internal/upstream/hint.go` | 按设计思路适配（合并 `FrameKind` 与 `GatewayHint` 为单一入口、裁剪 11133/11135 图片形态判定——本仓库无模型能力目录） |
| `normalizeToolChoice` | `internal/upstream/payload.go` | 逐字相同（含注释） |
| `normalizeReasoningEffort` / `effortRank` | 同上 | 按设计思路适配（新增 `off`/`none` 不上抬保护、值归一化写回；档位数据源改为上游实时目录而非静态表） |
| `backfillReasoningContent` / `isDeepSeekModel` | `internal/upstream/thinking.go` | 逐字相同（含注释） |
| `idleMonitoringBody` / `monitorBody` | `internal/upstream/idle.go` | 按设计思路适配（`monitorBody` 在 idle≤0 时仍承担 cancel 职责，调用方无需分支；新增三层超时拆分） |
| `idleTick` | 同上 | 逐字相同（含注释） |
| `firstCatalogEntry` / `supportedEffortsFor` | `internal/upstream/context_catalog.go` + `effort_catalog.go` | 按设计思路重写（不引入静态兜底表，档位一律以实时目录为准；跨站取并集） |

其余移植项（`errKind` 分类体系、成本分层选号、会话头族、内容拦截降级、多代理池）
为**按设计思路的裁剪重实现**，非逐字复制：结构对齐 wb2api 的调度语义，
但按基线的串行架构重写，函数名、数据流与判定顺序均不同。

**移植时的适配差异**（本仓库基线的既有行为必须保留）：

- `normalizeToolPairing` 排在 `ensureLeadingSystemMessage` **之后**。基线会把后续出现的
  system/developer 消息提升到首位（11-128 修复），而 repack 刚把夹在 tool 结果中间的
  developer 挪到结果之后——若顺序颠倒，提升动作会把它搬回前面，配对重新断裂。

> **MIT 许可声明**：本仓库包含来自 [Sliverkiss/workbuddy2api](https://github.com/Sliverkiss/workbuddy2api) 的代码，
> 版权归其作者所有，依 MIT 许可使用：
>
> ```
> MIT License
>
> Copyright (c) 2026 Sliverkiss
>
> Permission is hereby granted, free of charge, to any person obtaining a copy
> of this software and associated documentation files (the "Software"), to deal
> in the Software without restriction, including without limitation the rights
> to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
> copies of the Software, and to permit persons to whom the Software is
> furnished to do so, subject to the following conditions:
>
> The above copyright notice and this permission notice shall be included in all
> copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
> FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
> AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
> LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
> OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
> SOFTWARE.
> ```
>
> 基线仓库未附许可文件，本仓库作为 fork 沿用其分发方式。若你是权利人就许可问题有疑问，请通过 issue 联系。

### 两个上游仓库的协议关系

两者**面向同一套腾讯上游协议**，因此本仓库可以把 wb2api 的错误判定口径直接接过来：

| 协议端点 | 路径 | 说明 |
|---|---|---|
| 登录状态 / 账号 / 换码 | `/v2/plugin/auth/state`、`/v2/plugin/login/account`、`/v2/plugin/auth/token` | 扫码登录族，国内站用微信 / 企业微信 |
| Token 刷新 | `/v2/plugin/auth/token/refresh` | 凭据续期，按 `edition` 路由到对应站点 |
| 对话 | `/v2/chat/completions` | 上游统一入口，请求 / 响应结构一致 |
| 模型目录 | `/v2/enterprises/personal/models` | 实时接口，两站同路径 |
| 额度与签到 | `/v2/billing/meter/get-user-resource`、`/v2/billing/meter/daily-checkin` | 国内站签到用；国际站跳过签到 |

站点差异仅在 **Base 与登录方式**：

```text
国内站  Base = https://copilot.tencent.com     登录 = 扫码（终端 ASCII 二维码）
国际站  Base = https://www.workbuddy.ai        登录 = 浏览器内完成（邮箱 / 验证码 / SSO）
```

两仓库的实现差异在**调度与错误处理**，不在协议层：

| 维度 | CangShui/workbuddy-gateway（基线） | Sliverkiss/workbuddy2api |
|---|---|---|
| 并发模型 | 单账号严格串行（抗 WAF 的核心） | 多号并发 + 请求级轮换 |
| 出口 | 单代理 `-proxy` | 无内置代理 |
| 错误分类 | 8 个 `is*` 谓词（无统一枚举） | 13 个 `ErrKind` 常量 |
| 指纹脱敏 | 无（本仓库已补） | `sanitize.go` 三层净化 |
| 本仓库取舍 | **保留串行 + 引入 wb2api 的错误分类与脱敏** | — |

---

## 目录

- [代码来源与许可](#代码来源与许可)
- [核心特性](#核心特性)
- [上游 v1.14.21 合并功能](#上游-v11421-合并功能)
- [命令总览](#命令总览)
- [serve](#serve)
- [Web 控制台](#web-控制台)
- [login](#login)
- [status](#status)
- [refresh](#refresh)
- [monitor](#monitor)
- [probe](#probe)
- [reset](#reset)
- [version / help](#version--help)
- [多账号池](#多账号池)
- [模型列表与倍率](#模型列表与倍率)
- [客户端接入](#客户端接入)
- [各平台部署](#各平台部署)
- [安全提示](#安全提示)
- [从源码构建](#从源码构建)
- [自动构建与发布](#自动构建与发布)

---

## 核心特性

> 标注 ✦ 的条目为本仓库二次开发新增，其余为基线（CangShui/workbuddy-gateway）既有能力。

- **国内 / 国际双站反代**：两个站点走同一套协议，凭据通过 `edition` 字段区分，刷新与对话自动路由到各自上游。
- **模型完全透传**：客户端传什么 `model` 就原样中继到上游；唯一例外是 `config.json` 的模型黑/白名单（禁用模型在网关侧直接 403，不调用上游）。`/v1/models` 仅用于客户端自动补全，不影响实际转发。
- **模型列表双来源合并**：实时接口 + npm 静态目录，按 ID 去重、接口优先；失败用本地缓存，两边都失败且无缓存时该站点本轮不展示模型（不影响调用）。
- **模型倍率与价格探测**：促销生效时展示 `credits` × factor；促销过期或接口无有效倍率时，可由余额未耗尽的同站点账号实测。**后台自动探测默认关闭**，仅显式传入 `-allow-price-probes` 才开启；正常客户端请求仍会从 `usage.credit` 学习免费/收费状态，不消耗额外探测请求。
- **多账号池 + 轮询负载均衡**：`-auth` 逗号分隔或 `-auth-dir` 目录，请求按 round-robin 分发；国内站与国际站账号可混挂。
- **模型级隔离**：`6004` 只冷却触发它的账号 + 模型，`14018` 只阻断该账号的当前收费模型，不再因为一个模型拖垮整个账号。
- **免费站点优先**：同一模型若「一个站点免费、另一个站点收费」，优先使用免费站点账号直至其受限；两个站点都收费（仅倍率不同）时不做倾斜，正常轮询。
- **免费/收费学习**：按「账号 + 模型」从响应 `usage.credit` 学习；`credit=0` 且样本足够（`total_tokens ≥ 100`）才判定免费，避免小样本误判。
- ✦ **错误分类体系**：13 个 `errKind`（12 个实际分类 + `errNone` 兜底）区分「账号的问题」与「请求的问题」——前者冷却 / 禁用该账号并换号重试，后者不罚账号直接透传，避免把请求级错误记到账号头上。（移植 wb2api 的 `ErrKind`）
- ✦ **WAF 拦截识别**：403 且响应体无业务信封（无 `code` / `msg` 字段）判定为 WAF 拦截形态，仅软冷却 60 秒、**不删除凭据**；带业务信封的 403 才走授权失效判定。（修复基线误删凭据缺陷）
- ✦ **指纹脱敏**：出站请求体清洗上游内容审核拒绝的字面量（Claude Code / Codex 模板句、Anthropic 计费头键值对、`11-128` 字面量），改写单字破坏精确匹配而保留语义；覆盖 `content` / `reasoning_content` / `tool_calls.arguments` 三个字段。（基线实现恒为 no-op，本仓库修复）
- ✦ **官方会话头族**：出站恒发 `X-Conversation-ID` / `X-Conversation-Request-ID` / `X-Conversation-Message-ID`，语义对齐官方 WorkBuddy 5.8.4 客户端；同一次逻辑请求的换号/重试复用同一组标识，修复国际站与工具请求的 `11128 conversation id missing`。会话键支持入站头、`X-Session-ID` 与 body/metadata 字段，非 UUID 会话键按调用方隔离生成稳定 UUIDv5。（替换早期 wb2api 的 B3/Root 头方案）
- ✦ **内容拦截降级重试**：passthrough / append 模式下首遇内容拦截时，切换到中性系统提示词（直至次日 CST 00:00）同请求重试一次，破解模板句指纹误报；custom 模式已是网关提示词，不进入降级。
- **成本账本与分层选号**：每次成功请求按 `usage.credit` 折算每千 token 单价（EMA α=0.3 平滑、6 小时过期）记入「账号 + 模型」账本；选号时按 **免费 > 未知 > 收费** 硬过滤分层，收费层内单价低者优先，避免把流量浪费在贵号上。
- ✦ **成本条件探索（反垄断）**：免费层账号垄断某模型时，未知层账号永远轮不到、也就永远学不到属性。默认每 30 分钟（`-cost-explore-interval`，0 关停）把**一次真实用户请求**改道给未知账号搭车学习——零新增上游调用，学成即毕业；探索失败自动回退，不影响本次请求。（移植 wb2api issue #136 方案 a′）
- ✦ **多代理池**：`-proxies` 配置多个出口代理，账号按凭据文件名稳定散列绑定到其中一个出口 IP，单 IP 风控不再同时命中全部账号。
- ✦ **前缀缓存复用（费用优化）**：出站请求注入 `prompt_cache_key`，让同一客户端对同一账号的连续请求命中上游前缀缓存——实测同一段 8k token 前缀，命中后 `credit` 从 ≈0.34 降到 ≈0.02（**费用降约 17 倍**）。键按账号 UID 硬隔离（`wbgw-<uid8>-<会话哈希>`），跨账号绝不碰撞（否则会命中他人缓存、泄露对话内容）；客户端已显式携带该字段时原值保留不覆盖。（移植 wb2api 的 `cache_key.go`）
- ✦ **tool 配对自愈**：出站前对 `messages` 做两步归一化——**重排**（把夹在 tool 结果中间的非 tool 消息挪到整组之后）与**清理**（按 id 对称剔除无结果的 `tool_call` 与无调用的 `tool` 结果）。工具执行失败时客户端会把 `tool_calls` 写进历史却写不回结果，这条坏历史每次重放都让上游返回 400（`11148`）**顶死整条会话**；Codex 的 `image_resize_notice` 插在并行结果中间同样判配对断裂。网关是最后一道防线：宁可丢一轮工具上下文，也好过会话报废。（移植 wb2api 的 `tool_pairing.go`）
- ✦ **残缺工具参数检测**：流被截断时（`finish_reason=="length"` 模型因 max_tokens 中止，或上游连接中断未发 `[DONE]`）工具调用的 `arguments` 会只剩半截 JSON，原样下发会让客户端解析失败**卡死会话**。判定口径只认「非空但无法解析」——空串是合法的无参工具、能解析的标量/数组属模型输出错误，都不在此列；命中则丢弃该调用（不补成 `{}` 伪造合法外观）。（移植 wb2api 的 `truncation.go`）
- ✦ **轮转退避与抖动**：换号重试前等待 `500ms·2^n`（封顶 8s）并施加 ±25% 抖动，让上游频控窗口滑过。立即连环重试会以固定节奏持续撞击 WAF 的密度判罚；抖动打散多请求的同相位重试（齐步走的退避会以固定周期再次聚团）。首次尝试零开销，`ctx` 取消（客户端断连/停机）立即终止轮转。（移植 wb2api 的 `backoff.go`）
- ✦ **流式工具名收敛**：上游在**每一帧**重复下发 `tool_calls[].function.name`，累加型客户端（`name += ...`）会把工具名拼成 `BashBashBash` 导致工具调用失败。网关按 index 只保留首片的 `name` 键，后续分片删除该键——这是 OpenAI 官方流的真实形态，累加型（追加空串）与覆盖型（`??` 守卫保留旧值）客户端同时正确。（移植 wb2api 的 `stripToolCallNames`）
- ✦ **`max_completion_tokens` 别名翻译**：OpenAI 新别名，上游只认 `max_tokens`，直接透传会 400（`11101`）。显式 `max_tokens` 优先（别名只删不译）、非正值与畸形值不翻译，别名一律删除。（移植 wb2api 的 `translateMaxCompletionTokens`）
- ✦ **`error.gateway_hint` 附加说明**：错误响应中在 `error` 对象内**并列**附加网关视角的可操作说明（如「上下文超限，请减少历史」「WAF 拦截，等窗口过去再试」），`message` 永远是上游原文透传、绝不替换包装；未覆盖的错误形态不带该字段（不编造）。流式 error 帧同样附加。（移植 wb2api 的 `hint.go`）
- ✦ **`tool_choice` 归一化**：上游把该字段定义为 **string**，而 OpenAI 官方 SDK 默认发对象形式（`{"type":"function","function":{"name":"x"}}`），直接透传会 400（`11101`）——用官方 SDK 的客户端此前全部失败。网关按上游类型改写：`function` 对象取 name 转字符串、`auto`/`required` 对象转对应字符串、`none` 删字段并抑制 `tools`/`functions`、无法识别一律删字段。（移植 wb2api 的 `normalizeToolChoice`）
- ✦ **推理档位降级**：客户端传的档位模型不支持时（如对只支持到 `high` 的模型传 `max`）上游会 400。网关按上游实时目录下发的 `reasoning.supportedEfforts` 降到 ≤请求档位的最高支持档；支持档全部高于请求档时取最低档（偏离最小）。**`off`/`none` 绝不上抬**——那会把客户端明确的「关闭思考」反转成开启，比透传更糟。未收录档位的模型一律透传（不猜测），snake/camel 双字段兼容。（移植 wb2api 的 `normalizeReasoningEffort`）
- ✦ **DeepSeek 思维链回填**：多轮会话中历史 assistant 消息带过 `reasoning` 痕迹时，上游要求**所有** assistant 消息都带 `reasoning_content` 字段（可为空串），否则判会话不一致。网关检测到痕迹后为缺失者回填（有 `reasoning` 的复制、没有的补空串），已有值不覆盖；无痕迹时零改动（不白白加字段）。仅 deepseek 系模型生效。（移植 wb2api 的 `backfillReasoningContent`）
- ✦ **超时分层与流中空闲掐流**：客户端本身不设总时长，避免长思考/长输出被硬切断。上游请求由三层约束：`ResponseHeaderTimeout`（默认 5 分钟，首字节前换号）、流中空闲看门狗（默认 2 分钟无数据即中断并释放账号锁）与瞬时网络错误重试（默认 2 次）。控制类请求（刷新/额度/签到/目录/探测）另有 60 秒 ctx 兜底；`config.json` 的 `upstream` 段可热更新这些阈值。非流式请求在本地聚合上游 SSE，不再额外设置总时长。
- ✦ **`/v1/models` 能力透出**：模型条目附带上游声明的 `context_length` / `max_output_tokens` / `supports_images` / `supports_tool_call` / `reasoning_supported_efforts` / `reasoning_default_effort`，客户端可据此自动配置上下文预算与推理档位（不再盲传非法档位）。零值一律**省略字段**——不编造「假 131072」误导客户端提前截断、白白丢上下文。同时补上 npm 静态目录解析丢弃这些字段的缺陷（实时接口不可用时它是唯一来源）。（移植 wb2api 的 `context_catalog` / `effort_catalog` 思路）
- **国内站每日自动签到**：服务启动、凭据热加载时立即补签，之后每天 `UTC+8 09:00` 自动签到；国际站跳过。
- **凭据热加载（免重启）**：默认每 5 秒扫描凭据来源，新增 / 更新 / 删除凭据免重启生效。
- **授权失效安全处理**：401，或 403 携带业务信封且命中失效文案（`invalid token` / 登录过期等）时，先停止调度并写失效标记；删除凭据前还会做一次只读校验，确认凭据确实不可用才删除，网络/5xx/普通 403 无法判定时保守保留。**WAF 形态的 403 只软冷却，不删凭据**。
- **后台自动续期 + 主动保活**：每 5 分钟检查 Token，距过期不足 15 分钟自动刷新；另可用 `-keepalive-hours`（默认每天 22:00）在固定时刻主动刷新全部账号，避免长期空闲账号静默失效。连续刷新失败达到阈值才判定登录态失效。
- **流式分片规范化**：把上游每个分片携带的 `finish_reason:""` 归一化为 `null`，避免 Anthropic 翻译层误判 `stop_reason` 导致工具不执行；流正常关闭但没有 `finish_reason` 时拒绝伪造 `[DONE]`，向客户端下发明确的中断错误。
- **OpenAI 兼容协议**：`/v1/chat/completions`（SSE 流式 + 非流式聚合）、`/v1/responses`（Responses API）、`/v1/models`、`/health`。
- **Anthropic Messages API**：`/v1/messages`（流式 + 非流式聚合）与 `/v1/messages/count_tokens`，鉴权兼容 `x-api-key`，Claude Code 等 Anthropic 客户端无需外部翻译层。
- **Web 控制台（可选）**：`-webui` 启用独立管理端口（默认 8316），提供总览、账号、模型、日志、配置与凭据（脱敏）管理；管理 Key 与模型 API Key 相互独立，均由 `config.json` 的 `gateway` 段配置。
- **`config.json` 热加载**：模型/账号规则、`systemPrompt`、`debug`、日志保留、上游超时与重试在 1 秒内生效；解析失败保留上一份有效配置。端口变更需重启。
- **用量统计与账号告警**：`stats.json` 持久化近 5 小时用量/耗时统计，账号健康度与告警内联到状态快照，Web 总览与 `monitor` 可查看。

---

## 上游 v1.14.21 合并功能

本次合并在保留本 fork 串行调度、错误分类与成本账本的前提下，带入了上游 CangShui/workbuddy-gateway v1.14.21 的全部功能。

### Web 控制台

- `serve -webui` 启用独立管理端口（默认 `8316`），与模型 API 端口（默认 `8317`）分离。
- 页面：总览、账号、模型、日志、配置（脱敏）、凭据（令牌脱敏）、访问设置。
- 支持网页扫码新增账号、删除凭据、切换模型/账号规则、保存日志保留天数、切换模型 API Key 鉴权。
- 管理 Key（`gateway.adminKey`）与模型 API Key（`gateway.apiKey`）相互独立；管理台始终要求管理鉴权，关闭模型鉴权不会关闭管理台。
- 详细启动方式与 `config.json` 示例见 [`docs/user-guide.md`](docs/user-guide.md) 的「webui」章节。

### config.json 热加载

工作目录下的 `config.json` 控制网关行为，修改后约 1 秒生效（端口变更需重启）：

```json
{
  "gateway": {
    "apiPort": 8317,
    "webPort": 8316,
    "adminKey": "",
    "apiKeyEnabled": false,
    "apiKey": ""
  },
  "systemPrompt": { "fallback": "", "force": "" },
  "debug": { "enabled": false, "fullRequests": false },
  "logs": { "retentionDays": 0 },
  "upstream": {
    "headerTimeoutSeconds": 300,
    "idleTimeoutSeconds": 120,
    "transientRetries": 2
  },
  "models": {
    "blocklist": [],
    "accounts": {
      "all": { "allowlist": [], "blocklist": [] }
    }
  }
}
```

- `models.blocklist` / `models.allowlist` 控制模型是否可调用；非空黑白名单不能同时存在。
- `models.accounts` 支持保留键 `all` 与按模型指定凭据文件白/黑名单。
- `systemPrompt.fallback` 只在客户端没有 system 时替换保底提示；`force` 在首条 system 末尾追加网关指令（后置，风险自负）。
- `debug.enabled` 写入 `logs/debug-YYYY-MM-DD.jsonl`；`debug.fullRequests=true` 额外记录完整入站/出站请求（凭据字段脱敏，但正文仍属敏感数据，仅排障时开启）。
- `logs.retentionDays` 为 `0` 表示保留全部；正数按本地日期保留最近 N 天。
- `upstream` 段控制首字节超时、流空闲超时与瞬时网络重试次数。

### Anthropic Messages API

- 原生入口 `/v1/messages`（SSE 流式 + 非流式聚合）与 `/v1/messages/count_tokens`。
- 鉴权同时接受 `Authorization: Bearer <key>` 与 `x-api-key`，Claude Code 可直接把 `ANTHROPIC_BASE_URL` 指向网关。
- 与 Chat / Responses 共用同一套系统提示词、工具修复、推理历史与 11128 净化规则。

### 统计、告警与日志

- `stats.json` 持久化每模型请求数、Token、首 Token 与耗时，Web 总览与 `monitor` 使用近 5 小时滚动窗口。
- 账号健康度与告警内联到 `workbuddy-status.json`，Web 总览顶部展示，可跳转日志。
- 结构化 JSON 调试日志、日志保留与清理；`logs/` 下的运行/调试/部署/前端日志可按天保留。

### 凭据保活与安全校验

- `-keepalive-hours`（默认 `22`）每天固定时刻主动刷新全部账号，只调刷新接口，不请求模型。
- 覆盖新凭据前校验账号标识、站点与可用性；删除凭据前做只读校验，无法判定时保守保留。
- 连续刷新失败达到阈值才判定登录态失效；刷新令牌到期时间会在 `status` / `monitor` / Web 账号表展示。
- 企业成员账号改用分配额度接口，`limitNum=-1` 显示「不限」，不会误判为零余额。

### 模型与账号规则

- 模型黑/白名单命中后从 `/v1/models`、`/health` 与统计表隐藏，请求直接 403，不消耗上游额度。
- `models.accounts` 可按凭据文件名限制某模型可用账号；全局 `all` 规则与模型专属规则同时生效。
- 模型能力字段新增 `default_context_length`、`context_length_options`、`capabilities_by_site`，与 `context_length` / `max_output_tokens` 分开表达。

### 11128 渠道校验修复

- **缺会话头**：出站恒发 `X-Conversation-ID` / `X-Conversation-Request-ID` / `X-Conversation-Message-ID`，修复国际站与工具请求的 `11128 conversation id missing`。
- **身份句前缀**：system 消息以 `You are an AI agent powered by DeepSeek Harness` 开头时（偏移 0、忽略大小写、后缀任意），等义改写为中性前缀，修复 `11128 Illegal API invocation from an unapproved channel`。
- 两者都有单元测试覆盖；上游若继续收紧渠道校验，可能需要再补规则。

### 构建与文档

- [build.ps1](build.ps1) 支持 Windows / Linux / macOS 的 x86、x64、ARM、ARM64 产物与 SHA256 清单。
- 完整配置、网页操作、超时、诊断、企业账号与部署说明见 [`docs/user-guide.md`](docs/user-guide.md)。

> 本仓库的 CI（`.github/workflows/release.yml`）与 `build.ps1` 都覆盖上述 9 个平台/架构组合；推送 `v*` tag 后会在 Releases 中发布全部产物与 `SHA256SUMS`。

## 命令总览

```text
workbuddy-gateway [command] [options]

命令:
  serve     启动本地网关（默认命令，不带子命令时等同 serve）
  login     登录并获取 / 更新凭据
  status    查看账号池状态
  refresh   手动刷新所有账号访问令牌
  monitor   前台实时监控：账号表格 + 模型统计附表 + 最近日志
  probe     主动探测账号对指定模型的免费 / 收费属性（需 serve 运行中）
  reset     清空除登录凭据外的全部本地数据，并重新拉取模型与倍率
  version   查看版本信息
  help      查看帮助
```

全局选项（对所有命令可用）：

| 选项 | 默认 | 说明 |
|---|---|---|
| `-addr <ip>` | `127.0.0.1` | 网关监听地址 |
| `-port <port>` | `8317` | 网关监听端口 |
| `-auth <path>` | 自动发现 | 凭据文件路径，支持逗号分隔多个 |
| `-auth-dir <dir>` | 空 | 凭据目录，自动加载目录内所有 `workbuddy*.json` |
| `-api-key <key>` | 空 | 旧启动方式兼容：显式传入时覆盖 `config.json` 的 `gateway.apiKey` 并启用鉴权 |
| `-proxy <url>` | 空 | 上游请求代理，如 `http://127.0.0.1:7890`、`socks5://...` |
| `-verbose` | `false` | 输出详细调试日志 |
| `-intl` | `false` | 仅 `login` 生效：登录国际站 |
| `-webui` | `false` | 启用独立 Web 控制台（默认 `http://<addr>:8316/ui/`） |
| `-allow-price-probes` | `false` | 开启后台自动价格探测（会发真实生成请求，可能消耗积分） |
| `-keepalive-hours <h,h,...>` | `22` | 主动续期时刻；留空关闭 |
| `-prompt-mode <mode>` | `passthrough` | `passthrough` / `custom` / `append` |
| `-prompt-text <text>` | 空 | `custom` / `append` 模式的网关提示词 |
| `-reload-interval <sec>` | `5` | 凭据热加载扫描间隔，`0` 关闭 |
| `-models-refresh <min>` | `60` | 模型目录刷新间隔，`0` 关闭 |
| `-cost-explore-interval <dur>` | `30m` | costTier 条件探索窗口，`0` 关停 |
| `-proxies <url1,url2,...>` | 空 | 多代理池：账号按凭据文件名稳定绑定到其中一个出口 IP |
| `-extra-models <id1,id2,...>` | 空 | 额外模型 ID：上游可请求但官方目录未收录的模型，透出到 `/v1/models` 供客户端发现（去重、大小写归一） |

---

## serve

启动本地网关，默认命令。

```bash
# 默认监听 127.0.0.1:8317，自动加载当前目录下所有 workbuddy*.json
workbuddy-gateway serve

# 自定义端口与监听地址
workbuddy-gateway serve -port 9000 -addr 0.0.0.0

# 显式指定多个凭据文件（逗号分隔，轮询）
workbuddy-gateway serve -auth workbuddy.json,workbuddy2.json

# 目录模式：加载目录内所有 workbuddy*.json
workbuddy-gateway serve -auth-dir ./auths

# 上游走代理 + 详细日志（客户端鉴权建议在 config.json 的 gateway 段配置）
workbuddy-gateway serve -proxy http://127.0.0.1:7890 -verbose

# 关闭凭据热加载
workbuddy-gateway serve -reload-interval 0

# 关闭模型目录自动刷新
workbuddy-gateway serve -models-refresh 0

# 多代理池：账号按文件名稳定散列到多个出口 IP
workbuddy-gateway serve -proxies http://127.0.0.1:7890,socks5://127.0.0.1:1080

# 补充官方目录未收录但上游可用的模型（让客户端可发现）
workbuddy-gateway serve -extra-models deepseek-v4.1-flash

# 关停成本条件探索（回到纯成本分层行为）
workbuddy-gateway serve -cost-explore-interval 0

# 启用 Web 控制台（独立 8316 端口，管理 Key/端口由 config.json 配置）
workbuddy-gateway serve -webui

# 主动保活：每天 10:00 与 22:00 各刷新一次全部账号
workbuddy-gateway serve -keepalive-hours 10,22

# 显式开启后台价格探测（会发真实生成请求，可能消耗积分）
workbuddy-gateway serve -allow-price-probes
```

启动后提供的端点：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/chat/completions`、`/chat/completions` | Chat Completions，支持 SSE 流式与非流式 |
| POST | `/v1/responses`、`/responses` | OpenAI Responses API |
| POST | `/v1/messages` | Anthropic Messages API（流式 + 非流式聚合） |
| POST | `/v1/messages/count_tokens` | Anthropic token 估算（本地近似，不调用上游） |
| GET | `/v1/models`、`/models` | 模型列表，响应头 `X-Model-Source` 标注来源 |
| GET | `/health`、`/ping` | 健康检查，返回 `version`、`model_count`、`model_source` |
| POST | `/admin/probe` | 供 `probe` 命令调用，**仅接受回环来源** |
| GET | `/ui/`、`/admin/api/*` | Web 控制台与只读/管理接口（仅 `-webui` 时监听） |
| GET | `/` | 简单文本说明 |

后台任务（`serve` 启动后自动运行）：

| 任务 | 周期 | 说明 |
|---|---|---|
| Token 续期检查 | 5 分钟 | 距过期不足 15 分钟自动刷新 |
| 主动保活 | 每天 `-keepalive-hours` 指定时刻（默认 22:00） | 主动刷新全部账号，只调刷新接口 |
| 额度扫描 | 5 分钟 | 每凭据 10 秒超时，超时保留旧值；`剩余=0` 标记付费耗尽 |
| 模型目录刷新 | 60 分钟 | 实时接口 + npm 目录，合并去重后写缓存 |
| 模型价格探测 | 默认关闭 | 仅 `-allow-price-probes` 时每 30 分钟检查 / 每模型 12 小时一轮 |
| 每日签到 | 每天 `UTC+8 09:00` | 仅国内站 |
| 状态快照 | 3 秒 | 写 `workbuddy-status.json` 供 `monitor` / Web 控制台读取 |
| 凭据热加载 | 5 秒 | 扫描凭据新增 / 更新 / 删除 |
| `config.json` 热加载 | 1 秒 | 规则/提示词/调试/超时变化即时生效，端口需重启 |
| 用量统计落盘 | 周期写 `stats.json` | Web 总览与 `monitor` 使用近 5 小时窗口 |
| 日志清理 | 每小时 | 按 `logs.retentionDays` 清理过期日志 |

---

## Web 控制台

Web 控制台是**可选**的独立管理端口，默认不开；模型 API 仍监听 `8317`，管理台默认监听 `8316`。

```bash
# 启动 API + Web 控制台
workbuddy-gateway serve -webui

# 浏览器访问（仅本机或 SSH 端口转发）
http://127.0.0.1:8316/ui/
```

`config.json` 的 `gateway` 段控制端口与鉴权：

```json
{
  "gateway": {
    "apiPort": 8317,
    "webPort": 8316,
    "adminKey": "请替换为 32 位随机管理 Key",
    "apiKeyEnabled": true,
    "apiKey": "sk-请替换为模型 API Key"
  }
}
```

| 配置 | 说明 |
|---|---|
| `apiPort` | 模型 API 端口，默认 `8317`；`-port` 显式传入时覆盖 |
| `webPort` | Web 控制台端口，默认 `8316`；仅在 `-webui` 时监听 |
| `adminKey` | 管理台登录 Key；为空时，**仅本机/回环访问** `/ui/` 可一键生成，且只在弹窗展示一次 |
| `apiKeyEnabled` | 是否校验模型 API Key；关闭时不带 Key/任意 Key 都可调用 |
| `apiKey` | 模型 API Key；管理 Key 与它相互独立，不能互相登录 |

页面功能：

- **总览**：账号健康度、告警、站点统计、模型请求量/Token/首 Token/耗时。
- **账号**：账号表格、套餐、额度、冷却、模型账本；扫码新增账号、删除凭据。
- **模型**：模型列表、倍率来源、能力字段；切换模型启用/停用。
- **日志**：运行日志、调试日志、保留天数与立即清理。
- **配置**：查看/编辑 `config.json`（Key 脱敏），带风险确认。
- **访问设置**：保存 API/Web 端口、切换模型 API Key 鉴权。

安全注意：

- 管理台默认只监听 `127.0.0.1`；对外访问请置于反向代理 + TLS 之后，并设置强 `adminKey`。
- 管理台与模型 API 是两个端口、两套 Key；不要把 `8316` 直接暴露到公网。
- `config.json` 含明文管理 Key / API Key，已被 `.gitignore` 排除，请限制文件权限（`0600`）。
- 管理接口只接受同源请求；`/admin/api/*` 必须携带 `Authorization: Bearer <adminKey>`。

---

## login

登录并保存凭据。国内站输出终端 ASCII 二维码；国际站在浏览器内完成。

```bash
# 国内站（微信 / 企业微信扫码）
workbuddy-gateway login

# 保存到指定文件（多账号推荐）
workbuddy-gateway login -auth workbuddy2.json

# 国际站（浏览器内完成，邮箱 / 验证码 / SSO）
workbuddy-gateway login -intl
workbuddy-gateway login -intl -auth workbuddy-intl.json
```

说明：

- 默认保存到 `workbuddy.json`；`-auth` 可指定其他路径。
- 国际站凭据写入 `edition: "intl"`，与国内站凭据可混挂在同一账号池。
- 重新登录会覆盖原凭据并自动清除该账号的失效标记，无需重启服务（热加载会生效）。

---

## status

查看账号池状态，包含站点、套餐、冷却、额度、Token/刷新令牌到期时间与模型成本账本。

```bash
workbuddy-gateway status
```

输出示例：

```text
================== WorkBuddy 账号池状态 ==================
账号总数: 2

--- 账号 #1 ---
凭据文件:     workbuddy.json
站点:         国内站 (copilot.tencent.com)
用户昵称:     user-a
用户 UID:     uid-xxx
企业 ID:      (个人账号)
认证域名:     www.codebuddy.cn
刷新令牌到期: 2027-09-01 12:00:00 (剩余 8000h0m0s)
套餐:         pro
额度:         总额度 2300 / 已用 1200 / 剩余 1100
冷却状态:     可用
Token 状态:   有效
过期时间:     2026-09-22 12:32:07 (剩余 119h30m0s)
模型账本:     2 个模型（免费 1 / 收费 1）
```

> 套餐与刷新令牌到期时间来自上游权益/凭据；查询失败时保留上次结果并标记待刷新，不会误判成免费或已失效。

---

## refresh

立即刷新所有账号的 Access Token（正常情况下由后台每 5 分钟自动检查，无需手动执行）。

```bash
workbuddy-gateway refresh
```

- 成功 / 失败 / 跳过（授权失效）会分别统计。
- 单次刷新失败只累计失败次数，不立即删凭据；连续失败达到阈值才停止调度。
- 刷新成功后、覆盖旧凭据前，会校验新凭据的账号标识、站点与可用性，防止串号或用坏凭据覆盖好凭据。
- WAF 形态的 403 只软冷却，不删除凭据；普通 403 无法判定时保守保留。

---

## monitor

前台实时监控，周期刷新展示「账号表格 + 模型统计附表 + 最近日志」，`Ctrl+C` 退出。Web 控制台（`-webui`）提供更完整的图表与账号/模型管理；两者读取同一份状态快照，不互相替代。

```bash
# 必须在 serve 的工作目录执行（读取 workbuddy-status.json）
cd /opt/workbuddy-gateway
workbuddy-gateway monitor

# 附加展示 systemd 服务最近日志（Linux）
workbuddy-gateway monitor -journal workbuddy-gateway

# 附加展示指定日志文件
workbuddy-gateway monitor -logfile /var/log/workbuddy-gateway.log

# 调整刷新间隔与日志行数
workbuddy-gateway monitor -interval 2 -lines 20
```

| 选项 | 默认 | 说明 |
|---|---|---|
| `-interval <sec>` | `3` | 状态刷新间隔 |
| `-journal <svc>` | 空 | 同时展示 `journalctl -u <svc>` 最近日志 |
| `-logfile <path>` | 空 | 同时展示指定日志文件末尾内容 |
| `-lines <n>` | `15` | 每次展示的日志行数 |

**账号表格**

```text
账号池: 共 2 个 | 可用 1 | 冷却 0 | 付费耗尽 1 | 过期 0 | 失效 0
+------+----------------+------------+--------+------------+---------------------+----------+----------+--------+------------+------------+------------+
| 序号 | 凭据文件       | 账号       | 站点   | 状态       | Token 有效期        | 总额度   | 已用     | 剩余   | 付费用户   | 免费模型   | 模型冷却   |
+------+----------------+------------+--------+------------+---------------------+----------+----------+--------+------------+------------+------------+
| 1    | workbuddy.json | user-a     | 国内站 | 可用       | 2026-09-22 12:32:07 | 2300     | 1200     | 1100   | 否         | 1          | 0          |
| 2    | workbuddy2.json| user-b     | 国际站 | 付费耗尽   | 2027-09-05 01:57:00 | 1100     | 1100     | 0      | 否         | 0          | 0          |
+------+----------------+------------+--------+------------+---------------------+----------+----------+--------+------------+------------+------------+
```

状态取值：`可用`、`冷却`、`付费耗尽`、`已过期`、`失效`。

**模型统计附表**

```text
模型统计 (来源 live-api@2026-09-17 14:57):
+----------------------------+------------------+------------------+----------+----------+---------------+-----------------+
| 模型                       | 国内倍率         | 国际倍率         | 可用账号 | 请求     | 平均首字(5h)  | 平均总耗时(5h)  |
+----------------------------+------------------+------------------+----------+----------+---------------+-----------------+
| hy3                        | 0.00x            | 0.00x            | 8        | 3        | 1.9s          | 2.3s            |
| deepseek-v4.1-flash        | 0.03x            | 0.00x            | 5        | 12       | 820ms         | 3.4s            |
| hy4-preview                | 0.00x            | 收费(倍率未知)   | 8        | 4        | 1.3s          | 4.1s            |
+----------------------------+------------------+------------------+----------+----------+---------------+-----------------+
```

| 列 | 含义 |
|---|---|
| 模型 | 模型 ID |
| 国内倍率 | 国内站生效倍率（`credits` × 促销 factor）；免费显示 `0.00x`，促销过期或接口无有效倍率时显示 `-`，实测确认收费显示 `收费(倍率未知)` |
| 国际倍率 | 国际站同上 |
| 可用账号 | 当前可调度该模型的账号数（已计入账号冷却、模型冷却、模型额度阻断） |
| 请求 | 客户端请求次数 |
| 平均首字(5h) | 最近 5 小时滚动窗口内的平均首字响应时间（TTFT），按小时分桶、自动淘汰过期样本 |
| 平均总耗时(5h) | 最近 5 小时滚动窗口内的平均总耗时 |

---

## probe

免费 / 收费属性按「账号（含站点）+ 模型」学习，只有该账号真正请求过该模型才会写入账本。默认调度优先使用有余额账号，**余额耗尽的账号几乎不会被选中，也就学不到属性**。`probe` 用于主动补课。

> 后台自动价格探测默认关闭（`-allow-price-probes` 才开启），但 `probe` 是**显式主动探测**，仍会发起真实模型请求，可能消耗积分。
>
> 注意：额度耗尽的账号会被上游整体拒绝（`14018 Credits exhausted`），此时连免费模型也会失败。要验证某模型是否免费，请使用**额度未耗尽**的账号。

```bash
# 探测全部账号，每个账号取模型目录前 5 个模型
workbuddy-gateway probe

# 只探测指定账号
workbuddy-gateway probe -auth workbuddy4.json

# 指定模型
workbuddy-gateway probe -auth workbuddy4.json -models hy3,deepseek-v4.1-flash

# 指定数量上限（默认 5，上限 50）
workbuddy-gateway probe -auth workbuddy4.json -limit 8
```

| 选项 | 默认 | 说明 |
|---|---|---|
| `-auth <path>` | 全部账号 | 只探测指定凭据（文件名或路径均可） |
| `-models <m1,m2>` | 目录前几个 | 指定要探测的模型 |
| `-limit <n>` | `5` | 未指定 `-models` 时探测的模型数量，上限 50 |

输出示例：

```text
正在请求 http://127.0.0.1:8317/admin/probe（账号=workbuddy4.json，模型=hy3）...

账号                   站点   模型     结果     credit  tokens  说明
workbuddy4.json        intl   hy3      paid     0.42    820     usage.credit=0.42，收费

汇总: paid=1
```

结果状态：

| 状态 | 含义 |
|---|---|
| `free` | `usage.credit=0` 且 `total_tokens ≥ 100`，已学习为免费 |
| `paid` | `usage.credit > 0`，已学习为收费 |
| `unknown` | 未返回 `credit`，或 `credit=0` 但样本过小 |
| `quota` | `14018` 额度耗尽，记为该账号该模型收费并阻断该模型 |
| `rate_limited` | `6004` 模型级限流，只冷却该模型 |
| `auth_failed` | 授权失效（probe 不会自动禁用账号） |
| `skipped` | 账号失效或无凭据 |
| `error` | 网络 / 协议错误 |

> 原理：`probe` 作为客户端调用运行中服务的 `/admin/probe`。账本保存在 `serve` 进程内存中，独立进程直接写状态文件会被服务快照覆盖，因此探测必须由运行中的服务执行。该接口仅接受回环来源；开启模型 API 鉴权时需携带 `config.json` 的 `gateway.apiKey`。

---

## reset

清空**除登录凭据以外**的全部本地数据，并重新拉取模型与倍率。

```bash
workbuddy-gateway reset
```

清理范围：

- `workbuddy-status.json`（账号与模型状态快照）
- `stats.json`（用量与耗时统计）
- `wb-models-cache.json`（模型目录、倍率、价格探测结论）
- `*.disabled` / `*.json.disabled`（授权失效标记）
- `logs/`（运行日志、调试日志、构建/部署日志）

保留：`workbuddy*.json` 登录凭据、`config.json` 配置。

清理后会立即重新拉取模型目录与倍率。账号账本同时存在于 `serve` 进程内存中，若服务正在运行，请重启使其同步归零：

```bash
systemctl restart workbuddy-gateway
```

---

## version / help

```bash
workbuddy-gateway version    # 输出 WorkBuddy Local Gateway vX.Y.Z
workbuddy-gateway help       # 输出完整帮助
workbuddy-gateway -v         # 同 version
workbuddy-gateway -h         # 同 help
```

---

## 多账号池

三种配置方式：

```bash
# 方式一（推荐）：自动发现
# 把多个凭据文件放进工作目录，无需任何参数
workbuddy-gateway serve

# 方式二：-auth 逗号分隔
workbuddy-gateway serve -auth workbuddy.json,workbuddy2.json

# 方式三：-auth-dir 目录
workbuddy-gateway serve -auth-dir ./auths
```

行为说明：

- **轮询**：请求按 round-robin 在可用账号间分发。
- **429 冷却**：`6004` 只冷却触发模型；无法归因到模型的 429 才进入账号级冷却，冷却到期自动恢复。
- **授权失效**：401，或 403 携带业务信封且命中失效文案时，先停止调度并写 `*.disabled` 标记；删除凭据前做只读校验，确认不可用才删除，网络/5xx/普通 403 无法判定时保守保留。重新 `login` 后自动恢复。WAF 形态的 403（无业务信封）只软冷却，不删凭据。
- **额度耗尽**：`剩余=0` 标记「付费耗尽」，仍可服务已确认免费的模型；企业账号 `limitNum=-1` 显示「不限」，不按零余额处理。
- **热加载**：默认每 5 秒扫描，新增 / 更新 / 删除凭据免重启；Web 控制台也可扫码新增/删除账号。
- **主动保活**：`-keepalive-hours` 默认每天 22:00 刷新全部账号，避免刷新令牌静默过期。
- **串行化**：同一账号请求严格排队，避免并发双发触发风控；不同账号可并行。
- **模型/账号规则**：`config.json` 的 `models.blocklist` / `allowlist` 控制模型是否可调用；`models.accounts` 可按模型限制凭据文件，全局 `all` 与模型专属规则同时生效。

---

## 模型列表与倍率

**列表来源**：实时接口 `GET {Base}/v2/enterprises/personal/models` 与 npm 包静态目录，按模型 ID 去重、**接口优先**。

```text
两路都成功  → 合并去重
一路成功    → 使用成功那路
两路都失败  → 使用本地缓存 wb-models-cache.json
失败且无缓存→ 该站点本轮不展示模型（不影响模型调用）
```

**免费站点优先**：若某模型出现「一个站点免费、另一个站点收费」，调度优先使用免费站点的账号，直到该站点账号全部不可用（冷却 / 耗尽 / 失效）才回退到另一站点；若两个站点都免费或都收费（只是倍率不同），则不设优先，保持正常轮询。

**倍率**：

```text
1. 促销生效中：生效倍率 = credits × factor（factor=0 → 0.00x）
2. 促销已过期：接口 credits 不可信（上游常把促销价固化进 credits），
   探测出结果前显示 -，随后由实测决定
3. 模型不在接口目录中：同样交由实测决定
4. 无促销且 credits 有值：直接展示该倍率
```

**价格探测**：由「余额未耗尽」的同站点账号发一次最小请求实测。

> **默认关闭**：后台自动价格探测仅在显式传入 `-allow-price-probes` 时运行；默认情况下不会产生额外探测请求。正常客户端请求仍会从 `usage.credit` 学习免费/收费状态。

```text
探测免费 → 展示 0.00x，并每 12 小时复测确认
探测收费 → 展示 收费(倍率未知)，直到接口重新给出未过期的 0.00x
14018 / 无 usage.credit / 样本过小 → 不覆盖，保持未知
```

探测调度：

| 时机 | 说明 |
|---|---|
| 服务启动（仅显式开启时） | 启动后约 20 秒执行首轮 |
| 首次 / 重置后（仅显式开启时） | 单轮最多 30 个，快速补齐结论 |
| 收敛后 | 单轮最多 5 个，每模型 12 小时最多一次 |
| 待探测未清空 | 用 2 分钟短间隔追赶，清空后回到 30 分钟 |
| 目录刷新成功 | 立即触发一轮 |
| 凭据变化 | 立即触发一轮（含「原本没有某站点账号、后来加入」的情况） |

仅探测被实际请求过、或接口明确需要确认的模型，避免无谓消耗额度。

**模型能力字段**：`/v1/models` 会透出上游目录声明的 `context_length`、`max_output_tokens`、`default_context_length`、`context_length_options`、`capabilities_by_site`、`supports_images`、`supports_tool_call`、`reasoning_supported_efforts`、`reasoning_default_effort`。零值/未声明字段一律省略，不编造默认窗口。

**模型与账号规则**：`config.json` 可设置模型黑/白名单，被禁模型从 `/v1/models`、`/health` 的 `model_count` 与统计表隐藏；`models.accounts` 可按凭据文件名限制某模型的可用账号。请求命中禁用模型或无可用账号时返回 403，不消耗上游额度。

`/v1/models` 响应头 `X-Model-Source` 与 `/health` 的 `model_source` 会标注目录来源。

---

## 客户端接入

网关启动后的地址：

- OpenAI 兼容：`http://127.0.0.1:8317/v1`
- Anthropic：`http://127.0.0.1:8317`（Claude Code 会请求 `/v1/messages`）
- Web 控制台：`http://127.0.0.1:8316/ui/`（仅 `-webui` 时）

如果 `config.json` 开启了 `gateway.apiKeyEnabled`，客户端需携带对应的 `Authorization: Bearer <apiKey>`（Anthropic 客户端也可用 `x-api-key`）。未开启时任意 Key 均可。

curl：

```bash
curl -N -s http://127.0.0.1:8317/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"hy4-preview","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

Python OpenAI SDK：

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8317/v1", api_key="none")
resp = client.chat.completions.create(
    model="hy4-preview",
    messages=[{"role": "user", "content": "写一个快速排序"}],
)
print(resp.choices[0].message.content)
```

Claude Code（Anthropic 协议直连，免外部翻译层）：

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8317
export ANTHROPIC_API_KEY=你的模型APIKey   # config.json 未开启 apiKeyEnabled 时随意填
claude
```

网关在 `/v1/messages` 完成 Anthropic → Chat 双向转译：`thinking` 内容块、`tool_use` / `tool_result`、`stop_reason`（`tool_calls→tool_use`、`length→max_tokens`）都会映射；账号池轮询、工具序列自愈、11128 净化与 Chat 入口共用同一套逻辑。

Anthropic Messages curl：

```bash
curl -N -s http://127.0.0.1:8317/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: 你的模型APIKey" \
  -d '{"model":"hy4-preview","max_tokens":256,"stream":true,
       "messages":[{"role":"user","content":"你好"}]}'
```

DSH（`~/.dsh/settings.yaml`）：

```yaml
llm-pi-ai:
  providers:
    workbuddy-local:
      baseURL: http://127.0.0.1:8317/v1
      apiKeyEnv: LOCAL_API_KEY   # 任意字符串即可
      api: openai-completions
      models:
        - id: hy4-preview
          contextWindow: 1000000
          maxTokens: 128000
```

---

## 各平台部署

> **产物来源说明**：本仓库的 [build.ps1](build.ps1) 与 GitHub Actions（`.github/workflows/release.yml`）都支持 Windows / Linux / macOS 的 x86、x64、ARM、ARM64 完整矩阵，并生成 SHA256 清单/SHA256SUMS。
> 推送 `v*` tag 后，可直接从[本仓库 Releases](https://github.com/gakiyukr/workbuddy_to_api_plus/releases)下载对应平台产物；
> 也可以本地运行 `build.ps1`。

### 本地构建全部产物

```powershell
# 完整矩阵（Windows / Linux / macOS，输出到 dist/）
powershell -ExecutionPolicy Bypass -File .\build.ps1

# 只构建 Linux ARM64
powershell -ExecutionPolicy Bypass -File .\build.ps1 -Only linux -Architecture arm64

# 清理后重建，并跳过测试（不推荐）
powershell -ExecutionPolicy Bypass -File .\build.ps1 -Clean -SkipTests
```

脚本会先用当前 Go 工具链交叉编译（`CGO_ENABLED=0`），再生成 `SHA256SUMS` 与 `build-manifest.json`。产物命名与 Release 一致。

### Windows

1. 从[本仓库 Releases](https://github.com/gakiyukr/workbuddy_to_api_plus/releases) 下载 `workbuddy-gateway-windows-amd64.exe`（或 386 / arm64），或用本仓库 `build.ps1` 本地构建，或直接交叉编译：

   ```powershell
   $env:GOOS='windows'; $env:GOARCH='amd64'; $env:CGO_ENABLED='0'
   go build -trimpath -ldflags '-s -w' -o dist\workbuddy-gateway-windows-amd64.exe .
   ```

2. 在 PowerShell / CMD 中进入文件所在目录：

   ```powershell
   .\workbuddy-gateway-windows-amd64.exe login
   .\workbuddy-gateway-windows-amd64.exe serve -port 8317
   ```

3. 开机自启：`Win+R` → `shell:startup`，把 exe 快捷方式放入启动文件夹，并在快捷方式“目标”后追加 `serve`。

### Linux

从[本仓库 Releases](https://github.com/gakiyukr/workbuddy_to_api_plus/releases/latest) 下载（含 `.sha256` 校验文件）：

```bash
# x86_64
wget https://github.com/gakiyukr/workbuddy_to_api_plus/releases/latest/download/workbuddy-gateway-linux-amd64
wget https://github.com/gakiyukr/workbuddy_to_api_plus/releases/latest/download/workbuddy-gateway-linux-amd64.sha256
sha256sum -c workbuddy-gateway-linux-amd64.sha256
sudo install -m 755 workbuddy-gateway-linux-amd64 /usr/local/bin/workbuddy-gateway

# ARM64
wget https://github.com/gakiyukr/workbuddy_to_api_plus/releases/latest/download/workbuddy-gateway-linux-arm64
wget https://github.com/gakiyukr/workbuddy_to_api_plus/releases/latest/download/workbuddy-gateway-linux-arm64.sha256
sha256sum -c workbuddy-gateway-linux-arm64.sha256
sudo install -m 755 workbuddy-gateway-linux-arm64 /usr/local/bin/workbuddy-gateway

workbuddy-gateway login
workbuddy-gateway serve -addr 127.0.0.1 -port 8317
```

#### systemd 服务（推荐）

创建 `/etc/systemd/system/workbuddy-gateway.service`：

```ini
[Unit]
Description=WorkBuddy Local Gateway (CodeBuddy OpenAI-compatible proxy)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/workbuddy-gateway
ExecStart=/opt/workbuddy-gateway/workbuddy-gateway serve -addr 0.0.0.0 -port 8317
Restart=on-failure
RestartSec=5
User=root
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=false

[Install]
WantedBy=multi-user.target
```

部署与启动：

```bash
sudo mkdir -p /opt/workbuddy-gateway
sudo cp workbuddy-gateway /opt/workbuddy-gateway/
sudo /opt/workbuddy-gateway/workbuddy-gateway login
sudo systemctl daemon-reload
sudo systemctl enable --now workbuddy-gateway
sudo systemctl status workbuddy-gateway
sudo journalctl -u workbuddy-gateway -f
```

> `WorkingDirectory` 决定自动发现的凭据目录与 `config.json` 位置。把多个凭据文件放进该目录即可组成账号池，新增 / 更新 / 删除会自动热加载。
>
> 需要 Web 控制台时，在 `ExecStart` 追加 `-webui`，并在 `WorkingDirectory` 放置 `config.json`（配置 `gateway.webPort` / `gateway.adminKey`）。管理台默认监听 `127.0.0.1:8316`，不要把 8316 直接暴露到公网。

常用运维：

```bash
sudo systemctl restart workbuddy-gateway
sudo systemctl stop workbuddy-gateway
sudo systemctl disable workbuddy-gateway
```

对外开放时（例如局域网其他设备）把 `-addr` 改为 `0.0.0.0`，并**务必**在 `config.json` 的 `gateway` 段开启 `apiKeyEnabled` 并设置强 `apiKey`；旧的 `-api-key` 仍可作为一次性覆盖：

```ini
ExecStart=/opt/workbuddy-gateway/workbuddy-gateway serve -addr 0.0.0.0 -port 8317 -api-key sk-changeme
```

### macOS

1. 从[本仓库 Releases](https://github.com/gakiyukr/workbuddy_to_api_plus/releases) 下载 `workbuddy-gateway-darwin-arm64`（Apple Silicon）或 `workbuddy-gateway-darwin-amd64`（Intel）；也可用 `build.ps1` 或本地交叉编译：

   ```bash
   GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/workbuddy-gateway-darwin-arm64 .
   ```
2. 移除隔离属性：

   ```bash
   chmod +x workbuddy-gateway-darwin-arm64
   xattr -d com.apple.quarantine workbuddy-gateway-darwin-arm64 2>/dev/null || true
   ```

3. 登录与启动：

   ```bash
   ./workbuddy-gateway-darwin-arm64 login
   ./workbuddy-gateway-darwin-arm64 serve
   ```

4. 开机自启（launchd）：创建 `~/Library/LaunchAgents/com.workbuddy.gateway.plist`：

   ```xml
   <?xml version="1.0" encoding="UTF-8"?>
   <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
   <plist version="1.0">
   <dict>
     <key>Label</key><string>com.workbuddy.gateway</string>
     <key>ProgramArguments</key>
     <array>
       <string>/path/to/workbuddy-gateway-darwin-arm64</string>
       <string>serve</string>
       <string>-port</string><string>8317</string>
     </array>
     <key>RunAtLoad</key><true/>
     <key>KeepAlive</key><true/>
     <key>WorkingDirectory</key><string>/path/to/workbuddy-gateway-dir</string>
   </dict>
   </plist>
   ```

   ```bash
   launchctl load ~/Library/LaunchAgents/com.workbuddy.gateway.plist
   ```

---

## 安全提示

- `workbuddy*.json` 包含真实访问凭据（Access Token / Refresh Token），**严禁提交到 Git 或公开分享**；本仓库 `.gitignore` 已排除。
- `config.json` 含明文 `gateway.adminKey` / `gateway.apiKey`，同样已被 `.gitignore` 排除；请设置 `0600` 权限，不要提交或贴到公开渠道。
- 网关默认只监听 `127.0.0.1`。需要局域网 / 公网访问时改用 `-addr 0.0.0.0`，并在 `config.json` 开启 `apiKeyEnabled` 设置强 Key，或置于反向代理之后。
- Web 控制台是独立端口（默认 `8316`），默认只监听回环；对外访问必须走反向代理 + TLS，并设置强 `adminKey`。管理 API 使用 `Authorization: Bearer <adminKey>`，不接受模型 API Key。
- `/admin/probe` 仅接受回环来源调用。
- `debug.fullRequests=true` 会记录完整对话、工具参数与图片，即使凭据已脱敏仍属敏感数据；仅排障期间开启，并沿用 `logs.retentionDays` 清理。
- 不再需要某账号授权时，删除对应凭据文件并在 CodeBuddy 控制台撤销授权。

---

## 从源码构建

需要 Go 1.26.5+（`go.mod` 要求 1.26.5）：

```bash
git clone https://github.com/gakiyukr/workbuddy_to_api_plus.git
cd workbuddy_to_api_plus

go vet ./...
go test -count=1 ./...

# 当前平台
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o workbuddy-gateway .

# 需要 Web 控制台时，准备配置（可复制示例后修改 adminKey/apiKey）
cp config.example.json config.json

# 交叉编译示例
GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/workbuddy-gateway-linux-amd64 .
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/workbuddy-gateway-windows-amd64.exe .

# Windows 一键构建完整矩阵（PowerShell）
powershell -ExecutionPolicy Bypass -File .\build.ps1
```

> 编译产物**不依赖 tag**：`go build` 或 `build.ps1` 在任何提交上都能直接产出二进制。tag 只用于触发 GitHub Actions 发布 Release。

---

## 自动构建与发布

推送 `v*` 形式的 tag 时，GitHub Actions（`.github/workflows/release.yml`）会自动：

1. 运行 `go vet` + `go test`（不通过则中止，不发布坏产物）；
2. 交叉编译 9 个平台/架构组合（Windows/Linux/macOS 的 x86、x64、ARMv7、ARM64，`CGO_ENABLED=0`）；
3. 为每个产物生成 `.sha256`，并生成汇总 `SHA256SUMS`；
4. 创建 GitHub Release 并上传全部产物，下载即可用。

发布前会校验 tag 与 `main.go` 中的 `version` 常量一致，避免名实不符。当前 `main.go` 的版本是 `1.15.0`。

发版流程：

```bash
# 1. bump main.go 中的 version 常量（当前为 1.15.0）
# 2. 提交后打 tag 并推送
git commit -am "chore: bump version to 1.15.0"
git tag -a v1.15.0 -m "WorkBuddy Local Gateway v1.15.0"
git push origin main --follow-tags
```

---

## 免责声明

本项目仅用于个人学习与技术研究。腾讯 CodeBuddy（含国内站与国际站 workbuddy.ai）的接口协议与风控策略可能随时变化；请遵守腾讯服务条款，自行承担使用风险。本仓库不包含任何官方未公开的密钥或凭据。

本仓库是 [CangShui/workbuddy-gateway](https://github.com/CangShui/workbuddy-gateway) 的 fork，
并包含移植自 [Sliverkiss/workbuddy2api](https://github.com/Sliverkiss/workbuddy2api)（MIT 许可）的代码，
来源与许可详情见[代码来源与许可](#代码来源与许可)。
