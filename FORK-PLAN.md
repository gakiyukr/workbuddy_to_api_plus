# WorkBuddy Gateway 二開計劃

以 [CangShui/workbuddy-gateway](https://github.com/CangShui/workbuddy-gateway) 為基線，
移植 [Sliverkiss/workbuddy2api](https://github.com/Sliverkiss/workbuddy2api) 的優點。

## 背景與動機

實測結論（見下方「決策依據」）：

- **wb2api 更容易觸發 WAF**：單號在途 3 並發 × `MaxRotate=3` 輪轉放大 × 無出口代理，
  三者疊乘。作者自己在 issue #115 承認「同一 IP 約 1 秒內 3 次請求即有很大概率被 403 攔截」，
  且既有四層緩解「治標不治本」。
- **wbgw 抗 WAF 來自架構**：單帳號嚴格串行（`acc.lock` 包住整個上游調用），
  每請求恰好 1 次上游調用，請求間隔由響應耗時自然形成。
- **但 wbgw 有致命缺陷**：`isAuthFailure` 對任何 403 都返回 true，
  然後 `disableAccount` 執行 `os.Remove(path)` **刪除憑證文件**。
  WAF 攔截頁（HTML/空體 403）會被誤判為授權失效，直接刪掉有效期到 2027 年的憑證。

二開目標：**保留 wbgw 的串行調度與部署簡潔性，補上 wb2api 的錯誤分類體系與安全網。**

## 基線狀態

```
本地：C:/Projects/Chat/workbuddy-gateway   (upstream v1.12.0, 完整 git 歷史)
伺服器：154.88.65.226:2333 /opt/workbuddy-gateway  (v1.11.0 運行中, systemd)
帳號：workbuddy-intl3.json (melissapatterson46211, uid de7ef5e9)
      workbuddy-intl4.json (emilyhunt43072,       uid 3d011565)
      兩個都是國際站 (www.workbuddy.ai) —— WAF 最嚴重的域
```

| 檔案 | 行數 | 職責 |
|---|---|---|
| `main.go` | 3440 | 核心：CLI、帳號池、上游調用、HTTP 端點 |
| `models.go` | 1050 | 模型目錄（實時接口 + npm 靜態，雙來源合併） |
| `responses.go` | 707 | `/v1/responses` (Responses API) |
| `modelstats.go` | 450 | 模型統計（TTFB / 耗時 5h 滾動窗） |
| `probe.go` | 358 | 主動探測免費/收費屬性 |
| `reset.go` | 94 | 清理本地數據 |

依賴極簡：`go-qrcode` + `google/uuid`，零 CGO，單二進位。

## 現有能力缺口（已實證）

| 能力 | wbgw | wb2api | 嚴重度 |
|---|---|---|---|
| WAF 403 分類 | **無**（誤判為授權失效 → 刪憑證） | `ErrWafBlock` + `IsWafBlocked()` | **P0** |
| 錯誤分類體系 | 5 個 `is*` 函數 | 12 類 `ErrKind` 枚舉 | P1 |
| 出口代理 | `-proxy` 支持 | **無**（issue #115 未實現） | — |
| 模型倍率探測 | 2565 行（`models.go`+`probe.go`） | 722 行 | — |
| Responses API | `/v1/responses` | 無 | — |
| 憑證熱載入 | 5 秒掃描 | 需重啟容器 | — |
| 指紋脫敏 | 無 | `sanitize_blacklist_fingerprints` | P2 |
| 會話頭族 | 無 | `X-Conversation-Request-ID` 等 | P3 |
| 積分帳本 | 無 | 成本分層 + 三因子加權 | P3 |
| 定時任務 | 簽到 + token 續期 | 六類（含活躍地圖連發） | 不移植 |

## 分階段實作

### Phase 1 — 修 P0 缺陷（不移植，直接修 bug）

**1.1 拆分 403 語義**

`isAuthFailure(statusCode, body)` 當前：

```go
if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
    return true      // ← WAF 攔截頁也命中
}
```

改為引入 `isWafBlocked(status, body)`，移植 wb2api 的判定口徑：

```go
// 403 且 body 無業務信封（無 `"code":` / `"msg":`）→ WAF 攔截形態
// （HTML 攔截頁、空體、純文本均命中）
func hasBusinessEnvelope(body string) bool {
    return strings.Contains(body, `"code":`) || strings.Contains(body, `"msg":`)
}
func isWafBlocked(status int, body string) bool {
    return status == http.StatusForbidden && !hasBusinessEnvelope(body)
}
```

**1.2 WAF 分支的行為**

不 `disableAccount`、不刪憑證。改為帳號級軟冷卻（60s 起，與既有 `markCooldown` 復用），
日誌打 WARN 標明 WAF 而非授權失效。

**1.3 保留真正的授權失效路徑**

401、或 403 帶業務信封且含 `invalid token` / `登录已过期` 等文案 → 維持現行
`disableAccount`（這是正確行為）。

**1.4 修正固化了 bug 的測試**

`main_test.go:482` 的 `TestIsAuthFailure` 斷言了錯誤行為：

```go
{403, "{}", true},      // ← 空 body 的 403 正是 WAF 攔截頁形態
```

這個用例必須改為 `false`，並補上 WAF 形態的用例。
**這不是「調整測試以適配新代碼」，而是刪除一個把缺陷固化的斷言。**

**1.5 檢查另外兩個調用點**

`isAuthFailure` 有三處調用，需逐一評估：

| 位置 | 場景 | 處理 |
|---|---|---|
| `main.go:2783` | chat 上游響應 | WAF 會命中此處 → 必須加 `isWafBlocked` 前置判斷 |
| `main.go:1323` | token 刷新失敗 | refresh 端點，403 更可能是真失效，但同樣需判斷 |
| `probe.go:230` | probe 命令 | 同上，且 probe 明確「不會自動禁用帳號」 |

**驗收**：單元測試覆蓋四種輸入 —— 403+HTML（WAF）、403+空體（WAF）、
403+`{"code":11140}`（業務，不刪）、401（授權失效，刪）。

### Phase 2 — 錯誤分類體系（移植）

把 wbgw 的 5 個 `is*` 函數升級為 wb2api 的 `ErrKind` 枚舉（12 類）。
按調度決策分組，不必逐字移植全部：

| 分類 | 調度行為 | 現狀 |
|---|---|---|
| `waf_block` | 軟冷卻 60s，不刪憑證 | Phase 1 |
| `session_dead` | 禁用（真授權失效） | 已有 |
| `hard_credit` | 硬冷卻至次日 | `isQuotaExhausted` |
| `soft_rate` | 對齊上游重置時間 | `isRateLimited` |
| `model_blocked` | 只冷卻該模型 | `isModelRateLimited` |
| `content_blocked` | 不罰帳號，降級重試 | **缺** |
| `prompt_too_long` | 不罰帳號，直接透傳 | **缺** |
| `bad_params` | 不罰帳號，仍輪轉 | **缺** |

關鍵收益是**區分「帳號的問題」與「請求的問題」**——當前 wbgw 對所有 4xx
一律走 `recordModelFailure` + `writeOpenAIError`，會把請求級錯誤記到帳號頭上。

### Phase 3 — 指紋與請求改寫（按需）

- `sanitize_blacklist_fingerprints`：出站 body 清洗
- 會話頭族注入（`X-Conversation-Request-ID` 聚合主鍵）
- 內容攔截降級重試（`prompt.mode`）

這三項在 wbgw 當前架構下優先級最低——wbgw 已經不觸發 WAF，
這些是 wb2api 為了**壓制自己造成的問題**而加的。

### Phase 4 — 可選增強

- 積分帳本 / 成本分層（與抗 WAF 無關，純積分最優化）
- 多代理池 + 帳號綁定（wbgw 已有單代理，可擴展）

## 不做的事

- **不移植六類定時任務**：活躍地圖「每號連發 5 條」本身就是風控特徵
- **不移植並發調度**：wbgw 的串行是抗 WAF 的核心，不可動搖
- **不改 `MaxRotate` 語義**：wbgw 的換號上限 = 池大小且不放大單請求

## 驗證方式

| 層級 | 方法 |
|---|---|
| 單元 | `go test ./...`（現有 8519 行含 1900+ 行測試） |
| 建置 | `go build -trimpath -ldflags="-s -w"` 交叉編譯 linux/amd64 |
| 整合 | 部署到 154.88.65.226，`systemctl restart workbuddy-gateway` |
| 觀測 | WAF 計數 `grep -c "WAF Block Page" logs/*.log`；對照 wb2api 的 680 次 |
| 回歸 | 並發測試確認串行語義未被破壞 |

## 決策依據（實測記錄）

**wb2api 卸載前實測**（2026-09-17）：

- WAF 命中：680 次（日誌累計）
- 典型模式：單帳號 `emilyhunt43072` 每 2-5 秒被攔一次
- `waf ip-level block: 2 accounts hit waf 403 within 1m0s` — IP 級攔截觸發
- 配置改為 `max_in_flight=1` + 關閉活躍上報後：**WAF 歸零**
- 但代價：4 並發請求中 2 個直接 503（`no_healthy_account`）
  —— wb2api 是**拒絕**超出容量的請求，wbgw 是**排隊**

**伺服器現狀**：

- wbgw v1.11.0 運行中，2 帳號可用，Token 有效期至 2027
- wb2api 已卸載（容器/映像/目錄全清），備份在 `/root/backups/wb2api-uninstall.tar.gz`
