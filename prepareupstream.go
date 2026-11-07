package main

import "net/http"

// prepareUpstreamChatRequest 是 Chat / Responses 两个入口共用的 identity 请求侧
// 替换管道：首位通配只跑一次客户端原始块，systemprompt 注入后再跑一遍字面规则。
// 与上游同名函数相比，本 fork 未引入出站分流 profile（official 分支）与
// 并发控制段，故去掉相关调用；其余顺序与上游保持一致。
func prepareUpstreamChatRequest(obj map[string]any, r *http.Request, requestID uint64, model string) {
	trace := debugTraceID(r)
	normalizeDeveloperRoles(obj, r, requestID)
	applyOriginalIdentityReplacements(obj, r, requestID)
	prepareSystemPromptForUpstream(obj, r, requestID, trace)
	applyIdentityRulePass(obj, r, requestID, false)
	_ = model // 与上游签名保持一致；fork 的 thinking/max_tokens 归一化仍在各入口散装执行
}
