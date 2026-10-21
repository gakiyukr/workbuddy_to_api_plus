package main

import "net/http"

// writeUpstreamError 是 upstreamChat / 鉴权中间件的统一错误出口（OpenAI 形状）。
// Anthropic Messages 端点已移除，网关只提供 OpenAI 兼容协议。
func writeUpstreamError(w http.ResponseWriter, r *http.Request, statusCode int, errType, message string) {
	writeOpenAIError(w, statusCode, errType, message)
}
