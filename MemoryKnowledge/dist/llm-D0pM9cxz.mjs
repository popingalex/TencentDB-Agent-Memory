import { t as createLogger } from "./logger-CcNfQhS0.mjs";
import { generateText, streamText } from "ai";
import { createOpenAI } from "@ai-sdk/openai";
import { createAnthropic } from "@ai-sdk/anthropic";
//#region src/engines/wiki/ingest-v2/llm.ts
/**
* llm.ts — OpenAI 兼容 chat 调用封装（wiki ingest 专用）。
*
* 复用仓库已有的 Vercel AI SDK（`ai` + `@ai-sdk/openai`），走标准
* `/chat/completions`（compatibility: "compatible"），兼容各类 OpenAI 兼容后端。
*
* llmConfig 的实际形状由上层 module.ts 传入，字段命名为：
*   { provider, apiKey, model, customEndpoint, maxContextSize }
* 这里做归一化以兼容 INTERFACE 文档里写的 { baseUrl, maxTokens, timeoutMs } 别名。
*/
const log = createLogger("wiki-ingest-llm");
const DEFAULT_MODEL = "Memory-Model";
const DEFAULT_MAX_TOKENS = 8192;
const DEFAULT_TIMEOUT_MS = 12e5;
/**
* 把上层多种命名的 config 归一化。
*
* 注意：这里**不再**兜底读 process.env（历史上读 TDAI_LLM_*，会绕过 resolveLlmConfig
* 的 binding/mode 逻辑，造成"偷偷掉回直连"）。baseUrl/apiKey 必须由上层
* （module.ts → resolveLlmConfig）提供；缺失时 createLlmClient 直接抛错。
*/
function normalizeLlmConfig(raw) {
	const cfg = raw ?? {};
	return {
		protocol: cfg.protocol ?? "openai",
		baseUrl: cfg.baseUrl || cfg.customEndpoint || "",
		apiKey: cfg.apiKey || "",
		model: cfg.model || DEFAULT_MODEL,
		maxTokens: cfg.maxTokens ?? cfg.maxContextSize ?? DEFAULT_MAX_TOKENS,
		timeoutMs: cfg.timeoutMs ?? DEFAULT_TIMEOUT_MS,
		stream: cfg.stream ?? false
	};
}
/**
* 基于 AI SDK 的真实客户端。纯文本输出（不挂任何 tool，避免弱模型幻觉 tool call）。
*/
function createLlmClient(raw) {
	const config = normalizeLlmConfig(raw);
	if (!config.apiKey) throw new Error("LLM apiKey 未配置：proxy 模式需 TMC 为该 service_id 推送 llm_binding；或设 LLM_MODE=custom + LLM_API_KEY 走自带端点");
	if (!config.baseUrl) throw new Error("LLM baseUrl 未配置：proxy 模式需 TMC 为该 service_id 推送 llm_binding；或设 LLM_MODE=custom + LLM_BASE_URL 走自带端点");
	const provider = config.protocol === "anthropic" ? createAnthropic({
		baseURL: config.baseUrl,
		apiKey: config.apiKey
	}) : createOpenAI({
		baseURL: config.baseUrl,
		apiKey: config.apiKey
	});
	return {
		config,
		async chat(params) {
			const timeoutSignal = AbortSignal.timeout(config.timeoutMs);
			const signal = params.abortSignal ? AbortSignal.any([timeoutSignal, params.abortSignal]) : timeoutSignal;
			const label = params.label ?? "chat";
			const promptChars = params.system.length + params.prompt.length;
			const startMs = Date.now();
			log.info(`LLM 调用开始 [${label}]`, {
				model: config.model,
				protocol: config.protocol,
				promptChars,
				maxOutputTokens: params.maxOutputTokens ?? config.maxTokens,
				timeoutMs: config.timeoutMs
			});
			log.debug(`LLM system prompt [${label}] (model=${config.model})`, { text: params.system.slice(0, 200) });
			log.debug(`LLM user prompt [${label}] (model=${config.model})`, { text: params.prompt.slice(0, 500) });
			try {
				const callParams = {
					model: provider.chat(config.model),
					system: params.system,
					prompt: params.prompt,
					maxOutputTokens: params.maxOutputTokens ?? config.maxTokens,
					...params.temperature !== void 0 ? { temperature: params.temperature } : {},
					abortSignal: signal,
					experimental_telemetry: {
						isEnabled: true,
						functionId: params.label ?? "chat"
					}
				};
				const { text, usage, finishReason } = config.stream ? await (async () => {
					const r = streamText(callParams);
					return {
						text: (await r.text ?? "").trim(),
						usage: await r.usage,
						finishReason: await r.finishReason
					};
				})() : await (async () => {
					const r = await generateText(callParams);
					return {
						text: (r.text ?? "").trim(),
						usage: r.usage,
						finishReason: r.finishReason
					};
				})();
				const u = usage ?? {};
				log.info(`LLM 调用完成 [${label}]`, {
					ms: Date.now() - startMs,
					promptTokens: u.inputTokens ?? null,
					completionTokens: u.outputTokens ?? null,
					totalTokens: u.totalTokens ?? null,
					finishReason: finishReason ?? null,
					outputChars: text.length
				});
				if (!text) log.warn(`LLM 返回空文本 [${label}]`, { finishReason: finishReason ?? null });
				return text;
			} catch (err) {
				log.error(`LLM 调用失败 [${label}]`, {
					ms: Date.now() - startMs,
					error: err instanceof Error ? err.message : String(err)
				});
				throw err;
			}
		}
	};
}
//#endregion
export { normalizeLlmConfig as n, createLlmClient as t };
