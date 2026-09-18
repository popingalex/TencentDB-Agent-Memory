import { t as createLlmClient } from "./llm-D0pM9cxz.mjs";
//#region src/callback.ts
/**
* Generate wiki summary via LLM.
* Reads page titles + descriptions, asks LLM for a ≤100 char Chinese summary.
* 复用 createLlmClient（自动走正确协议 openai/anthropic + Langfuse 追踪 + 超时处理）。
*/
const TAG = "[callback]";
const RETRY_DELAY_MS = 1e3;
/**
* Send status callback to TMC.
* Failures are logged but never thrown — this runs in async task paths.
*/
async function callbackTMC(payload, config) {
	if (!config.tmcCallbackUrl) return;
	const url = `${config.tmcCallbackUrl.replace(/\/$/, "")}/api/v1/knowledge/status-callback`;
	const body = JSON.stringify(payload);
	for (let attempt = 0; attempt < 2; attempt++) {
		try {
			const resp = await fetch(url, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body,
				signal: AbortSignal.timeout(5e3)
			});
			if (resp.ok) return;
			const respText = await resp.text().catch(() => "(unreadable)");
			console.warn(`${TAG} TMC callback HTTP ${resp.status} for ${payload.knowledge_id} (attempt ${attempt + 1}): ${respText.slice(0, 500)}`);
		} catch (err) {
			console.warn(`${TAG} TMC callback failed for ${payload.knowledge_id} (attempt ${attempt + 1}/2):`, err);
		}
		if (attempt === 0) await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
	}
	console.error(`${TAG} TMC callback gave up after 2 attempts for ${payload.knowledge_id} (type=${payload.type}, status=${payload.status})`);
}
/**
* Fire-and-forget progress callback during wiki ingest.
* Failures are logged as warn only — never block the ingest pipeline.
*/
function sendProgressCallback(tmcCallbackUrl, payload) {
	if (!tmcCallbackUrl) return;
	const url = `${tmcCallbackUrl.replace(/\/$/, "")}/api/v1/knowledge/status-callback`;
	fetch(url, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify(payload),
		signal: AbortSignal.timeout(5e3)
	}).catch((err) => {
		console.warn(`${TAG} progress callback failed for ${payload.wiki_id}:`, err);
	});
}
/** Build an onProgress fn that POSTs ingest_progress to TMC/Panel. */
function buildProgressFn(tmcCallbackUrl, wikiId, serviceId, teamId, runId) {
	return (progress) => {
		sendProgressCallback(tmcCallbackUrl, {
			wiki_id: wikiId,
			service_id: serviceId,
			team_id: teamId,
			event: "ingest_progress",
			progress,
			...runId ? { run_id: runId } : {}
		});
	};
}
async function generateWikiSummary(wikiId, name, pages, llm) {
	if (pages.length === 0) {
		console.warn(`${TAG} wiki summary skipped: no pages for ${wikiId}`);
		return "";
	}
	const prompt = `请为以下知识库生成一个不超过100字的中文摘要，描述它的主要内容和用途。只输出摘要文本，不要输出其他内容。

知识库名称：${name}
包含的页面：
${pages.slice(0, 20).map((p) => `- ${p.title}${p.description ? `: ${p.description.slice(0, 80)}` : ""}`).join("\n")}`;
	console.info(`${TAG} wiki summary LLM call start for ${wikiId} (model=${llm.model}, protocol=${llm.protocol}, pages=${pages.length})`);
	try {
		const result = (await createLlmClient(llm).chat({
			system: "你是一个知识库摘要生成器。只输出摘要文本，不要输出其他内容。",
			prompt,
			maxOutputTokens: 1024,
			temperature: .3,
			label: `wiki-summary`
		})).slice(0, 256);
		console.info(`${TAG} wiki summary LLM call done for ${wikiId} (len=${result.length}, empty=${result.length === 0})`);
		return result;
	} catch (err) {
		console.error(`${TAG} wiki summary generation failed for ${wikiId}:`, err);
		return "";
	}
}
/**
* Generate code-graph summary via template (no LLM call).
* Format: "{repo_name}（{branch}）- {files} 个文件、{nodes} 个符号节点"
*/
function generateCodeGraphSummary(repoName, branch, stats) {
	if (!stats) return `${repoName}（${branch}）`;
	return `${repoName}（${branch}）- ${stats.files} 个文件、${stats.nodes} 个符号节点`.slice(0, 256);
}
//#endregion
export { sendProgressCallback as a, generateWikiSummary as i, callbackTMC as n, generateCodeGraphSummary as r, buildProgressFn as t };
