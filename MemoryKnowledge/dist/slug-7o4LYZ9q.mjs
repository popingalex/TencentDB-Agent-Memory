//#region src/engines/wiki/ingest-v2/slug.ts
/**
* slug.ts — 实体名 → 稳定文件名 slug
*
* dedup（去重合并）的基础：同一实体名两次摄取必须产出同一 slug，
* 这样第二次摄取能命中已存在的 `wiki/<type>/<slug>.md` 走合并而非新建。
*
* 规则（PRD FR-4 / INTERFACE §2，源中英混合）：
*   - 英文/数字：转小写，空格与标点转连字符，去首尾/重复连字符。
*   - 中文（CJK）：保留汉字本身，只去空格（不转拼音、不丢弃）。
*   - 混合：分别按上面规则处理后拼接（`Redis 主从` → `redis-主从`）。
*/
const CJK_RE = /[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]/;
function isCjkChar(ch) {
	return CJK_RE.test(ch);
}
function isAlnumChar(ch) {
	return /[a-zA-Z0-9]/.test(ch);
}
/**
* 把实体名/标题归一化为稳定 slug。
*
* 实现策略：逐字符扫描，分成「CJK 段」与「拉丁/数字段」交替的 token，
* 段之间用连字符连接；拉丁段转小写；其余字符（空格、标点）都视作段边界。
*/
function slugify(input) {
	const trimmed = (input ?? "").trim();
	if (!trimmed) return "";
	const tokens = [];
	let buf = "";
	let bufKind = null;
	const flush = () => {
		if (buf) {
			tokens.push(bufKind === "latin" ? buf.toLowerCase() : buf);
			buf = "";
		}
		bufKind = null;
	};
	for (const ch of trimmed) if (isCjkChar(ch)) {
		if (bufKind !== "cjk") flush();
		bufKind = "cjk";
		buf += ch;
	} else if (isAlnumChar(ch)) {
		if (bufKind !== "latin") flush();
		bufKind = "latin";
		buf += ch;
	} else flush();
	flush();
	return tokens.join("-").replace(/-+/g, "-").replace(/^-|-$/g, "");
}
/**
* 给定页面类型与标题，返回 wiki 内的相对路径（不含前导 `wiki/`）。
* 例如 type="entity", title="Redis Cluster" → "entities/redis-cluster.md"
*
* type → 目录的映射对齐现有 manager 的目录约定（entities/concepts/sources/...）。
*/
const TYPE_DIR = {
	source: "sources",
	entity: "entities",
	concept: "concepts",
	comparison: "comparisons",
	synthesis: "synthesis",
	thesis: "synthesis",
	methodology: "concepts",
	finding: "synthesis"
};
/** 把 type 映射到目录名；未知 type 退到以 type 复数化的 generic 目录。 */
function dirForType(type) {
	const key = (type ?? "").trim().toLowerCase();
	return TYPE_DIR[key] ?? `${key || "other"}`;
}
//#endregion
export { slugify as n, dirForType as t };
