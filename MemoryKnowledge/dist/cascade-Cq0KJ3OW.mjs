import { n as slugify } from "./slug-7o4LYZ9q.mjs";
import { n as parseFrontmatter, t as buildPage } from "./frontmatter-Ce2kZn6q.mjs";
import { existsSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { basename, join, relative } from "node:path";
//#region src/engines/wiki/ingest-v2/cascade.ts
/**
* cascade.ts — 删除级联（raw/rm 与 page/rm 的下游清理）。
*
* 行为契约见 PRD §3.7-3 与 wiki-service.ts 的调用签名。
*
*  - deleteSourceFiles：删 raw 源文件，并按各 page 的 frontmatter `sources` 级联——
*      独占该源的 page → 删除；共享的 page → 重写去掉该源。
*  - cascadeDeleteWikiPagesWithRefs：删 wiki page 文件，并清理其它 page 正文中
*      指向已删页的 [[wikilink]]（悬空链接）。
*/
/** 递归收集 wiki/ 下所有 .md 页的绝对路径（跳过 media 目录）。 */
function collectWikiPages(wikiDir) {
	const out = [];
	const walk = (dir) => {
		let entries;
		try {
			entries = readdirSync(dir);
		} catch {
			return;
		}
		for (const entry of entries) {
			const full = join(dir, entry);
			let st;
			try {
				st = statSync(full);
			} catch {
				continue;
			}
			if (st.isDirectory()) {
				if (entry !== "media") walk(full);
			} else if (entry.endsWith(".md")) out.push(full);
		}
	};
	if (existsSync(wikiDir)) walk(wikiDir);
	return out;
}
/** 结构性文件不参与级联删除/重写。 */
function isStructural(relFromWiki) {
	return relFromWiki === "index.md" || relFromWiki === "schema.md" || relFromWiki === "purpose.md";
}
/**
* 删除 raw 源文件并级联清理引用它们的 wiki page。
*
* @param projectPath wiki 项目根
* @param sourceFullPaths 要删除的 raw 源文件绝对路径列表
*/
async function deleteSourceFiles(projectPath, sourceFullPaths, _opts = {}) {
	const deletedNames = /* @__PURE__ */ new Set();
	for (const p of sourceFullPaths) {
		deletedNames.add(basename(p));
		try {
			if (existsSync(p)) rmSync(p, { force: true });
		} catch {}
	}
	const wikiDir = join(projectPath, "wiki");
	const deletedWikiPaths = [];
	let rewrittenSourcePages = 0;
	for (const pagePath of collectWikiPages(wikiDir)) {
		if (isStructural(relative(wikiDir, pagePath).replace(/\\/g, "/"))) continue;
		let content;
		try {
			content = readFileSync(pagePath, "utf-8");
		} catch {
			continue;
		}
		const parsed = parseFrontmatter(content);
		const sources = Array.isArray(parsed.frontmatter.sources) ? parsed.frontmatter.sources.filter((x) => typeof x === "string") : [];
		if (sources.length === 0) continue;
		const remaining = sources.filter((s) => !deletedNames.has(s));
		if (remaining.length === sources.length) continue;
		if (remaining.length === 0) try {
			rmSync(pagePath, { force: true });
			deletedWikiPaths.push(pagePath);
		} catch {}
		else try {
			writeFileSync(pagePath, buildPage({
				...parsed.frontmatter,
				sources: remaining
			}, parsed.body), "utf-8");
			rewrittenSourcePages++;
		} catch {}
	}
	return {
		deletedWikiPaths,
		rewrittenSourcePages
	};
}
/** 从一个页路径与内容推导出它可能被 [[wikilink]] 引用的标识符（小写归一）。 */
function linkAliasesFor(pagePath, content) {
	const aliases = /* @__PURE__ */ new Set();
	const base = basename(pagePath, ".md");
	aliases.add(base.toLowerCase());
	aliases.add(slugify(base).toLowerCase());
	const { frontmatter } = parseFrontmatter(content);
	if (typeof frontmatter.title === "string" && frontmatter.title.trim()) {
		aliases.add(frontmatter.title.trim().toLowerCase());
		aliases.add(slugify(frontmatter.title).toLowerCase());
	}
	return aliases;
}
/** 归一化一个 wikilink 目标（去 |label、trim、小写）。 */
function normalizeLinkTarget(raw) {
	return raw.split("|")[0].trim().toLowerCase();
}
/**
* 删除 wiki page 文件，并清理其它 page 正文中指向已删页的 [[wikilink]]。
*
* @param projectPath wiki 项目根
* @param pageFullPaths 要删除的 wiki page 绝对路径列表
*/
async function cascadeDeleteWikiPagesWithRefs(projectPath, pageFullPaths) {
	const wikiDir = join(projectPath, "wiki");
	const deletedAliases = /* @__PURE__ */ new Set();
	const toDelete = new Set(pageFullPaths.map((p) => p));
	for (const p of pageFullPaths) {
		let content = "";
		try {
			content = readFileSync(p, "utf-8");
		} catch {}
		for (const a of linkAliasesFor(p, content)) deletedAliases.add(a);
	}
	const deletedPaths = [];
	for (const p of pageFullPaths) try {
		if (existsSync(p)) {
			rmSync(p, { force: true });
			deletedPaths.push(p);
		}
	} catch {}
	let rewrittenFiles = 0;
	const linkRe = /\[\[([^\]]+?)\]\]/g;
	for (const pagePath of collectWikiPages(wikiDir)) {
		if (toDelete.has(pagePath)) continue;
		let content;
		try {
			content = readFileSync(pagePath, "utf-8");
		} catch {
			continue;
		}
		let changed = false;
		const next = content.replace(linkRe, (whole, inner) => {
			const target = normalizeLinkTarget(inner);
			if (deletedAliases.has(target)) {
				changed = true;
				const parts = String(inner).split("|");
				return (parts[1] ?? parts[0]).trim();
			}
			return whole;
		});
		if (changed) try {
			writeFileSync(pagePath, next, "utf-8");
			rewrittenFiles++;
		} catch {}
	}
	return {
		deletedPaths,
		rewrittenFiles
	};
}
//#endregion
export { cascadeDeleteWikiPagesWithRefs, deleteSourceFiles };
