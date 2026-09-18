import { t as createLogger } from "./logger-CcNfQhS0.mjs";
import { n as slugify } from "./slug-7o4LYZ9q.mjs";
import { n as DEFAULT_SCHEMA, t as DEFAULT_PURPOSE } from "./template-BGLLCFYb.mjs";
import { t as buildProgressFn } from "./callback-wlLfYQlz.mjs";
import { createRequire } from "node:module";
import "dotenv/config";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { LangfuseSpanProcessor } from "@langfuse/otel";
import { trace } from "@opentelemetry/api";
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { swaggerUI } from "@hono/swagger-ui";
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, normalize, resolve } from "node:path";
import { homedir } from "node:os";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { index, integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { and, desc, eq, isNull, sql } from "drizzle-orm";
import pLimit from "p-limit";
import { createHash, randomInt, randomUUID } from "node:crypto";
import { LRUCache } from "lru-cache";
import { existsSync as existsSync$1, mkdirSync as mkdirSync$1, readFileSync as readFileSync$1, readdirSync as readdirSync$1, statSync as statSync$1, writeFileSync as writeFileSync$1 } from "fs";
import { basename as basename$1, join as join$1, relative as relative$1 } from "path";
import Graph from "graphology";
import { pathToFileURL } from "url";
import simpleGit, { CleanOptions, ResetMode } from "simple-git";
//#region \0rolldown/runtime.js
var __defProp = Object.defineProperty;
var __exportAll = (all, no_symbols) => {
	let target = {};
	for (var name in all) __defProp(target, name, {
		get: all[name],
		enumerable: true
	});
	if (!no_symbols) __defProp(target, Symbol.toStringTag, { value: "Module" });
	return target;
};
//#endregion
//#region src/telemetry.ts
const log$8 = createLogger("telemetry");
let sdk = null;
/** 初始化 OpenTelemetry + Langfuse。未配置 key 时静默跳过。 */
function initTelemetry() {
	if (sdk) return;
	if (!process.env.LANGFUSE_SECRET_KEY) {
		log$8.info("Langfuse telemetry disabled (LANGFUSE_SECRET_KEY not set)");
		return;
	}
	try {
		sdk = new NodeSDK({ spanProcessors: [new LangfuseSpanProcessor({ shouldExportSpan: () => true })] });
		sdk.start();
		log$8.info("Langfuse telemetry initialized", { baseUrl: process.env.LANGFUSE_BASE_URL ?? "https://cloud.langfuse.com" });
	} catch (err) {
		log$8.warn("Langfuse telemetry init failed", { error: err instanceof Error ? err.message : String(err) });
		sdk = null;
	}
	process.on("SIGTERM", () => {
		sdk?.shutdown().catch(() => {});
	});
}
/** 共享 tracer，供 ingest 流程创建 parent span。 */
const tracer = trace.getTracer("knowledge-wiki");
/**
* 在 span 上下文中执行异步函数。AI SDK 的 experimental_telemetry 会自动
* 将 generateText 的 span 归并为当前 active span 的子 span。
*
* 用法：
*   const result = await withSpan("wiki-ingest", async (span) => {
*     span.setAttribute("wiki.name", name);
*     return runIngest(...);
*   });
*/
async function withSpan(name, fn) {
	return tracer.startActiveSpan(name, async (span) => {
		span.setAttribute("langfuse.name", name);
		try {
			return await fn(span);
		} finally {
			span.end();
		}
	});
}
//#endregion
//#region src/config.ts
/**
* Service configuration — environment variables + config loading.
*
* .env is auto-loaded via dotenv on import of this module.
* All config is loaded from env vars with sensible defaults.
* LLM config can also be passed explicitly to createKnowledgeModule.
*/
/** Expand leading ~/ to the user's home directory. */
function expandHome(filepath) {
	if (filepath.startsWith("~/")) return `${homedir()}${filepath.slice(1)}`;
	return filepath;
}
function env(key, fallback) {
	const val = process.env[key];
	return val !== void 0 && val !== "" ? val : fallback;
}
function envInt(key, fallback) {
	const val = process.env[key];
	if (val === void 0 || val === "") return fallback;
	const n = parseInt(val, 10);
	return Number.isNaN(n) ? fallback : n;
}
function clamp$2(n, min, max) {
	return Math.max(min, Math.min(max, n));
}
/**
* 单 wiki 内阶段1 并发 LLM 抽取数。
* KNOWLEDGE_WIKI_INGEST_CONCURRENCY，默认 3，clamp 1~10。
*/
function getIngestConcurrency(env = process.env) {
	const raw = parseInt(env.KNOWLEDGE_WIKI_INGEST_CONCURRENCY ?? "", 10);
	return clamp$2(Number.isNaN(raw) ? 3 : raw, 1, 10);
}
/**
* 全局 LLM 最大并发数（跨所有 wiki 的 extract + merge）。
* KNOWLEDGE_LLM_GLOBAL_CONCURRENCY，默认 5，clamp 1~20。
*/
function getGlobalLlmConcurrency(env = process.env) {
	const raw = parseInt(env.KNOWLEDGE_LLM_GLOBAL_CONCURRENCY ?? "", 10);
	return clamp$2(Number.isNaN(raw) ? 5 : raw, 1, 20);
}
function envBool(key, fallback) {
	const val = process.env[key];
	if (val === void 0 || val === "") return fallback;
	return [
		"1",
		"true",
		"yes",
		"on"
	].includes(val.toLowerCase());
}
function validateClickHouseConfig(config) {
	if (!config.enabled) return;
	if (!config.url) throw new Error("KNOWLEDGE_CLICKHOUSE_URL is required when telemetry is enabled");
	const url = new URL(config.url);
	if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("KNOWLEDGE_CLICKHOUSE_URL must use http or https");
	const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/;
	if (!identifier.test(config.database)) throw new Error("Invalid KNOWLEDGE_CLICKHOUSE_DATABASE");
	if (!identifier.test(config.table)) throw new Error("Invalid KNOWLEDGE_CLICKHOUSE_TABLE");
	if (config.flushIntervalMs < 100) throw new Error("KNOWLEDGE_CLICKHOUSE_FLUSH_INTERVAL_MS must be >= 100");
	if (config.flushThreshold < 1) throw new Error("KNOWLEDGE_CLICKHOUSE_FLUSH_THRESHOLD must be >= 1");
	if (config.ttlDays < 0) throw new Error("KNOWLEDGE_CLICKHOUSE_TTL_DAYS must be >= 0");
	if (config.requestTimeoutMs < 100) throw new Error("KNOWLEDGE_CLICKHOUSE_REQUEST_TIMEOUT_MS must be >= 100");
}
/**
* Load service configuration from environment variables.
*/
function loadConfig() {
	const clickhouse = {
		enabled: envBool("KNOWLEDGE_CLICKHOUSE_ENABLED", false),
		url: env("KNOWLEDGE_CLICKHOUSE_URL", ""),
		database: env("KNOWLEDGE_CLICKHOUSE_DATABASE", "default"),
		table: env("KNOWLEDGE_CLICKHOUSE_TABLE", "tool_call_logs"),
		user: env("KNOWLEDGE_CLICKHOUSE_USER", "default"),
		password: env("KNOWLEDGE_CLICKHOUSE_PASSWORD", ""),
		flushIntervalMs: envInt("KNOWLEDGE_CLICKHOUSE_FLUSH_INTERVAL_MS", 5e3),
		flushThreshold: envInt("KNOWLEDGE_CLICKHOUSE_FLUSH_THRESHOLD", 50),
		ttlDays: envInt("KNOWLEDGE_CLICKHOUSE_TTL_DAYS", 90),
		requestTimeoutMs: envInt("KNOWLEDGE_CLICKHOUSE_REQUEST_TIMEOUT_MS", 5e3)
	};
	validateClickHouseConfig(clickhouse);
	return {
		port: envInt("PORT", 8421),
		dataDir: expandHome(env("KNOWLEDGE_DATA_DIR", "./data")),
		dbPath: expandHome(env("KNOWLEDGE_DB_PATH", "./data/knowledge.db")),
		logLevel: env("LOG_LEVEL", "debug"),
		apiPrefix: env("API_PREFIX", "/v3"),
		publicBaseUrl: env("KNOWLEDGE_PUBLIC_BASE_URL", ""),
		tmcCallbackUrl: env("TMC_CALLBACK_URL", ""),
		clickhouse,
		llm: {
			mode: env("LLM_MODE", "proxy") === "custom" ? "custom" : "proxy",
			protocol: env("LLM_PROTOCOL", "openai") === "anthropic" ? "anthropic" : "openai",
			provider: env("LLM_PROVIDER", "custom"),
			apiKey: env("LLM_API_KEY", ""),
			model: env("LLM_MODEL", "Memory-Model"),
			baseUrl: env("LLM_BASE_URL", ""),
			maxTokens: envInt("LLM_MAX_TOKENS", 32768),
			timeoutMs: envInt("LLM_TIMEOUT_MS", 12e5),
			stream: process.env.LLM_STREAM === "true"
		}
	};
}
//#endregion
//#region src/db/schema.ts
/**
* Drizzle ORM schema — 4 SQLite tables for knowledge metadata.
*
* Tables:
*   knowledge_code_graph       — code repo index metadata + status
*   knowledge_wiki             — wiki knowledge base metadata + status
*   knowledge_wiki_audit       — wiki state-change audit log (append-only)
*   knowledge_code_graph_audit — code-graph state-change audit log
*
* Soft-delete via `deleted_at` + partial unique index (WHERE deleted_at IS NULL).
*/
var schema_exports = /* @__PURE__ */ __exportAll({
	CODE_DATA_VERSION: () => 0,
	WIKI_DATA_VERSION: () => 0,
	knowledgeCodeGraph: () => knowledgeCodeGraph,
	knowledgeCodeGraphAudit: () => knowledgeCodeGraphAudit,
	knowledgeWiki: () => knowledgeWiki,
	knowledgeWikiAudit: () => knowledgeWikiAudit,
	llmBinding: () => llmBinding
});
const knowledgeCodeGraph = sqliteTable("knowledge_code_graph", {
	codeGraphId: text("code_graph_id").primaryKey(),
	serviceId: text("service_id").notNull(),
	teamId: text("team_id").notNull(),
	repoName: text("repo_name").notNull().default(""),
	repoUrl: text("repo_url").notNull(),
	branch: text("branch").notNull(),
	commitHash: text("commit_hash"),
	ownerUserId: text("owner_user_id"),
	userId: text("user_id"),
	agentId: text("agent_id"),
	taskId: text("task_id"),
	visibility: text("visibility").notNull().default("team"),
	status: text("status").notNull().default("pending"),
	internalStatus: text("internal_status"),
	syncError: text("sync_error"),
	statsJson: text("stats_json"),
	serviceUrl: text("service_url"),
	summary: text("summary"),
	version: integer("version").notNull().default(0),
	lastSyncAt: text("last_sync_at"),
	createdAt: text("created_at").notNull(),
	updatedAt: text("updated_at").notNull(),
	deletedAt: text("deleted_at")
}, (table) => [uniqueIndex("idx_kcg_team_repo_branch").on(table.serviceId, table.teamId, table.repoUrl, table.branch).where(sql`deleted_at IS NULL`), index("idx_kcg_team_status").on(table.serviceId, table.teamId, table.status)]);
const knowledgeWiki = sqliteTable("knowledge_wiki", {
	wikiId: text("wiki_id").primaryKey(),
	serviceId: text("service_id").notNull(),
	teamId: text("team_id").notNull(),
	name: text("name").notNull(),
	sourceType: text("source_type"),
	sourceUrl: text("source_url"),
	ownerUserId: text("owner_user_id"),
	userId: text("user_id"),
	agentId: text("agent_id"),
	taskId: text("task_id"),
	visibility: text("visibility").notNull().default("team"),
	status: text("status").notNull().default("draft"),
	internalStatus: text("internal_status"),
	syncError: text("sync_error"),
	pageCount: integer("page_count"),
	serviceUrl: text("service_url"),
	summary: text("summary"),
	version: integer("version").notNull().default(0),
	lastSyncAt: text("last_sync_at"),
	createdAt: text("created_at").notNull(),
	updatedAt: text("updated_at").notNull(),
	deletedAt: text("deleted_at")
}, (table) => [uniqueIndex("idx_kwiki_team_name").on(table.serviceId, table.teamId, table.name).where(sql`deleted_at IS NULL`), index("idx_kwiki_team_status").on(table.serviceId, table.teamId, table.status)]);
const knowledgeWikiAudit = sqliteTable("knowledge_wiki_audit", {
	id: integer("id").primaryKey({ autoIncrement: true }),
	wikiId: text("wiki_id").notNull(),
	serviceId: text("service_id"),
	version: integer("version").notNull().default(0),
	action: text("action").notNull(),
	userId: text("user_id"),
	agentId: text("agent_id"),
	detail: text("detail"),
	createdAt: text("created_at").notNull()
}, (table) => [index("idx_kwa_wiki_version").on(table.wikiId, table.version)]);
const knowledgeCodeGraphAudit = sqliteTable("knowledge_code_graph_audit", {
	id: integer("id").primaryKey({ autoIncrement: true }),
	codeGraphId: text("code_graph_id").notNull(),
	serviceId: text("service_id"),
	version: integer("version").notNull().default(0),
	action: text("action").notNull(),
	userId: text("user_id"),
	agentId: text("agent_id"),
	detail: text("detail"),
	createdAt: text("created_at").notNull()
}, (table) => [index("idx_kcga_cg_version").on(table.codeGraphId, table.version)]);
const llmBinding = sqliteTable("llm_binding", {
	serviceId: text("service_id").primaryKey(),
	mode: text("mode").notNull().default("proxy"),
	proxyBaseUrl: text("proxy_base_url"),
	apiKey: text("api_key"),
	baseUrl: text("base_url"),
	enabled: integer("enabled").notNull().default(1),
	updatedAt: text("updated_at").notNull()
});
//#endregion
//#region src/db/client.ts
/**
* Drizzle client initialization — creates better-sqlite3 Database + drizzle wrapper.
*
* Synchronous driver, matches existing store call patterns.
*/
/**
* Create a Drizzle-wrapped better-sqlite3 database.
* Sets WAL mode + busy_timeout for production safety.
*/
function createDb(opts) {
	if (opts.path !== ":memory:") mkdirSync(dirname(opts.path), { recursive: true });
	const raw = new Database(opts.path);
	raw.pragma("journal_mode = WAL");
	raw.pragma("busy_timeout = 5000");
	const db = drizzle(raw, { schema: schema_exports });
	if (opts.autoMigrate !== false) migrate(db, raw);
	return {
		db,
		raw
	};
}
/**
* Run idempotent CREATE TABLE IF NOT EXISTS for all 4 tables + indexes.
* Uses raw SQL for partial unique indexes (Drizzle schema definition generates them
* via drizzle-kit, but for runtime we ensure tables exist).
*/
function migrate(_db, raw) {
	raw.exec(`
    CREATE TABLE IF NOT EXISTS knowledge_code_graph (
      code_graph_id   TEXT PRIMARY KEY,
      service_id      TEXT NOT NULL,
      team_id         TEXT NOT NULL,
      repo_name       TEXT NOT NULL DEFAULT '',
      repo_url        TEXT NOT NULL,
      branch          TEXT NOT NULL,
      commit_hash     TEXT,
      owner_user_id   TEXT,
      user_id         TEXT,
      agent_id        TEXT,
      task_id         TEXT,
      visibility      TEXT NOT NULL DEFAULT 'team',
      status          TEXT NOT NULL DEFAULT 'pending',
      internal_status TEXT,
      sync_error      TEXT,
      stats_json      TEXT,
      version         INTEGER NOT NULL DEFAULT 0,
      last_sync_at    TEXT,
      created_at      TEXT NOT NULL,
      updated_at      TEXT NOT NULL,
      deleted_at      TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_kcg_team_repo_branch
      ON knowledge_code_graph(service_id, team_id, repo_url, branch)
      WHERE deleted_at IS NULL;

    CREATE INDEX IF NOT EXISTS idx_kcg_team_status
      ON knowledge_code_graph(service_id, team_id, status);

    CREATE TABLE IF NOT EXISTS knowledge_wiki (
      wiki_id         TEXT PRIMARY KEY,
      service_id      TEXT NOT NULL,
      team_id         TEXT NOT NULL,
      name            TEXT NOT NULL,
      source_type     TEXT,
      source_url      TEXT,
      owner_user_id   TEXT,
      user_id         TEXT,
      agent_id        TEXT,
      task_id         TEXT,
      visibility      TEXT NOT NULL DEFAULT 'team',
      status          TEXT NOT NULL DEFAULT 'draft',
      internal_status TEXT,
      sync_error      TEXT,
      page_count      INTEGER,
      version         INTEGER NOT NULL DEFAULT 0,
      last_sync_at    TEXT,
      created_at      TEXT NOT NULL,
      updated_at      TEXT NOT NULL,
      deleted_at      TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_kwiki_team_name
      ON knowledge_wiki(service_id, team_id, name)
      WHERE deleted_at IS NULL;

    CREATE INDEX IF NOT EXISTS idx_kwiki_team_status
      ON knowledge_wiki(service_id, team_id, status);

    CREATE TABLE IF NOT EXISTS knowledge_wiki_audit (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      wiki_id    TEXT NOT NULL,
      service_id TEXT,
      version    INTEGER NOT NULL DEFAULT 0,
      action     TEXT NOT NULL,
      user_id    TEXT,
      agent_id   TEXT,
      detail     TEXT,
      created_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_kwa_wiki_version
      ON knowledge_wiki_audit(wiki_id, version DESC);

    CREATE TABLE IF NOT EXISTS knowledge_code_graph_audit (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      code_graph_id TEXT NOT NULL,
      service_id    TEXT,
      version       INTEGER NOT NULL DEFAULT 0,
      action        TEXT NOT NULL,
      user_id       TEXT,
      agent_id      TEXT,
      detail        TEXT,
      created_at    TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_kcga_cg_version
      ON knowledge_code_graph_audit(code_graph_id, version DESC);

    CREATE TABLE IF NOT EXISTS llm_binding (
      service_id     TEXT PRIMARY KEY,
      mode           TEXT NOT NULL DEFAULT 'proxy',
      proxy_base_url TEXT,
      api_key        TEXT,
      model          TEXT,
      base_url       TEXT,
      enabled        INTEGER NOT NULL DEFAULT 1,
      updated_at     TEXT NOT NULL
    );
  `);
	addColumnIfMissing(raw, "knowledge_code_graph", "service_url", "TEXT");
	addColumnIfMissing(raw, "knowledge_code_graph", "summary", "TEXT");
	addColumnIfMissing(raw, "knowledge_wiki", "service_url", "TEXT");
	addColumnIfMissing(raw, "knowledge_wiki", "summary", "TEXT");
	addColumnIfMissing(raw, "knowledge_wiki_audit", "service_id", "TEXT");
	addColumnIfMissing(raw, "knowledge_code_graph_audit", "service_id", "TEXT");
}
/** Add a column to a table if it doesn't already exist. SQLite-safe. */
function addColumnIfMissing(raw, table, column, type) {
	if (!raw.pragma(`table_info(${table})`).some((c) => c.name === column)) raw.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${type};`);
}
//#endregion
//#region src/store/ids.ts
/**
* Knowledge asset global ID generation.
*
* Contract (see knowledge-api.yaml):
*   - LLM-Wiki   → `wiki-` + 8 chars [0-9a-z]
*   - Code-Graph → `cg-`   + 8 chars [0-9a-z]
*
* Globally unique, immutable; 8-char base36 ≈ 36^8 ≈ 2.8e12 space.
* Random collision handled by PK constraint + retry on insert.
*/
const ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz";
const RANDOM_LEN = 8;
const WIKI_ID_PREFIX = "wiki-";
/** Generate 8-char unbiased random base36 string. */
function randomSuffix(len = RANDOM_LEN) {
	let out = "";
	for (let i = 0; i < len; i++) out += ALPHABET[randomInt(36)];
	return out;
}
function genWikiId() {
	return WIKI_ID_PREFIX + randomSuffix();
}
function genCodeGraphId() {
	return "cg-" + randomSuffix();
}
function isWikiId(id) {
	return new RegExp(`^${WIKI_ID_PREFIX}[0-9a-z]{${RANDOM_LEN}}$`).test(id);
}
function isCodeGraphId(id) {
	return new RegExp(`^cg-[0-9a-z]{${RANDOM_LEN}}$`).test(id);
}
//#endregion
//#region src/store/sqlite-store.ts
/**
* SqliteKnowledgeStore — SQLite/Drizzle implementation of IKnowledgeStore.
*
* Responsibilities:
*   - code-graph / wiki asset CRUD (hard delete; soft-delete markers via deleted_at)
*   - Multi-tenant isolation (001, phase 5): EVERY read/write is scoped by
*     `service_id` (first parameter), then `team_id` where applicable. id-only
*     accessors also filter service_id so a foreign tenant can never read/mutate
*     another tenant's row (returns null/false → 404).
*   - Global ID generation (wiki-/cg-) + idempotency: same
*     (service_id, team_id, repo_url, branch) or (service_id, team_id, name)
*     duplicate create returns existing.
*   - Status state machine + restart recovery.
*/
const ID_RETRY = 5;
function nowIso() {
	return (/* @__PURE__ */ new Date()).toISOString();
}
function isUniqueViolation(err) {
	const msg = err instanceof Error ? err.message : String(err);
	return /UNIQUE constraint failed|SQLITE_CONSTRAINT/i.test(msg);
}
var SqliteKnowledgeStore = class {
	constructor(db) {
		this.db = db;
	}
	/**
	* Idempotent create: hit (service_id, team_id, repo_url, branch) returns existing
	* (existed=true); otherwise generate cg- id and insert (PK conflict auto-retry).
	*/
	createCodeGraph(input) {
		const existing = this.db.select().from(knowledgeCodeGraph).where(and(eq(knowledgeCodeGraph.serviceId, input.service_id), eq(knowledgeCodeGraph.teamId, input.team_id), eq(knowledgeCodeGraph.repoUrl, input.repo_url), eq(knowledgeCodeGraph.branch, input.branch), isNull(knowledgeCodeGraph.deletedAt))).get();
		if (existing) return {
			row: this.mapCgRow(existing),
			existed: true
		};
		const ts = nowIso();
		for (let attempt = 0; attempt < ID_RETRY; attempt++) {
			const id = genCodeGraphId();
			try {
				this.db.insert(knowledgeCodeGraph).values({
					codeGraphId: id,
					serviceId: input.service_id,
					teamId: input.team_id,
					repoName: input.repo_name ?? "",
					repoUrl: input.repo_url,
					branch: input.branch,
					ownerUserId: input.owner_user_id ?? null,
					userId: input.user_id ?? null,
					agentId: input.agent_id ?? null,
					taskId: input.task_id ?? null,
					visibility: input.visibility ?? "team",
					status: "pending",
					serviceUrl: input.service_url ?? null,
					version: 0,
					createdAt: ts,
					updatedAt: ts
				}).run();
				const row = this.db.select().from(knowledgeCodeGraph).where(eq(knowledgeCodeGraph.codeGraphId, id)).get();
				return {
					row: this.mapCgRow(row),
					existed: false
				};
			} catch (err) {
				const raced = this.db.select().from(knowledgeCodeGraph).where(and(eq(knowledgeCodeGraph.serviceId, input.service_id), eq(knowledgeCodeGraph.teamId, input.team_id), eq(knowledgeCodeGraph.repoUrl, input.repo_url), eq(knowledgeCodeGraph.branch, input.branch), isNull(knowledgeCodeGraph.deletedAt))).get();
				if (raced) return {
					row: this.mapCgRow(raced),
					existed: true
				};
				if (!isUniqueViolation(err) || attempt === ID_RETRY - 1) throw err;
			}
		}
		throw new Error("createCodeGraph: failed to allocate unique id");
	}
	getCodeGraph(serviceId, teamId, codeGraphId) {
		const row = this.db.select().from(knowledgeCodeGraph).where(and(eq(knowledgeCodeGraph.codeGraphId, codeGraphId), eq(knowledgeCodeGraph.serviceId, serviceId), eq(knowledgeCodeGraph.teamId, teamId))).get();
		return row ? this.mapCgRow(row) : null;
	}
	/** id-only accessor — STILL scoped by service_id (cross-Memory leak guard, 001 §2.4). */
	getCodeGraphById(serviceId, codeGraphId) {
		const row = this.db.select().from(knowledgeCodeGraph).where(and(eq(knowledgeCodeGraph.codeGraphId, codeGraphId), eq(knowledgeCodeGraph.serviceId, serviceId))).get();
		return row ? this.mapCgRow(row) : null;
	}
	listCodeGraphs(serviceId, teamId, opts) {
		const conditions = [eq(knowledgeCodeGraph.serviceId, serviceId), eq(knowledgeCodeGraph.teamId, teamId)];
		if (opts?.syncStatus) conditions.push(eq(knowledgeCodeGraph.status, opts.syncStatus));
		return this.db.select().from(knowledgeCodeGraph).where(and(...conditions)).orderBy(desc(knowledgeCodeGraph.updatedAt)).limit(opts?.limit ?? 20).offset(opts?.offset ?? 0).all().map((r) => this.mapCgRow(r));
	}
	countCodeGraphs(serviceId, teamId, opts) {
		const conditions = [eq(knowledgeCodeGraph.serviceId, serviceId), eq(knowledgeCodeGraph.teamId, teamId)];
		if (opts?.syncStatus) conditions.push(eq(knowledgeCodeGraph.status, opts.syncStatus));
		return this.db.select({ total: sql`count(*)` }).from(knowledgeCodeGraph).where(and(...conditions)).get()?.total ?? 0;
	}
	/** id-only mutation — scoped by service_id so a foreign tenant cannot mutate. */
	updateCodeGraphStatus(serviceId, codeGraphId, patch) {
		const set = { updatedAt: nowIso() };
		if (patch.status !== void 0) set.status = patch.status;
		if (patch.internal_status !== void 0) set.internalStatus = patch.internal_status;
		if (patch.sync_error !== void 0) set.syncError = patch.sync_error;
		if (patch.commit_hash !== void 0) set.commitHash = patch.commit_hash;
		if (patch.stats_json !== void 0) set.statsJson = patch.stats_json;
		if (patch.last_sync_at !== void 0) set.lastSyncAt = patch.last_sync_at;
		if (patch.service_url !== void 0) set.serviceUrl = patch.service_url;
		if (patch.summary !== void 0) set.summary = patch.summary;
		if (patch.version !== void 0) set.version = patch.version;
		this.db.update(knowledgeCodeGraph).set(set).where(and(eq(knowledgeCodeGraph.codeGraphId, codeGraphId), eq(knowledgeCodeGraph.serviceId, serviceId))).run();
	}
	/** Hard delete; memory/team mismatch returns false. */
	deleteCodeGraph(serviceId, teamId, codeGraphId) {
		return this.db.delete(knowledgeCodeGraph).where(and(eq(knowledgeCodeGraph.codeGraphId, codeGraphId), eq(knowledgeCodeGraph.serviceId, serviceId), eq(knowledgeCodeGraph.teamId, teamId))).run().changes > 0;
	}
	/** Update code-graph metadata (repo_name, summary). memory mismatch → null. */
	updateCodeGraphMeta(serviceId, codeGraphId, patch) {
		const set = { updatedAt: nowIso() };
		if (patch.repo_name !== void 0) set.repoName = patch.repo_name;
		if (patch.summary !== void 0) set.summary = patch.summary;
		this.db.update(knowledgeCodeGraph).set(set).where(and(eq(knowledgeCodeGraph.codeGraphId, codeGraphId), eq(knowledgeCodeGraph.serviceId, serviceId))).run();
		return this.getCodeGraphById(serviceId, codeGraphId);
	}
	createWiki(input) {
		const existing = this.db.select().from(knowledgeWiki).where(and(eq(knowledgeWiki.serviceId, input.service_id), eq(knowledgeWiki.teamId, input.team_id), eq(knowledgeWiki.name, input.name), isNull(knowledgeWiki.deletedAt))).get();
		if (existing) return {
			row: this.mapWikiRow(existing),
			existed: true
		};
		const ts = nowIso();
		for (let attempt = 0; attempt < ID_RETRY; attempt++) {
			const id = genWikiId();
			try {
				this.db.insert(knowledgeWiki).values({
					wikiId: id,
					serviceId: input.service_id,
					teamId: input.team_id,
					name: input.name,
					sourceType: input.source_type ?? null,
					sourceUrl: input.source_url ?? null,
					ownerUserId: input.owner_user_id ?? null,
					userId: input.user_id ?? null,
					agentId: input.agent_id ?? null,
					taskId: input.task_id ?? null,
					visibility: input.visibility ?? "team",
					status: "draft",
					serviceUrl: input.service_url ?? null,
					version: 0,
					createdAt: ts,
					updatedAt: ts
				}).run();
				const row = this.db.select().from(knowledgeWiki).where(eq(knowledgeWiki.wikiId, id)).get();
				return {
					row: this.mapWikiRow(row),
					existed: false
				};
			} catch (err) {
				const raced = this.db.select().from(knowledgeWiki).where(and(eq(knowledgeWiki.serviceId, input.service_id), eq(knowledgeWiki.teamId, input.team_id), eq(knowledgeWiki.name, input.name), isNull(knowledgeWiki.deletedAt))).get();
				if (raced) return {
					row: this.mapWikiRow(raced),
					existed: true
				};
				if (!isUniqueViolation(err) || attempt === ID_RETRY - 1) throw err;
			}
		}
		throw new Error("createWiki: failed to allocate unique id");
	}
	getWiki(serviceId, teamId, wikiId) {
		const row = this.db.select().from(knowledgeWiki).where(and(eq(knowledgeWiki.wikiId, wikiId), eq(knowledgeWiki.serviceId, serviceId), eq(knowledgeWiki.teamId, teamId))).get();
		return row ? this.mapWikiRow(row) : null;
	}
	/** id-only accessor — STILL scoped by service_id (cross-Memory leak guard, 001 §2.4). */
	getWikiById(serviceId, wikiId) {
		const row = this.db.select().from(knowledgeWiki).where(and(eq(knowledgeWiki.wikiId, wikiId), eq(knowledgeWiki.serviceId, serviceId))).get();
		return row ? this.mapWikiRow(row) : null;
	}
	listWikis(serviceId, teamId, opts) {
		const conditions = [eq(knowledgeWiki.serviceId, serviceId), eq(knowledgeWiki.teamId, teamId)];
		if (opts?.syncStatus) conditions.push(eq(knowledgeWiki.status, opts.syncStatus));
		return this.db.select().from(knowledgeWiki).where(and(...conditions)).orderBy(desc(knowledgeWiki.updatedAt)).limit(opts?.limit ?? 20).offset(opts?.offset ?? 0).all().map((r) => this.mapWikiRow(r));
	}
	countWikis(serviceId, teamId, opts) {
		const conditions = [eq(knowledgeWiki.serviceId, serviceId), eq(knowledgeWiki.teamId, teamId)];
		if (opts?.syncStatus) conditions.push(eq(knowledgeWiki.status, opts.syncStatus));
		return this.db.select({ total: sql`count(*)` }).from(knowledgeWiki).where(and(...conditions)).get()?.total ?? 0;
	}
	/** id-only mutation — scoped by service_id so a foreign tenant cannot mutate. */
	updateWikiStatus(serviceId, wikiId, patch) {
		const set = { updatedAt: nowIso() };
		if (patch.status !== void 0) set.status = patch.status;
		if (patch.internal_status !== void 0) set.internalStatus = patch.internal_status;
		if (patch.sync_error !== void 0) set.syncError = patch.sync_error;
		if (patch.page_count !== void 0) set.pageCount = patch.page_count;
		if (patch.last_sync_at !== void 0) set.lastSyncAt = patch.last_sync_at;
		if (patch.service_url !== void 0) set.serviceUrl = patch.service_url;
		if (patch.summary !== void 0) set.summary = patch.summary;
		if (patch.version !== void 0) set.version = patch.version;
		this.db.update(knowledgeWiki).set(set).where(and(eq(knowledgeWiki.wikiId, wikiId), eq(knowledgeWiki.serviceId, serviceId))).run();
	}
	deleteWiki(serviceId, teamId, wikiId) {
		return this.db.delete(knowledgeWiki).where(and(eq(knowledgeWiki.wikiId, wikiId), eq(knowledgeWiki.serviceId, serviceId), eq(knowledgeWiki.teamId, teamId))).run().changes > 0;
	}
	/** Update wiki metadata (name, summary). memory mismatch → null. */
	updateWikiMeta(serviceId, wikiId, patch) {
		const set = { updatedAt: nowIso() };
		if (patch.name !== void 0) set.name = patch.name;
		if (patch.summary !== void 0) set.summary = patch.summary;
		this.db.update(knowledgeWiki).set(set).where(and(eq(knowledgeWiki.wikiId, wikiId), eq(knowledgeWiki.serviceId, serviceId))).run();
		return this.getWikiById(serviceId, wikiId);
	}
	appendWikiAudit(input) {
		this.db.insert(knowledgeWikiAudit).values({
			wikiId: input.asset_id,
			serviceId: input.service_id ?? null,
			version: input.version,
			action: input.action,
			userId: input.user_id ?? null,
			agentId: input.agent_id ?? null,
			detail: input.detail ?? null,
			createdAt: nowIso()
		}).run();
	}
	appendCodeGraphAudit(input) {
		this.db.insert(knowledgeCodeGraphAudit).values({
			codeGraphId: input.asset_id,
			serviceId: input.service_id ?? null,
			version: input.version,
			action: input.action,
			userId: input.user_id ?? null,
			agentId: input.agent_id ?? null,
			detail: input.detail ?? null,
			createdAt: nowIso()
		}).run();
	}
	listWikiAudit(serviceId, wikiId, limit = 20, offset = 0) {
		return this.db.select().from(knowledgeWikiAudit).where(and(eq(knowledgeWikiAudit.wikiId, wikiId), eq(knowledgeWikiAudit.serviceId, serviceId))).orderBy(desc(knowledgeWikiAudit.version), desc(knowledgeWikiAudit.id)).limit(limit).offset(offset).all().map((r) => ({
			id: r.id,
			service_id: r.serviceId ?? null,
			asset_id: r.wikiId,
			version: r.version,
			action: r.action,
			user_id: r.userId,
			agent_id: r.agentId,
			detail: r.detail,
			created_at: r.createdAt
		}));
	}
	listCodeGraphAudit(serviceId, codeGraphId, limit = 20, offset = 0) {
		return this.db.select().from(knowledgeCodeGraphAudit).where(and(eq(knowledgeCodeGraphAudit.codeGraphId, codeGraphId), eq(knowledgeCodeGraphAudit.serviceId, serviceId))).orderBy(desc(knowledgeCodeGraphAudit.version), desc(knowledgeCodeGraphAudit.id)).limit(limit).offset(offset).all().map((r) => ({
			id: r.id,
			service_id: r.serviceId ?? null,
			asset_id: r.codeGraphId,
			version: r.version,
			action: r.action,
			user_id: r.userId,
			agent_id: r.agentId,
			detail: r.detail,
			created_at: r.createdAt
		}));
	}
	/**
	* Sweep all non-terminal (pending/processing) assets to failed, across all tenants.
	* After restart, in-memory SerialQueue tasks are lost; this makes them visible to control plane.
	* @returns total affected rows (code + wiki combined).
	*/
	markInterruptedAsFailed(reason = "interrupted by restart") {
		const ts = nowIso();
		const a = this.db.update(knowledgeCodeGraph).set({
			status: "failed",
			syncError: reason,
			updatedAt: ts
		}).where(sql`status IN ('pending','processing')`).run();
		const b = this.db.update(knowledgeWiki).set({
			status: "failed",
			syncError: reason,
			updatedAt: ts
		}).where(sql`status IN ('pending','processing')`).run();
		return a.changes + b.changes;
	}
	/** All ready code-graphs (with service_id) so module.ts can rebuild per-tenant dirs. */
	listSyncedCodeGraphs() {
		return this.db.select({
			code_graph_id: knowledgeCodeGraph.codeGraphId,
			service_id: knowledgeCodeGraph.serviceId,
			team_id: knowledgeCodeGraph.teamId
		}).from(knowledgeCodeGraph).where(and(eq(knowledgeCodeGraph.status, "ready"), isNull(knowledgeCodeGraph.deletedAt))).all();
	}
	listSyncedWikis() {
		return this.db.select({
			wiki_id: knowledgeWiki.wikiId,
			service_id: knowledgeWiki.serviceId,
			team_id: knowledgeWiki.teamId
		}).from(knowledgeWiki).where(and(eq(knowledgeWiki.status, "ready"), isNull(knowledgeWiki.deletedAt))).all();
	}
	mapCgRow(r) {
		return {
			code_graph_id: r.codeGraphId,
			service_id: r.serviceId,
			team_id: r.teamId,
			repo_name: r.repoName,
			repo_url: r.repoUrl,
			branch: r.branch,
			commit_hash: r.commitHash,
			owner_user_id: r.ownerUserId,
			user_id: r.userId,
			agent_id: r.agentId,
			task_id: r.taskId,
			visibility: r.visibility,
			status: r.status,
			internal_status: r.internalStatus,
			sync_error: r.syncError,
			stats_json: r.statsJson,
			service_url: r.serviceUrl ?? null,
			summary: r.summary ?? null,
			version: r.version,
			last_sync_at: r.lastSyncAt,
			created_at: r.createdAt,
			updated_at: r.updatedAt,
			deleted_at: r.deletedAt ?? null
		};
	}
	mapWikiRow(r) {
		return {
			wiki_id: r.wikiId,
			service_id: r.serviceId,
			team_id: r.teamId,
			name: r.name,
			source_type: r.sourceType,
			source_url: r.sourceUrl,
			owner_user_id: r.ownerUserId,
			user_id: r.userId,
			agent_id: r.agentId,
			task_id: r.taskId,
			visibility: r.visibility,
			status: r.status,
			internal_status: r.internalStatus,
			sync_error: r.syncError,
			page_count: r.pageCount,
			service_url: r.serviceUrl ?? null,
			summary: r.summary ?? null,
			version: r.version,
			last_sync_at: r.lastSyncAt,
			created_at: r.createdAt,
			updated_at: r.updatedAt,
			deleted_at: r.deletedAt ?? null
		};
	}
};
//#endregion
//#region src/store/serial-queue.ts
var SerialQueue = class {
	name;
	queue = [];
	running = false;
	paused = false;
	idleResolvers = [];
	constructor(name = "unnamed") {
		this.name = name;
	}
	get size() {
		return this.queue.length;
	}
	get pending() {
		return this.running;
	}
	get idle() {
		return this.queue.length === 0 && !this.running;
	}
	add(task) {
		return new Promise((resolve, reject) => {
			this.queue.push({
				task,
				resolve,
				reject
			});
			this.drain();
		});
	}
	pause() {
		this.paused = true;
	}
	start() {
		this.paused = false;
		this.drain();
	}
	onIdle() {
		if (this.queue.length === 0 && !this.running) return Promise.resolve();
		return new Promise((resolve) => {
			this.idleResolvers.push(resolve);
		});
	}
	clear() {
		for (const entry of this.queue) entry.reject(/* @__PURE__ */ new Error("Queue cleared"));
		this.queue = [];
	}
	drain() {
		if (this.running || this.paused || this.queue.length === 0) return;
		const entry = this.queue.shift();
		this.running = true;
		entry.task().then((result) => entry.resolve(result)).catch((err) => entry.reject(err)).finally(() => {
			this.running = false;
			if (this.queue.length === 0) {
				const resolvers = this.idleResolvers;
				this.idleResolvers = [];
				for (const resolve of resolvers) resolve();
			} else this.drain();
		});
	}
};
//#endregion
//#region src/store/build-queue.ts
/**
* BuildQueue — per-asset-key serial execution queue.
*
* Each asset id gets its own SerialQueue → same asset never rebuilds
* concurrently (git/SQLite/files don't conflict).
* enqueue is fire-and-forget; onIdle() for tests / graceful shutdown.
*/
var BuildQueue = class {
	queues = /* @__PURE__ */ new Map();
	/** Enqueue job to this key's serial queue; fire-and-forget. */
	enqueue(key, job) {
		let q = this.queues.get(key);
		if (!q) {
			q = new SerialQueue(key);
			this.queues.set(key, q);
		}
		q.add(job).catch(() => {});
	}
	/** Wait for a key (or all) queue to be idle. Mainly for tests / shutdown. */
	async onIdle(key) {
		if (key) {
			await this.queues.get(key)?.onIdle();
			return;
		}
		await Promise.all([...this.queues.values()].map((q) => q.onIdle()));
	}
};
//#endregion
//#region src/store/llm-binding-store.ts
/**
* LlmBindingStore — per-instance (service_id) LLM routing binding.
*
* Decoupled from IKnowledgeStore: a single small table keyed by service_id that
* tells wiki ingest/summary which LLM endpoint to use for that instance.
*   - mode='proxy' → route through context_proxy with a dedicated knowledge-service
*     user_key so LLM usage is billed per instance.
*   - mode='byo'   → user-supplied OpenAI-compatible endpoint.
*
* No binding (or disabled/incomplete) → behaviour depends on the global LLM_MODE:
*   - LLM_MODE=custom → fall back to the global LLM_* (direct BYO) config.
*   - LLM_MODE=proxy (default) → NO silent direct fallback; an "unconfigured"
*     config is returned so wiki ingest fails loudly (forces the proxy binding
*     chain to actually be verified instead of masking bugs).
*
* Model is NOT stored per-instance — it always comes from the global `LLM_MODEL`
* env (single source of truth, see resolveLlmConfig).
*/
function createLlmBindingStore(db) {
	return {
		get(serviceId) {
			const row = db.select().from(llmBinding).where(eq(llmBinding.serviceId, serviceId)).all()[0];
			return row ? toRow(row) : null;
		},
		listAll() {
			return db.select().from(llmBinding).all().map(toRow);
		},
		upsert(serviceId, input) {
			const now = (/* @__PURE__ */ new Date()).toISOString();
			const existing = this.get(serviceId);
			const apiKey = input.api_key !== void 0 ? input.api_key : existing?.api_key ?? null;
			const values = {
				serviceId,
				mode: input.mode,
				proxyBaseUrl: input.proxy_base_url ?? null,
				apiKey,
				baseUrl: input.base_url ?? null,
				enabled: input.enabled === false ? 0 : 1,
				updatedAt: now
			};
			db.insert(llmBinding).values(values).onConflictDoUpdate({
				target: llmBinding.serviceId,
				set: {
					mode: values.mode,
					proxyBaseUrl: values.proxyBaseUrl,
					apiKey: values.apiKey,
					baseUrl: values.baseUrl,
					enabled: values.enabled,
					updatedAt: values.updatedAt
				}
			}).run();
			return this.get(serviceId);
		},
		status(serviceId) {
			const row = this.get(serviceId);
			if (!row) return {
				bound: false,
				mode: null,
				enabled: false
			};
			return {
				bound: true,
				mode: row.mode,
				enabled: row.enabled
			};
		}
	};
}
function toRow(r) {
	return {
		service_id: r.serviceId,
		mode: r.mode === "byo" ? "byo" : "proxy",
		proxy_base_url: r.proxyBaseUrl ?? null,
		api_key: r.apiKey ?? null,
		base_url: r.baseUrl ?? null,
		enabled: r.enabled !== 0,
		updated_at: r.updatedAt
	};
}
function trimTrailingSlash(url) {
	return url.replace(/\/+$/, "");
}
/**
* Pure resolver: turn a binding (or its absence) into an effective LlmConfig.
* maxTokens/timeoutMs/provider/mode always inherit from the fallback; the binding
* only overrides endpoint/key.
*
* Model always comes from `fallback.model` (= global `LLM_MODEL` env) — no
* per-instance model storage, single source of truth.
*
* proxy mode → baseUrl = {proxy_base_url}/proxy/{service_id}/v1 (so context_proxy
* uses service_id as x-tdai-service-id for auth/verify + per-instance billing).
*
* When there is no usable binding, the global default is decided by fallback.mode:
*   - 'custom' → return the global config as-is (direct BYO).
*   - 'proxy'  → return the global config with baseUrl/apiKey blanked, so
*     createLlmClient throws instead of silently calling a direct endpoint.
*/
function resolveLlmConfig(serviceId, binding, fallback) {
	const globalDefault = () => fallback.mode === "custom" ? fallback : {
		...fallback,
		baseUrl: "",
		apiKey: ""
	};
	if (!binding || !binding.enabled) return globalDefault();
	if (binding.mode === "proxy") {
		if (!binding.proxy_base_url || !binding.api_key) return globalDefault();
		return {
			mode: fallback.mode,
			protocol: fallback.protocol,
			provider: fallback.provider,
			apiKey: binding.api_key,
			model: fallback.model,
			baseUrl: `${trimTrailingSlash(binding.proxy_base_url)}/proxy/${serviceId}/v1`,
			maxTokens: fallback.maxTokens,
			timeoutMs: fallback.timeoutMs,
			stream: fallback.stream
		};
	}
	if (!binding.base_url || !binding.api_key) return globalDefault();
	return {
		mode: fallback.mode,
		protocol: fallback.protocol,
		provider: fallback.provider,
		apiKey: binding.api_key,
		model: fallback.model,
		baseUrl: binding.base_url,
		maxTokens: fallback.maxTokens,
		timeoutMs: fallback.timeoutMs,
		stream: fallback.stream
	};
}
//#endregion
//#region src/store/code-graph-service.ts
/**
* CodeGraphService — code-graph 资产的异步编排。
*
* 把 IKnowledgeStore（元数据/状态）+ BuildQueue（后台串行）+ 可注入的
* worker（实际 git clone + codegraph 建图）粘合，实现：
*   - create/sync 立即返回（fire-and-forget），管控轮询 status；
*   - 状态机 pending → processing(cloning/indexing) → ready / failed(+sync_error)；
*   - memory + team 隔离、幂等（同 memory+team+repo+branch 返回已存在）、硬删 + 四类资源清理。
*
* delete 语义（008 / 007 §5.5）：任何状态（含 pending/processing）均可删。
* 用内存 cancelled 标记通知 in-flight worker 中止（不落库、不软删）；worker 在
* 结束前的检查点发现被删则跳过 ready/回调并做幂等清理。清理覆盖四类资源：
* instance pool（内存）→ 元数据行（硬删）→ 磁盘目录（rmSync），分步 try/catch
* 保证任一步失败不影响其余（异常安全 + 幂等）。远端元数据上报本阶段不做。
*
* worker 注入便于单测（无需真实 git/codegraph）；生产实现见 router 装配处。
* 物理目录：{dataRoot}/{service_id}/{team_id}/{code_graph_id}/（001 多租户）。
*/
var CodeGraphService = class {
	store;
	dataRoot;
	worker;
	queue;
	logger;
	callbackConfig;
	releaseInstance;
	/**
	* In-flight delete 标记：delete 命中一个正在排队/执行的资源时置位，
	* worker 在检查点读取以决定中止。仅内存态（同 id 由 SerialQueue 串行 +
	* Node 单线程，读写无并发）。清理收尾后移除。
	*/
	cancelled = /* @__PURE__ */ new Set();
	constructor(opts) {
		this.store = opts.store;
		this.dataRoot = opts.dataRoot;
		this.worker = opts.worker;
		this.queue = opts.queue ?? new BuildQueue();
		this.logger = opts.logger;
		this.callbackConfig = opts.callbackConfig;
		this.releaseInstance = opts.releaseInstance;
	}
	dirFor(serviceId, teamId, codeGraphId) {
		return join(this.dataRoot, serviceId, teamId, codeGraphId);
	}
	/**
	* 幂等创建并异步建图。
	* - 已存在（同 memory+team+repo+branch）→ 直接返回已有行，不重复建图。
	* - 新建 → 入库 pending + 后台建图。
	*/
	create(params) {
		const { row, existed } = this.store.createCodeGraph(params);
		if (!existed) {
			this.audit(row, "create", `clone ${row.repo_url}@${row.branch}`, params.user_id);
			this.enqueueBuild(row);
		}
		return {
			row,
			existed
		};
	}
	/** Persist service_url for a code-graph. Returns updated row or null. */
	updateServiceUrl(serviceId, codeGraphId, serviceUrl) {
		this.store.updateCodeGraphStatus(serviceId, codeGraphId, { service_url: serviceUrl });
		return this.store.getCodeGraphById(serviceId, codeGraphId);
	}
	/** Update code-graph metadata (repo_name, summary). Returns updated row or null. */
	updateMeta(serviceId, codeGraphId, patch) {
		return this.store.updateCodeGraphMeta(serviceId, codeGraphId, patch);
	}
	/** 重新拉取 + 重建（管控显式触发）。memory/team 不匹配返回 not_found；pending/processing 返回 busy。 */
	sync(serviceId, teamId, codeGraphId, requesterUserId) {
		const row = this.store.getCodeGraph(serviceId, teamId, codeGraphId);
		if (!row) return { kind: "not_found" };
		if (row.status === "pending" || row.status === "processing") return {
			kind: "busy",
			status: row.status,
			step: row.internal_status
		};
		const nextVersion = row.version + 1;
		this.store.updateCodeGraphStatus(serviceId, codeGraphId, {
			status: "pending",
			internal_status: null,
			sync_error: null,
			version: nextVersion
		});
		this.audit({
			...row,
			version: nextVersion
		}, "ingest", "manual sync", requesterUserId);
		const fresh = this.store.getCodeGraph(serviceId, teamId, codeGraphId);
		if (fresh) this.enqueueBuild(fresh);
		return fresh ? {
			kind: "ok",
			row: fresh
		} : { kind: "not_found" };
	}
	get(serviceId, teamId, codeGraphId) {
		return this.store.getCodeGraph(serviceId, teamId, codeGraphId);
	}
	/** 按全局唯一 code_graph_id 查询（仍按 service_id 收敛防跨租户）。spec id-only 端点专用。 */
	getById(serviceId, codeGraphId) {
		return this.store.getCodeGraphById(serviceId, codeGraphId);
	}
	list(serviceId, teamId, opts) {
		return this.store.listCodeGraphs(serviceId, teamId, opts);
	}
	count(serviceId, teamId, opts) {
		return this.store.countCodeGraphs(serviceId, teamId, opts);
	}
	/**
	* 删除 code-graph（008 / 007 §5.5）。任何状态均可删（含 pending/processing）。
	* memory/team 不匹配返回 false；否则硬删 + 四类资源清理，返回 true。
	*
	* 若资源正在排队/执行（pending/processing），先置 cancelled 标记通知 worker
	* 在检查点中止；随后立即硬删 + 清理（不等 worker）。worker 结束前重查发现
	* 已删则跳过 ready/回调并再做一次幂等清理，无残留。
	*/
	delete(serviceId, teamId, codeGraphId) {
		const row = this.store.getCodeGraph(serviceId, teamId, codeGraphId);
		if (!row) return false;
		if (row.status === "pending" || row.status === "processing") this.cancelled.add(codeGraphId);
		this.audit(row, "delete", null);
		this.cleanupResources(serviceId, teamId, codeGraphId);
		return true;
	}
	/**
	* 四类资源幂等清理（顺序：先释放内存/连接，再删盘）。
	* 每步独立 try/catch —— 任一步失败不影响其余，保证异常安全。
	*   1. instance pool（内存）：releaseInstance（pool.delete + closeIndex）
	*   2. 元数据行：硬删（命中 0 行也安全，支持 worker + delete 双重清理）
	*   3. 磁盘目录：rmSync recursive+force（幂等）
	* BuildQueue 排队任务由 runBuild 入口检查 cancelled/行存在性跳过，无需在此处理。
	*/
	cleanupResources(serviceId, teamId, codeGraphId) {
		try {
			this.releaseInstance?.(codeGraphId);
		} catch (err) {
			this.logger?.warn?.(`[code-graph] release instance failed ${codeGraphId}: ${String(err)}`);
		}
		try {
			this.store.deleteCodeGraph(serviceId, teamId, codeGraphId);
		} catch (err) {
			this.logger?.warn?.(`[code-graph] hard-delete row failed ${codeGraphId}: ${String(err)}`);
		}
		try {
			rmSync(this.dirFor(serviceId, teamId, codeGraphId), {
				recursive: true,
				force: true
			});
		} catch (err) {
			this.logger?.warn?.(`[code-graph] rm dir failed ${codeGraphId}: ${String(err)}`);
		}
	}
	/**
	* worker 检查点：资源是否已被删除（cancelled 标记命中，或行已不在库）。
	* 双判据覆盖：①delete 发生在 worker 运行中（cancelled）；②delete 已完成
	* 且行被硬删（getById → null）。任一即视为已删。
	*/
	isDeleted(serviceId, codeGraphId) {
		return this.cancelled.has(codeGraphId) || this.store.getCodeGraphById(serviceId, codeGraphId) === null;
	}
	/** 写一条 code-graph 审计记录。失败不阻断主流程。 */
	audit(row, action, detail, requesterUserId) {
		try {
			this.store.appendCodeGraphAudit({
				service_id: row.service_id,
				asset_id: row.code_graph_id,
				version: row.version,
				action,
				user_id: requesterUserId ?? row.user_id,
				agent_id: row.agent_id,
				detail
			});
		} catch (err) {
			this.logger?.warn?.(`[code-graph] audit ${action} failed: ${String(err)}`);
		}
	}
	enqueueBuild(row) {
		this.queue.enqueue(row.code_graph_id, () => this.runBuild(row.service_id, row.code_graph_id, row.team_id, row.repo_url, row.branch));
	}
	async runBuild(serviceId, codeGraphId, teamId, repoUrl, branch) {
		if (this.isDeleted(serviceId, codeGraphId)) {
			this.finishCancelled(serviceId, teamId, codeGraphId);
			return;
		}
		this.store.updateCodeGraphStatus(serviceId, codeGraphId, {
			status: "processing",
			internal_status: "cloning",
			sync_error: null
		});
		try {
			const result = await this.worker({
				codeGraphId,
				serviceId,
				teamId,
				repoUrl,
				branch,
				dir: this.dirFor(serviceId, teamId, codeGraphId),
				setInternalStatus: (s) => this.store.updateCodeGraphStatus(serviceId, codeGraphId, {
					status: "processing",
					internal_status: s
				})
			});
			if (this.isDeleted(serviceId, codeGraphId)) {
				this.finishCancelled(serviceId, teamId, codeGraphId);
				return;
			}
			this.store.updateCodeGraphStatus(serviceId, codeGraphId, {
				status: "ready",
				internal_status: null,
				sync_error: null,
				commit_hash: result.commitHash ?? null,
				stats_json: result.stats ? JSON.stringify(result.stats) : null,
				last_sync_at: (/* @__PURE__ */ new Date()).toISOString()
			});
			const synced = this.store.getCodeGraphById(serviceId, codeGraphId);
			if (synced) this.audit(synced, "ready", result.stats ? JSON.stringify(result.stats) : null);
			this.logger?.info?.(`[code-graph] ${codeGraphId} ready`);
			await this.onBuildComplete(synced, "ready", null, result.stats ?? null);
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			if (this.isDeleted(serviceId, codeGraphId)) {
				this.finishCancelled(serviceId, teamId, codeGraphId);
				return;
			}
			this.store.updateCodeGraphStatus(serviceId, codeGraphId, {
				status: "failed",
				internal_status: null,
				sync_error: msg.slice(0, 500)
			});
			const failed = this.store.getCodeGraphById(serviceId, codeGraphId);
			if (failed) this.audit(failed, "failed", msg.slice(0, 500));
			this.logger?.warn?.(`[code-graph] ${codeGraphId} failed: ${msg}`);
			await this.onBuildComplete(failed, "failed", msg, null);
		}
	}
	/**
	* worker 检查点判定“已删”后的收尾：幂等清理 worker 可能刚写下的盘/句柄，
	* 并移除 cancelled 标记（该 id 的 worker 到此结束，标记使命完成）。
	*/
	finishCancelled(serviceId, teamId, codeGraphId) {
		this.cleanupResources(serviceId, teamId, codeGraphId);
		this.cancelled.delete(codeGraphId);
		this.logger?.info?.(`[code-graph] ${codeGraphId} build aborted (deleted during processing)`);
	}
	/**
	* Post-build hook: generate summary (if synced) and callback TMC.
	* Never throws — runs after the main build is already committed.
	*/
	async onBuildComplete(row, status, errorMsg, stats) {
		if (!row || !this.callbackConfig) return;
		let summary = null;
		if (status === "ready") {
			const { generateCodeGraphSummary } = await import("./callback-dsWv60u2.mjs");
			summary = generateCodeGraphSummary(row.repo_name || row.repo_url, row.branch, stats);
			if (summary) this.store.updateCodeGraphStatus(row.service_id, row.code_graph_id, { summary });
		}
		const { callbackTMC } = await import("./callback-dsWv60u2.mjs");
		await callbackTMC({
			knowledge_id: row.code_graph_id,
			service_id: row.service_id,
			type: "code-graph",
			status,
			summary,
			sync_error: errorMsg?.slice(0, 500) ?? null,
			timestamp: (/* @__PURE__ */ new Date()).toISOString()
		}, this.callbackConfig);
	}
	/** 等待后台任务完成（测试 / 停机）。 */
	async onIdle(codeGraphId) {
		await this.queue.onIdle(codeGraphId);
	}
};
//#endregion
//#region src/engines/wiki/index-db.ts
/**
* Per-wiki `index.db` connection management (设计 006).
*
* 每个 wiki 一个独立 SQLite 文件 `index.db`，放在该 wiki 的数据目录下（与正文 `.md`
* 同目录同生命周期），承载本 wiki 的全部私有索引数据：
*   - `wiki_fts`   FTS5 预分词倒排（BM25 全文检索）
*   - `page_meta`  页元数据（title/type/rel_path/snippet；正文不入库，留磁盘）
*   - `graph_edge` 知识图谱有向边（多跳 BFS 用）
*   - `source`     源文件一等实体（增量判断 + 生命周期；DDL 本轮建好，读写方法见 003 阶段）
*
* 连接策略（设计 §4）：
*   - 写（ingest/sync：重建 FTS5 + graph_edge + 更新 source）：**独立连接**，事务内完成
*     → `wal_checkpoint(TRUNCATE)` → `close()`，不进池，避免被读池 LRU 驱逐的竞态。
*   - 读（search/graph）：走 **LRU 连接池**，热 wiki 常驻、冷 wiki 驱逐。
*
* 内存上限 = POOL_MAX × cache_size（约 600MB），与 wiki 总数解耦；SQLite 打开连接
* 亚毫秒、数据按 page 懒加载，不是"打开即全量入内存"，正是它根治 MiniSearch 20GB OOM 的原因。
*
* fd 约束（设计 §4.3）：WAL 每连接占 3 fd（db+wal+shm），`POOL_MAX × 3 + 富余` 需 ≤ ulimit -n。
* POOL_MAX 与部署 ulimit 联动，默认 300（约 900 fd，建议 ulimit -n ≥ 2048）。
*/
/**
* 读连接池上限。与部署 ulimit 联动（WAL 每连接 3 fd，需 ulimit -n ≥ POOL_MAX*3 + 富余）。
* 可用环境变量覆盖（仅用于测试或特殊部署环境），默认 300。
*/
const POOL_MAX = (() => {
	const raw = process.env.KNOWLEDGE_WIKI_POOL_MAX;
	const n = raw ? Number.parseInt(raw, 10) : NaN;
	return Number.isInteger(n) && n > 0 ? n : 300;
})();
/** 每连接 page cache 上限（KB）；cache_size 用负数表示 KB。 */
const CACHE_KB = 2e3;
/** 驱逐/关闭一个读连接：先 checkpoint 合并 WAL，再关闭。失败静默（连接可能已损坏）。 */
function disposeDb(db) {
	try {
		if (db.open) {
			db.pragma("wal_checkpoint(TRUNCATE)");
			db.close();
		}
	} catch {}
}
/**
* 读连接 LRU 池（lru-cache，MIT）：热 wiki 连接常驻、冷 wiki 被驱逐。
* 驱逐（超出 max）与显式 `delete`（wiki 删除）都会触发 `dispose` → checkpoint + close。
*/
const readPool = new LRUCache({
	max: POOL_MAX,
	dispose: (db) => disposeDb(db)
});
/** 每个连接打开时统一设置的 pragma（设计 §4.2）。 */
function applyPragmas(db) {
	db.pragma("journal_mode = WAL");
	db.pragma("synchronous = NORMAL");
	db.pragma(`cache_size = -${CACHE_KB}`);
	db.pragma("busy_timeout = 5000");
}
/** 建 4 张表（幂等）。仅在 initIndexDb（wiki 显式创建）时调用。 */
function initSchema(db) {
	db.exec(`CREATE VIRTUAL TABLE IF NOT EXISTS wiki_fts USING fts5(
       page_id UNINDEXED,
       title_tok,
       content_tok,
       tokenize = 'unicode61 remove_diacritics 0'
     );`);
	db.exec(`CREATE TABLE IF NOT EXISTS page_meta (
       page_id   TEXT PRIMARY KEY,
       title     TEXT,
       type      TEXT,
       rel_path  TEXT,
       snippet   TEXT
     );`);
	db.exec(`CREATE TABLE IF NOT EXISTS graph_edge (
       source_id TEXT NOT NULL,
       target_id TEXT NOT NULL,
       PRIMARY KEY (source_id, target_id)
     );`);
	db.exec(`CREATE TABLE IF NOT EXISTS source (
       filename          TEXT PRIMARY KEY,
       sha256            TEXT NOT NULL,
       size              INTEGER NOT NULL,
       status            TEXT NOT NULL,
       created_at        TEXT NOT NULL,
       updated_at        TEXT NOT NULL,
       last_modified_by  TEXT,
       ingested_at       TEXT,
       ingest_error      TEXT
     );`);
}
function dbPath(wikiDir) {
	return join(wikiDir, "index.db");
}
/**
* ★ 显式建库：在 wiki 创建接口里调一次，建好 4 张表。幂等（IF NOT EXISTS）。
* 此后 getReadDb / withWriteDb 只打开已存在的库、不建表。
*/
function initIndexDb(wikiDir) {
	const db = new Database(dbPath(wikiDir));
	applyPragmas(db);
	try {
		initSchema(db);
		db.pragma("wal_checkpoint(TRUNCATE)");
	} finally {
		db.close();
	}
}
/**
* 读连接（search/graph）：走池、复用。库必须已由 initIndexDb 建好。
* 库不存在 → 抛错（视为"wiki 未正确创建/数据损坏"，不静默 lazy 建）。
*/
function getReadDb(wikiId, wikiDir) {
	let db = readPool.get(wikiId);
	if (!db || !db.open) {
		const path = dbPath(wikiDir);
		if (!existsSync(path)) throw new Error(`index.db missing (wiki not created?): ${wikiId}`);
		db = new Database(path, { readonly: false });
		applyPragmas(db);
		readPool.set(wikiId, db);
	}
	return db;
}
/**
* 写连接（ingest/sync/rawWrite）：独立创建，事务内完成后 checkpoint + close，不进池。
* `fn` 内的重建（FTS5 + graph_edge + page_meta + source）在同一事务里原子完成。
*/
function withWriteDb(wikiDir, fn) {
	const path = dbPath(wikiDir);
	if (!existsSync(path)) throw new Error(`index.db missing (wiki not created?): ${wikiDir}`);
	const db = new Database(path);
	applyPragmas(db);
	try {
		const out = db.transaction(fn)(db);
		db.pragma("wal_checkpoint(TRUNCATE)");
		return out;
	} finally {
		db.close();
	}
}
/** wiki 删除：先关读连接（dispose 内部 checkpoint+close），调用方再 rmSync 目录。 */
function evictWikiDb(wikiId) {
	readPool.delete(wikiId);
}
/** 计算内容 SHA-256（增量判断与 source 登记共用同一份 sha）。 */
function sha256(text) {
	return createHash("sha256").update(text, "utf8").digest("hex");
}
/**
* rawWrite 登记 source（设计 §3.4，先查再更新，非盲 UPSERT）。必须在 withWriteDb 事务内调用。
* - 新文件 → INSERT，status=uploaded，last_modified_by=创建人；
* - sha 变化 → UPDATE，**保留 created_at**，重置 uploaded、记最后变更人、清 ingest_error；
* - sha 未变 → 幂等，什么都不动（相同内容重复上传，方案 a）。
*/
function upsertSource(db, entry) {
	const now = (/* @__PURE__ */ new Date()).toISOString();
	const old = db.prepare("SELECT sha256 FROM source WHERE filename = ?").get(entry.filename);
	if (!old) {
		db.prepare(`INSERT INTO source(filename, sha256, size, status, created_at, updated_at, last_modified_by, ingested_at, ingest_error)
       VALUES (?, ?, ?, 'uploaded', ?, ?, ?, NULL, NULL)`).run(entry.filename, entry.sha256, entry.size, now, now, entry.userId ?? null);
		return "created";
	}
	if (old.sha256 !== entry.sha256) {
		db.prepare(`UPDATE source SET sha256 = ?, size = ?, status = 'uploaded', updated_at = ?, last_modified_by = ?, ingest_error = NULL
       WHERE filename = ?`).run(entry.sha256, entry.size, now, entry.userId ?? null, entry.filename);
		return "updated";
	}
	return "unchanged";
}
/** 读全部 source 行（rawLs），按 filename 排序。 */
function listSources(db) {
	return db.prepare(`SELECT filename, sha256, size, status, created_at, updated_at, last_modified_by, ingested_at, ingest_error
       FROM source ORDER BY filename`).all();
}
/** 读 filename → {sha256, status} 映射（增量判断用）。 */
function readSourceStates(db) {
	const rows = db.prepare("SELECT filename, sha256, status FROM source").all();
	const m = /* @__PURE__ */ new Map();
	for (const r of rows) m.set(r.filename, {
		sha256: r.sha256,
		status: r.status
	});
	return m;
}
/** 删除 source 行（rawRm / ingest 时文件已消失）。在事务内调用。 */
function deleteSources(db, filenames) {
	if (filenames.length === 0) return;
	const stmt = db.prepare("DELETE FROM source WHERE filename = ?");
	for (const fn of filenames) stmt.run(fn);
}
/**
* ingest 后登记单个源的抽取结果（设计 §3.6 step 6，在索引重建同事务内调用）。
* - 已有行：只更新 status/ingested_at/ingest_error，**不动** created_at/updated_at/sha256/size
*   （sha 由 rawWrite 维护、内容未变；updated_at 表示"内容变更"，抽取不算内容变更）；
* - 无行（源文件未经 rawWrite 直接落盘）：以磁盘现值 INSERT（created_at=updated_at=now）。
* ok=true → ingested + ingested_at；ok=false → failed + ingest_error。
*/
function recordSourceIngestResult(db, entry) {
	const now = (/* @__PURE__ */ new Date()).toISOString();
	const status = entry.ok ? "ingested" : "failed";
	const ingestedAt = entry.ok ? now : null;
	const ingestError = entry.ok ? null : (entry.error ?? "unknown").slice(0, 500);
	if (db.prepare("SELECT 1 FROM source WHERE filename = ?").get(entry.filename)) db.prepare("UPDATE source SET status = ?, ingested_at = ?, ingest_error = ? WHERE filename = ?").run(status, ingestedAt, ingestError, entry.filename);
	else db.prepare(`INSERT INTO source(filename, sha256, size, status, created_at, updated_at, last_modified_by, ingested_at, ingest_error)
       VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)`).run(entry.filename, entry.sha256, entry.size, status, now, now, ingestedAt, ingestError);
}
/**
* 增量分类（设计 §3.6 step 3，纯函数，便于单测）：
* 对比"磁盘源文件"与"source 表上次状态"，判定各文件的去向。
* - toIngest：新增 || 未成功抽取（status≠ingested，含 uploaded/failed）|| sha 变化 → 需抽取；
* - skipped ：status=ingested 且 sha 未变 → 跳过 LLM（省 token）；
* - deleted ：表中有但磁盘已无 → 待级联删除 + 删 source 行。
*/
function classifySources(disk, oldStates) {
	const diskNames = new Set(disk.map((d) => d.filename));
	const deleted = [...oldStates.keys()].filter((fn) => !diskNames.has(fn));
	const toIngest = [];
	const skipped = [];
	for (const d of disk) {
		const prev = oldStates.get(d.filename);
		if (!prev || prev.status !== "ingested" || prev.sha256 !== d.sha256) toIngest.push(d.filename);
		else skipped.push(d.filename);
	}
	return {
		toIngest,
		skipped,
		deleted
	};
}
//#endregion
//#region src/store/wiki-service.ts
/**
* WikiService — wiki 资产的异步编排（与 CodeGraphService 对称）。
*
* IKnowledgeStore（元数据/状态）+ BuildQueue（后台串行）+ 可注入 worker
* （实际 ingest / 建索引）。状态机：pending → processing(scanning/ingesting)
* → ready / failed(+sync_error)。memory + team 隔离、幂等（同 memory+team+name 返回已存在）、
* 软删 + 清目录。物理目录 {dataRoot}/{service_id}/{team_id}/{wiki_id}/（001 多租户）。
*
* 文件层（11 文档定稿）：raw / page 各一套 ls/read/write/rm，对齐 L2 Scenario。
* - raw/* 仅操作 raw/sources/，不触发 ingest。
* - page/* 操作 wiki/，写入自动注入 frontmatter `locked: true`，删除调
*   lib 层 cascadeDeleteWikiPagesWithRefs 做引用级联。
*/
const PAGE_WRITE_MAX_BYTES = 512 * 1024;
const RAW_WRITE_MAX_BYTES = 5 * 1024 * 1024;
const PAGE_RM_MAX = 20;
const RAW_RM_MAX = 50;
const RAW_READ_MAX = 50;
const RAW_WRITE_MAX = 50;
const PAGE_READ_MAX = 20;
const PAGE_WRITE_MAX = 20;
/** wiki/ 下不允许 page/write 与 page/rm 触碰的结构性文件（去掉 .md 也算）。 */
const PAGE_FORBIDDEN_REFS = new Set([
	"index",
	"schema",
	"purpose",
	"wiki/index",
	"wiki/schema",
	"wiki/purpose"
]);
var WikiService = class {
	store;
	dataRoot;
	worker;
	queue;
	logger;
	callbackConfig;
	/**
	* In-flight delete 标记：delete 命中一个正在排队/执行的 wiki 时置位，
	* worker 在检查点读取以决定中止。仅内存态（同 id 由 SerialQueue 串行 +
	* Node 单线程，读写无并发）。清理收尾后移除。
	*/
	cancelled = /* @__PURE__ */ new Set();
	constructor(opts) {
		this.store = opts.store;
		this.dataRoot = opts.dataRoot;
		this.worker = opts.worker;
		this.queue = opts.queue ?? new BuildQueue();
		this.logger = opts.logger;
		this.callbackConfig = opts.callbackConfig;
	}
	dirFor(serviceId, teamId, wikiId) {
		return join(this.dataRoot, serviceId, teamId, wikiId);
	}
	/**
	* 创建 wiki 元数据 + 目录壳。**不自动 ingest**。
	* 幂等：同 (service_id, team_id, name) 返回已有行。
	*/
	create(params) {
		const { row, existed } = this.store.createWiki(params);
		if (!existed) {
			const dir = this.dirFor(row.service_id, row.team_id, row.wiki_id);
			mkdirSync(join(dir, "raw", "sources"), { recursive: true });
			try {
				initIndexDb(dir);
			} catch (err) {
				this.logger?.warn?.(`[wiki] initIndexDb failed for ${row.wiki_id}: ${String(err)}`);
			}
			this.audit(row, "create", `create wiki ${row.name}`, params.user_id);
		}
		return {
			row,
			existed
		};
	}
	/** Persist service_url for a wiki. Returns updated row or null. */
	updateServiceUrl(serviceId, wikiId, serviceUrl) {
		this.store.updateWikiStatus(serviceId, wikiId, { service_url: serviceUrl });
		return this.store.getWikiById(serviceId, wikiId);
	}
	/** Update wiki metadata (name, summary). Returns updated row or null. */
	updateMeta(serviceId, wikiId, patch) {
		return this.store.updateWikiMeta(serviceId, wikiId, patch);
	}
	/**
	* 显式触发 ingest（LLM 加工 raw → page + 建索引）。
	* 立即返回，后台异步执行。memory/team 不匹配返回 not_found；pending/processing 返回 busy。
	*/
	ingest(serviceId, teamId, wikiId, requesterUserId) {
		const row = this.store.getWiki(serviceId, teamId, wikiId);
		if (!row) return { kind: "not_found" };
		if (row.status === "pending" || row.status === "processing") return {
			kind: "busy",
			status: row.status,
			step: row.internal_status
		};
		const nextVersion = row.version + 1;
		this.store.updateWikiStatus(serviceId, wikiId, {
			status: "pending",
			internal_status: null,
			sync_error: null,
			version: nextVersion
		});
		this.audit({
			...row,
			version: nextVersion
		}, "ingest", "manual ingest", requesterUserId);
		const fresh = this.store.getWiki(serviceId, teamId, wikiId);
		if (fresh) this.enqueueBuild(fresh);
		return fresh ? {
			kind: "ok",
			row: fresh
		} : { kind: "not_found" };
	}
	/** sync 语义 = 重跑 ingest（管控显式触发）。 */
	sync(serviceId, teamId, wikiId, requesterUserId) {
		return this.ingest(serviceId, teamId, wikiId, requesterUserId);
	}
	get(serviceId, teamId, wikiId) {
		return this.store.getWiki(serviceId, teamId, wikiId);
	}
	/** 按全局唯一 wiki_id 查询（仍按 service_id 收敛防跨租户）。spec id-only 端点专用。 */
	getById(serviceId, wikiId) {
		return this.store.getWikiById(serviceId, wikiId);
	}
	list(serviceId, teamId, opts) {
		return this.store.listWikis(serviceId, teamId, opts);
	}
	count(serviceId, teamId, opts) {
		return this.store.countWikis(serviceId, teamId, opts);
	}
	/**
	* 删除 wiki（008 / 007 §5.5）。任何状态均可删（含 pending/processing）。
	* memory/team 不匹配返回 false；否则硬删 + 四类资源清理，返回 true。
	*
	* 若资源正在排队/执行，先置 cancelled 标记通知 worker 在检查点中止，随后立即
	* 硬删 + 清理（不等 worker）。worker 结束前重查发现已删则跳过 ready/回调并再做
	* 一次幂等清理，无残留。
	*/
	delete(serviceId, teamId, wikiId) {
		const row = this.store.getWiki(serviceId, teamId, wikiId);
		if (!row) return false;
		if (row.status === "pending" || row.status === "processing") this.cancelled.add(wikiId);
		this.audit(row, "delete", null);
		this.cleanupResources(serviceId, teamId, wikiId);
		return true;
	}
	/**
	* 四类资源幂等清理（顺序：先释放连接，再删盘）。每步独立 try/catch，异常安全。
	*   1. index.db 读连接池：evictWikiDb（幂等；worker 的 withWriteDb finally 本就 close 写连接）
	*   2. 元数据行：硬删（命中 0 行也安全，支持 worker + delete 双重清理）
	*   3. 磁盘目录（wiki/ raw/ index.db 及 -wal/-shm）：rmSync recursive+force（幂等）
	* BuildQueue 排队任务由 runBuild 入口检查 cancelled/行存在性跳过，无需在此处理。
	*/
	cleanupResources(serviceId, teamId, wikiId) {
		try {
			evictWikiDb(wikiId);
		} catch (err) {
			this.logger?.warn?.(`[wiki] evict index.db failed ${wikiId}: ${String(err)}`);
		}
		try {
			this.store.deleteWiki(serviceId, teamId, wikiId);
		} catch (err) {
			this.logger?.warn?.(`[wiki] hard-delete row failed ${wikiId}: ${String(err)}`);
		}
		try {
			rmSync(this.dirFor(serviceId, teamId, wikiId), {
				recursive: true,
				force: true
			});
		} catch (err) {
			this.logger?.warn?.(`[wiki] rm dir failed ${wikiId}: ${String(err)}`);
		}
	}
	/**
	* worker 检查点：wiki 是否已被删除（cancelled 标记命中，或行已不在库）。
	* 双判据覆盖 delete-during-run 与 delete-already-done 两种时序。
	*/
	isDeleted(serviceId, wikiId) {
		return this.cancelled.has(wikiId) || this.store.getWikiById(serviceId, wikiId) === null;
	}
	/**
	* worker 检查点判定“已删”后的收尾：幂等清理 worker 可能刚写下的盘/连接，
	* 并移除 cancelled 标记。
	*/
	finishCancelled(serviceId, teamId, wikiId) {
		this.cleanupResources(serviceId, teamId, wikiId);
		this.cancelled.delete(wikiId);
		this.logger?.info?.(`[wiki] ${wikiId} build aborted (deleted during processing)`);
	}
	/** 写一条 wiki 审计记录。失败不阻断主流程。 */
	audit(row, action, detail, requesterUserId) {
		try {
			this.store.appendWikiAudit({
				service_id: row.service_id,
				asset_id: row.wiki_id,
				version: row.version,
				action,
				user_id: requesterUserId ?? row.user_id,
				agent_id: row.agent_id,
				detail
			});
		} catch (err) {
			this.logger?.warn?.(`[wiki] audit ${action} failed: ${String(err)}`);
		}
	}
	/** 列出 raw/sources/ 下的素材文件（改查 source 表，设计 003 §3.5）。wiki 不存在返回 null。 */
	rawLs(serviceId, teamId, wikiId) {
		if (!this.store.getWiki(serviceId, teamId, wikiId)) return null;
		const dir = this.dirFor(serviceId, teamId, wikiId);
		try {
			return listSources(getReadDb(wikiId, dir)).map((s) => ({
				filename: s.filename,
				size: s.size,
				status: s.status,
				created_at: s.created_at,
				updated_at: s.updated_at,
				last_modified_by: s.last_modified_by,
				ingested_at: s.ingested_at,
				uploaded_at: s.created_at
			}));
		} catch {
			return [];
		}
	}
	/** 读单个 raw 文件原文。文件不存在返回 null（含 wiki 不存在）。 */
	rawRead(serviceId, teamId, wikiId, filename) {
		if (!this.store.getWiki(serviceId, teamId, wikiId)) return null;
		const sourcesDir = join(this.dirFor(serviceId, teamId, wikiId), "raw", "sources");
		const safe = this.resolveRawPath(sourcesDir, filename);
		if (!safe) return null;
		try {
			return readFileSync(safe, "utf-8");
		} catch {
			return null;
		}
	}
	/**
	* 批量读 raw 文件。
	* - wiki 不存在 → null
	* - 任一 filename 路径穿越 → "invalid_path"
	* - 超 RAW_READ_MAX → 抛错（router 转 400）
	* 单个文件不存在不报错，对应 item 标 not_found:true（spec：整体仍 200）。
	*/
	rawReadMany(serviceId, teamId, wikiId, filenames) {
		if (!this.store.getWiki(serviceId, teamId, wikiId)) return null;
		if (filenames.length > RAW_READ_MAX) throw new Error(`filenames exceeds max ${RAW_READ_MAX}`);
		const sourcesDir = join(this.dirFor(serviceId, teamId, wikiId), "raw", "sources");
		const safePaths = [];
		for (const fn of filenames) {
			const safe = this.resolveRawPath(sourcesDir, fn);
			if (!safe) return "invalid_path";
			safePaths.push(safe);
		}
		const items = [];
		for (let i = 0; i < filenames.length; i++) {
			const filename = filenames[i];
			try {
				const content = readFileSync(safePaths[i], "utf-8");
				items.push({
					filename,
					content
				});
			} catch {
				items.push({
					filename,
					not_found: true
				});
			}
		}
		return items;
	}
	/**
	* 写入/覆盖单个 raw 文件（upsert）+ 登记 source 表（设计 003 §3.4）。
	* - wiki 不存在 → null
	* - processing 中 → "processing"
	* - 路径穿越 → "invalid_path"
	* - 超 5MB → "too_large"
	*/
	rawWrite(serviceId, teamId, wikiId, filename, content, userId) {
		const row = this.store.getWiki(serviceId, teamId, wikiId);
		if (!row) return null;
		if (row.status === "processing") return "processing";
		const size = Buffer.byteLength(content, "utf-8");
		if (size > RAW_WRITE_MAX_BYTES) return "too_large";
		const sourcesDir = join(this.dirFor(serviceId, teamId, wikiId), "raw", "sources");
		const safe = this.resolveRawPath(sourcesDir, filename);
		if (!safe) return "invalid_path";
		mkdirSync(sourcesDir, { recursive: true });
		writeFileSync(safe, content, "utf-8");
		this.registerSources(serviceId, teamId, wikiId, [{
			filename,
			content,
			size
		}], userId);
		return {
			filename,
			size
		};
	}
	/**
	* 批量写入 raw 文件（整批原子）。
	* - 先全部校验：路径穿越 → "invalid_path"；任一项超 5MB → "too_large"
	* - 全部通过后逐文件落盘；任一落盘失败回滚之前已写文件（删原有的不在请求里
	*   的文件），保证整批要么都成功要么都没生效。
	* 错误码同 rawWrite。
	*/
	rawWriteMany(serviceId, teamId, wikiId, files, userId) {
		const row = this.store.getWiki(serviceId, teamId, wikiId);
		if (!row) return null;
		if (row.status === "processing") return "processing";
		if (files.length > RAW_WRITE_MAX) throw new Error(`files exceeds max ${RAW_WRITE_MAX}`);
		const sourcesDir = join(this.dirFor(serviceId, teamId, wikiId), "raw", "sources");
		const plans = [];
		for (const { filename, content } of files) {
			if (typeof content !== "string") return "invalid_path";
			const size = Buffer.byteLength(content, "utf-8");
			if (size > RAW_WRITE_MAX_BYTES) return "too_large";
			const safe = this.resolveRawPath(sourcesDir, filename);
			if (!safe) return "invalid_path";
			let pre = null;
			try {
				pre = readFileSync(safe, "utf-8");
			} catch {
				pre = null;
			}
			plans.push({
				filename,
				safePath: safe,
				content,
				size,
				preExistingContent: pre
			});
		}
		mkdirSync(sourcesDir, { recursive: true });
		const written = [];
		try {
			for (const p of plans) {
				writeFileSync(p.safePath, p.content, "utf-8");
				written.push(p);
			}
		} catch (err) {
			for (const p of written) try {
				if (p.preExistingContent === null) rmSync(p.safePath, { force: true });
				else writeFileSync(p.safePath, p.preExistingContent, "utf-8");
			} catch {}
			throw err;
		}
		this.registerSources(serviceId, teamId, wikiId, plans.map((p) => ({
			filename: p.filename,
			content: p.content,
			size: p.size
		})), userId);
		return plans.map(({ filename, size }) => ({
			filename,
			size
		}));
	}
	/**
	* 批量删除 raw 文件 + 级联清理下游 page。
	* 调用 lib 层 deleteSourceFiles，由其内部决定 page 命运。
	* - wiki 不存在 → null
	* - processing → "processing"
	* - filenames 含路径穿越 → "invalid_path"
	* - 超 50 → 抛错（由 router 转 400）
	*/
	async rawRm(serviceId, teamId, wikiId, filenames) {
		const row = this.store.getWiki(serviceId, teamId, wikiId);
		if (!row) return null;
		if (row.status === "processing") return "processing";
		if (filenames.length > RAW_RM_MAX) throw new Error(`filenames exceeds max ${RAW_RM_MAX}`);
		const projectPath = this.dirFor(serviceId, teamId, wikiId);
		const sourcesDir = join(projectPath, "raw", "sources");
		const fullPaths = [];
		for (const fn of filenames) {
			const safe = this.resolveRawPath(sourcesDir, fn);
			if (!safe) return "invalid_path";
			fullPaths.push(safe);
		}
		const { deleteSourceFiles } = await import("./cascade-Cq0KJ3OW.mjs");
		const result = await deleteSourceFiles(projectPath, fullPaths, { logReason: "wiki/raw/rm" });
		try {
			initIndexDb(projectPath);
			withWriteDb(projectPath, (db) => deleteSources(db, filenames));
		} catch (err) {
			this.logger?.warn?.(`[wiki] source rows delete failed: ${String(err)}`);
		}
		return {
			deleted_files: filenames,
			deleted_pages: result.deletedWikiPaths.map((p) => this.absToPageRef(projectPath, p)),
			rewritten_pages: result.rewrittenSourcePages
		};
	}
	/**
	* 列出 wiki/ 下的 page 文件（recursive 扫描 .md 取 frontmatter）。
	* status≠ready 时返回空数组。
	*/
	pageLs(serviceId, teamId, wikiId) {
		const row = this.store.getWiki(serviceId, teamId, wikiId);
		if (!row) return null;
		if (row.status !== "ready") return [];
		const wikiDir = join(this.dirFor(serviceId, teamId, wikiId), "wiki");
		if (!existsSync(wikiDir)) return [];
		const items = [];
		this.scanPagesRecursive(wikiDir, wikiDir, items);
		return items;
	}
	/** 读单个 page 原文。ref 可以是 page id 或 relPath。 */
	pageRead(serviceId, teamId, wikiId, ref) {
		if (!this.store.getWiki(serviceId, teamId, wikiId)) return null;
		const projectPath = this.dirFor(serviceId, teamId, wikiId);
		const safe = this.resolvePageRef(projectPath, ref);
		if (!safe) return null;
		try {
			return readFileSync(safe, "utf-8");
		} catch {
			return null;
		}
	}
	/**
	* 批量读 page 原文。
	* - wiki 不存在 → null
	* - 任一 ref 路径穿越 → "invalid_path"
	* - 超 PAGE_READ_MAX → 抛错
	* 单个 ref 不存在不报错，对应 item 标 not_found:true（spec：整体仍 200）。
	*/
	pageReadMany(serviceId, teamId, wikiId, refs) {
		if (!this.store.getWiki(serviceId, teamId, wikiId)) return null;
		if (refs.length > PAGE_READ_MAX) throw new Error(`refs exceeds max ${PAGE_READ_MAX}`);
		const projectPath = this.dirFor(serviceId, teamId, wikiId);
		const safePaths = [];
		for (const r of refs) {
			const safe = this.resolvePageRef(projectPath, r, { allowMissing: true });
			if (!safe) return "invalid_path";
			safePaths.push(safe);
		}
		const items = [];
		for (let i = 0; i < refs.length; i++) {
			const ref = refs[i];
			try {
				const content = readFileSync(safePaths[i], "utf-8");
				items.push({
					ref,
					content
				});
			} catch {
				items.push({
					ref,
					not_found: true
				});
			}
		}
		return items;
	}
	/**
	* 写入/覆盖单个 page（upsert）。自动在 frontmatter 注入 `locked: true`。
	* - wiki 不存在 → null
	* - processing → "processing"
	* - 路径穿越 → "invalid_path"
	* - 结构性文件 → "forbidden_path"
	* - 超 512KB → "too_large"
	*/
	pageWrite(serviceId, teamId, wikiId, ref, content) {
		const row = this.store.getWiki(serviceId, teamId, wikiId);
		if (!row) return null;
		if (row.status === "processing") return "processing";
		if (Buffer.byteLength(content, "utf-8") > PAGE_WRITE_MAX_BYTES) return "too_large";
		if (this.isForbiddenPageRef(ref)) return "forbidden_path";
		const projectPath = this.dirFor(serviceId, teamId, wikiId);
		const safe = this.resolvePageRef(projectPath, ref, { allowMissing: true });
		if (!safe) return "invalid_path";
		const { content: finalContent, lockedInjected } = injectLockedTrue(content);
		mkdirSync(join(safe, ".."), { recursive: true });
		writeFileSync(safe, finalContent, "utf-8");
		return {
			ref,
			locked_injected: lockedInjected
		};
	}
	/**
	* 批量写 page（整批原子）。每项自动注入 frontmatter `locked: true`。
	* - 先全部校验：处理中 → "processing"；路径穿越 → "invalid_path"；
	*   结构性文件 → "forbidden_path"；超 512KB → "too_large"
	* - 全部通过后逐文件落盘；任一失败回滚已写文件。
	*/
	pageWriteMany(serviceId, teamId, wikiId, pages) {
		const row = this.store.getWiki(serviceId, teamId, wikiId);
		if (!row) return null;
		if (row.status === "processing") return "processing";
		if (pages.length > PAGE_WRITE_MAX) throw new Error(`pages exceeds max ${PAGE_WRITE_MAX}`);
		const projectPath = this.dirFor(serviceId, teamId, wikiId);
		const plans = [];
		for (const { ref, content } of pages) {
			if (typeof content !== "string") return "invalid_path";
			if (this.isForbiddenPageRef(ref)) return "forbidden_path";
			if (Buffer.byteLength(content, "utf-8") > PAGE_WRITE_MAX_BYTES) return "too_large";
			const safe = this.resolvePageRef(projectPath, ref, { allowMissing: true });
			if (!safe) return "invalid_path";
			const { content: finalContent, lockedInjected } = injectLockedTrue(content);
			let pre = null;
			try {
				pre = readFileSync(safe, "utf-8");
			} catch {
				pre = null;
			}
			plans.push({
				ref,
				safePath: safe,
				finalContent,
				lockedInjected,
				preExistingContent: pre
			});
		}
		const written = [];
		try {
			for (const p of plans) {
				mkdirSync(join(p.safePath, ".."), { recursive: true });
				writeFileSync(p.safePath, p.finalContent, "utf-8");
				written.push(p);
			}
		} catch (err) {
			for (const p of written) try {
				if (p.preExistingContent === null) rmSync(p.safePath, { force: true });
				else writeFileSync(p.safePath, p.preExistingContent, "utf-8");
			} catch {}
			throw err;
		}
		return plans.map(({ ref, lockedInjected }) => ({
			ref,
			locked_injected: lockedInjected
		}));
	}
	/**
	* 批量删除 page + 级联清理引用。调用 lib 层 cascadeDeleteWikiPagesWithRefs。
	* - wiki 不存在 → null
	* - processing → "processing"
	* - 含路径穿越 → "invalid_path"
	* - 含结构性文件 → "forbidden_path"
	* - 超 20 → 抛错
	*/
	async pageRm(serviceId, teamId, wikiId, refs) {
		const row = this.store.getWiki(serviceId, teamId, wikiId);
		if (!row) return null;
		if (row.status === "processing") return "processing";
		if (refs.length > PAGE_RM_MAX) throw new Error(`refs exceeds max ${PAGE_RM_MAX}`);
		const projectPath = this.dirFor(serviceId, teamId, wikiId);
		const fullPaths = [];
		for (const r of refs) {
			if (this.isForbiddenPageRef(r)) return "forbidden_path";
			const safe = this.resolvePageRef(projectPath, r);
			if (!safe) return "invalid_path";
			fullPaths.push(safe);
		}
		const { cascadeDeleteWikiPagesWithRefs } = await import("./cascade-Cq0KJ3OW.mjs");
		const result = await cascadeDeleteWikiPagesWithRefs(projectPath, fullPaths);
		return {
			deleted_pages: result.deletedPaths.map((p) => this.absToPageRef(projectPath, p)),
			rewritten_files: result.rewrittenFiles
		};
	}
	/**
	* 登记一批源文件到 source 表（rawWrite/rawWriteMany 用）。
	* 保证 index.db 存在（幂等 initIndexDb），在一个写事务里对每个文件 upsertSource
	* （先查再更新：新建 uploaded / sha 变则重置 uploaded / sha 未变幂等）。
	* 登记失败不阻断写盘主流程（文件已落盘）——记 warn，交由后续 ingest/rawLs 兜底。
	*/
	registerSources(serviceId, teamId, wikiId, files, userId) {
		const dir = this.dirFor(serviceId, teamId, wikiId);
		try {
			initIndexDb(dir);
			withWriteDb(dir, (db) => {
				for (const f of files) upsertSource(db, {
					filename: f.filename,
					sha256: sha256(f.content),
					size: f.size,
					userId: userId ?? null
				});
			});
		} catch (err) {
			this.logger?.warn?.(`[wiki] source register failed for ${wikiId}: ${String(err)}`);
		}
	}
	resolveRawPath(sourcesDir, filename) {
		if (!filename || filename.includes("..") || filename.startsWith("/")) return null;
		const normalized = normalize(filename);
		if (normalized.startsWith("..") || normalized.startsWith("/")) return null;
		const base = resolve(sourcesDir);
		const safe = resolve(base, normalized);
		const dirWithSep = base.endsWith("/") ? base : base + "/";
		if (safe !== base && !safe.startsWith(dirWithSep)) return null;
		return safe;
	}
	/**
	* 解析 page ref（id 或 relPath）→ 绝对路径。要求落在 wiki/ 子树下。
	* - allowMissing=true 用于 write，路径不存在仍允许
	* - allowMissing=false 用于 read/rm，要求文件已存在
	*/
	resolvePageRef(projectPath, ref, opts = {}) {
		if (!ref || ref.includes("..") || ref.startsWith("/")) return null;
		const cleanRef = ref.replace(/^wiki\//, "");
		if (cleanRef.includes("..")) return null;
		const wikiDir = resolve(projectPath, "wiki");
		const wikiDirSep = wikiDir.endsWith("/") ? wikiDir : wikiDir + "/";
		const candidates = cleanRef.endsWith(".md") ? [cleanRef] : [cleanRef + ".md", cleanRef];
		for (const c of candidates) {
			const safe = resolve(wikiDir, c);
			if (safe !== wikiDir && !safe.startsWith(wikiDirSep)) continue;
			if (opts.allowMissing) return c.endsWith(".md") ? safe : null;
			if (existsSync(safe)) return safe;
		}
		if (opts.allowMissing) {
			const safe = resolve(wikiDir, cleanRef.endsWith(".md") ? cleanRef : cleanRef + ".md");
			if (safe === wikiDir || !safe.startsWith(wikiDirSep)) return null;
			return safe;
		}
		return null;
	}
	/** 把 wiki/.../page.md 绝对路径转换回 ref（如 "concepts/redis"）。 */
	absToPageRef(projectPath, abs) {
		const wikiDir = resolve(projectPath, "wiki");
		const prefix = wikiDir.endsWith("/") ? wikiDir : wikiDir + "/";
		if (!abs.startsWith(prefix)) return abs;
		return abs.slice(prefix.length).replace(/\.md$/, "");
	}
	isForbiddenPageRef(ref) {
		const cleanRef = ref.replace(/^wiki\//, "").replace(/\.md$/, "");
		return PAGE_FORBIDDEN_REFS.has(cleanRef) || PAGE_FORBIDDEN_REFS.has(`wiki/${cleanRef}`);
	}
	scanPagesRecursive(baseDir, dir, out) {
		if (!existsSync(dir)) return;
		for (const entry of readdirSync(dir)) {
			const full = join(dir, entry);
			let st;
			try {
				st = statSync(full);
			} catch {
				continue;
			}
			if (st.isDirectory()) {
				if (entry === "media") continue;
				this.scanPagesRecursive(baseDir, full, out);
				continue;
			}
			if (!entry.endsWith(".md")) continue;
			let content = "";
			try {
				content = readFileSync(full, "utf-8");
			} catch {
				continue;
			}
			const rel = full.slice(baseDir.length + 1).replace(/\\/g, "/");
			const id = rel.replace(/\.md$/, "");
			const fm = parseFrontmatterMin(content);
			out.push({
				id,
				title: fm.title || entry.replace(/\.md$/, "").replace(/-/g, " "),
				type: fm.type || "other",
				path: `wiki/${rel}`,
				...fm.description ? { description: fm.description } : {},
				locked: fm.locked
			});
		}
	}
	enqueueBuild(row) {
		this.queue.enqueue(row.wiki_id, () => this.runBuild(row.service_id, row.wiki_id, row.team_id, row.name));
	}
	async runBuild(serviceId, wikiId, teamId, name) {
		if (this.isDeleted(serviceId, wikiId)) {
			this.finishCancelled(serviceId, teamId, wikiId);
			return;
		}
		this.store.updateWikiStatus(serviceId, wikiId, {
			status: "processing",
			internal_status: "scanning",
			sync_error: null
		});
		const ingestRunId = randomUUID();
		try {
			const result = await this.worker({
				wikiId,
				serviceId,
				teamId,
				name,
				dir: this.dirFor(serviceId, teamId, wikiId),
				setInternalStatus: (s) => this.store.updateWikiStatus(serviceId, wikiId, {
					status: "processing",
					internal_status: s
				}),
				ingestRunId
			});
			if (this.isDeleted(serviceId, wikiId)) {
				this.finishCancelled(serviceId, teamId, wikiId);
				return;
			}
			this.store.updateWikiStatus(serviceId, wikiId, {
				status: "ready",
				internal_status: null,
				sync_error: null,
				page_count: result?.pageCount ?? null,
				last_sync_at: (/* @__PURE__ */ new Date()).toISOString()
			});
			const synced = this.store.getWikiById(serviceId, wikiId);
			if (synced) this.audit(synced, "ready", result?.pageCount != null ? `pages: ${result.pageCount}` : null);
			this.logger?.info?.(`[wiki] ${wikiId} ready (pages: ${result?.pageCount ?? "?"})`);
			await this.onBuildComplete(synced, "ready", null, ingestRunId);
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			if (this.isDeleted(serviceId, wikiId)) {
				this.finishCancelled(serviceId, teamId, wikiId);
				return;
			}
			this.store.updateWikiStatus(serviceId, wikiId, {
				status: "failed",
				internal_status: null,
				sync_error: msg.slice(0, 500)
			});
			const failed = this.store.getWikiById(serviceId, wikiId);
			if (failed) this.audit(failed, "failed", msg.slice(0, 500));
			this.logger?.warn?.(`[wiki] ${wikiId} failed: ${msg}`);
			await this.onBuildComplete(failed, "failed", msg, ingestRunId);
		}
	}
	/**
	* Post-build hook: generate summary (if synced) and callback TMC.
	* Never throws — runs after the main build is already committed.
	*/
	async onBuildComplete(row, status, errorMsg, ingestRunId) {
		if (!row || !this.callbackConfig) return;
		let summary = null;
		if (status === "ready") try {
			const pages = this.pageLs(row.service_id, row.team_id, row.wiki_id) ?? [];
			this.logger?.info?.(`[wiki] summary generation start (wikiId=${row.wiki_id}, pages=${pages.length}, status=${status})`);
			const { generateWikiSummary } = await import("./callback-dsWv60u2.mjs");
			summary = await generateWikiSummary(row.wiki_id, row.name, pages.map((p) => ({
				title: p.title,
				description: p.description
			})), this.callbackConfig.resolveLlm(row.service_id));
			this.logger?.info?.(`[wiki] summary generation done (wikiId=${row.wiki_id}, len=${summary?.length ?? 0}, empty=${!summary})`);
			if (summary) this.store.updateWikiStatus(row.service_id, row.wiki_id, { summary });
		} catch (err) {
			this.logger?.warn?.(`[wiki] summary generation failed: ${String(err)}`);
		}
		const { callbackTMC } = await import("./callback-dsWv60u2.mjs");
		await callbackTMC({
			knowledge_id: row.wiki_id,
			service_id: row.service_id,
			type: "wiki",
			status,
			summary,
			sync_error: errorMsg?.slice(0, 500) ?? null,
			timestamp: (/* @__PURE__ */ new Date()).toISOString(),
			...ingestRunId ? { run_id: ingestRunId } : {}
		}, this.callbackConfig);
	}
	async onIdle(wikiId) {
		await this.queue.onIdle(wikiId);
	}
};
/** 极简 frontmatter 解析（取 title/type/description/locked），与 manager 一致风格。 */
function parseFrontmatterMin(content) {
	const fmMatch = content.match(/^---\n([\s\S]*?)\n---/);
	const fm = fmMatch ? fmMatch[1] : "";
	const titleMatch = fm.match(/^title:\s*["']?(.+?)["']?\s*$/m);
	const typeMatch = fm.match(/^type:\s*["']?(.+?)["']?\s*$/m);
	const descMatch = fm.match(/^description:\s*["']?(.+?)["']?\s*$/m);
	const lockedMatch = fm.match(/^locked:\s*(true|false)\s*$/m);
	return {
		title: titleMatch ? titleMatch[1].trim() : "",
		type: typeMatch ? typeMatch[1].trim().toLowerCase() : "",
		description: descMatch ? descMatch[1].trim() : "",
		locked: lockedMatch ? lockedMatch[1] === "true" : false
	};
}
/**
* 在 frontmatter 中注入 `locked: true`：
* - 有 frontmatter：若已有 locked: 字段，强制改 true；否则在 frontmatter 末尾追加一行
* - 无 frontmatter：在文件最前面包一段 frontmatter（仅含 locked: true）
*
* 返回 { content, lockedInjected }；lockedInjected 表示**本次**是否真正补/改了 locked
* 字段（已是 true 也算 lockedInjected=false，因为没有改动）。
*/
function injectLockedTrue(content) {
	const fmMatch = content.match(/^---\n([\s\S]*?)\n---\n?/);
	if (!fmMatch) return {
		content: `---\nlocked: true\n---\n${content.startsWith("\n") ? content.slice(1) : content}`,
		lockedInjected: true
	};
	const fmBody = fmMatch[1];
	const lockedMatch = fmBody.match(/^locked:\s*(true|false)\s*$/m);
	if (lockedMatch) {
		if (lockedMatch[1] === "true") return {
			content,
			lockedInjected: false
		};
		const newFmBody = fmBody.replace(/^locked:\s*(true|false)\s*$/m, "locked: true");
		return {
			content: content.replace(fmBody, newFmBody),
			lockedInjected: true
		};
	}
	const newFmBody = fmBody.endsWith("\n") ? `${fmBody}locked: true` : `${fmBody}\nlocked: true`;
	return {
		content: content.replace(fmBody, newFmBody),
		lockedInjected: true
	};
}
//#endregion
//#region src/store/auto-sync-scheduler.ts
/**
* AutoSyncScheduler — 定时拉取 git 仓库并更新 codegraph 索引。
*
* 模型：FIFO 队列 + 定长 worker pool。
*   - Scanner：每 scanIntervalMs 扫描一次所有 ready 状态的仓库；
*     用 Set 去重（已入队或 worker 处理中的跳过）后 push 到内存 queue。
*   - Workers：常驻 maxConcurrentSyncs 个协程，FIFO 从 queue 取任务调
*     CodeGraphService.sync()；队列空时轮询等待，stop 后自然退出。
*
* 单仓库同步频率 = max(单次 sync 耗时, scanIntervalMs)。无额外冷却字段——
* 想控制频率直接调 SCAN_INTERVAL_MIN。
*
* 设计目标（源自需求）：
*   1. 定时感知 git 仓库更新，自动拉取最新代码并重建 codegraph 索引
*   2. 使用任务队列，避免突发性大量拉取打爆服务（并发受 worker 数硬限）
*   3. 队列内 + 处理中的仓库不重复入队（Set 去重，队列大小上界 = 仓库数）
*   4. 单个仓库同步失败不影响其他仓库（worker 吞异常继续消费）
*   5. 复用 CodeGraphService.sync() 已有的 busy/not_found 拒绝语义
*
* 环境变量配置：
*   - KNOWLEDGE_AUTO_SYNC_ENABLED: 启用开关 (default: false)
*   - KNOWLEDGE_AUTO_SYNC_SCAN_INTERVAL_MIN: 扫描周期（分钟）(default: 10)
*   - KNOWLEDGE_AUTO_SYNC_MAX_CONCURRENT: 全局最大并发同步数 (default: 3)
*/
const log$7 = createLogger("auto-sync-scheduler");
const MIN_MS = 60 * 1e3;
/** worker 空转时的轮询间隔（ms）。测试 fake timer 下也能被 advance。 */
const WORKER_IDLE_POLL_MS = 100;
/**
* 从环境变量解析配置，支持 fallback 默认值。
* 所有数值字段做 clamp 防止不合理配置。
*/
function resolveAutoSyncConfig(env = process.env) {
	const enabled = parseBoolean(env.KNOWLEDGE_AUTO_SYNC_ENABLED, false);
	const scanIntervalMin = clamp$1(parseFloat(env.KNOWLEDGE_AUTO_SYNC_SCAN_INTERVAL_MIN ?? "") || 10, 1, 60);
	const maxConcurrent = clamp$1(parseInt(env.KNOWLEDGE_AUTO_SYNC_MAX_CONCURRENT ?? "") || 3, 1, 20);
	return {
		enabled,
		scanIntervalMs: scanIntervalMin * MIN_MS,
		maxConcurrentSyncs: maxConcurrent
	};
}
var AutoSyncScheduler = class {
	store;
	cgService;
	config;
	/** 启动延迟 + 周期 scan 的 timer。 */
	startupTimer = null;
	scanTimer = null;
	/** worker 空转 sleep 的 timer 集合（stop 时统一清理）。 */
	workerSleepTimers = /* @__PURE__ */ new Set();
	/** FIFO 待处理队列 + 去重 Set（队列内 + 处理中的 id）。 */
	queue = [];
	inFlight = /* @__PURE__ */ new Set();
	/** 当前正在执行 sync 的 worker 数。 */
	activeSyncs = 0;
	/** worker 数（常驻）。 */
	workerCount = 0;
	/** 停止标记。stop 后 workers 循环退出。 */
	stopped = true;
	/** 上一轮 scan 是否仍在进行。 */
	scanning = false;
	constructor(deps) {
		this.store = deps.store;
		this.cgService = deps.cgService;
		this.config = deps.config;
	}
	/**
	* 启动调度：
	*   - 延迟 30s 首扫（让 restore 先完成，避免抢磁盘）
	*   - 周期 scanIntervalMs 扫描
	*   - 启动 maxConcurrentSyncs 个常驻 worker
	*/
	start() {
		if (!this.config.enabled) {
			log$7.info("[auto-sync] disabled by config, skipping start");
			return;
		}
		if (!this.stopped) {
			log$7.warn("[auto-sync] already started");
			return;
		}
		this.stopped = false;
		log$7.info("[auto-sync] starting scheduler", {
			scanIntervalMs: this.config.scanIntervalMs,
			maxConcurrentSyncs: this.config.maxConcurrentSyncs
		});
		for (let i = 0; i < this.config.maxConcurrentSyncs; i++) {
			this.workerCount++;
			this.runWorker(i).finally(() => {
				this.workerCount--;
			});
		}
		const startupDelay = 3e4;
		this.startupTimer = setTimeout(() => {
			this.startupTimer = null;
			if (this.stopped) return;
			this.scan();
			this.scanTimer = setInterval(() => {
				if (this.stopped) return;
				this.scan();
			}, this.config.scanIntervalMs);
		}, startupDelay);
		log$7.info(`[auto-sync] first scan in ${startupDelay / 1e3}s`);
	}
	/** 停止调度器：取消 timer、通知 worker 退出（已在跑的 sync 自然完成）。 */
	stop() {
		this.stopped = true;
		if (this.startupTimer !== null) {
			clearTimeout(this.startupTimer);
			this.startupTimer = null;
		}
		if (this.scanTimer !== null) {
			clearInterval(this.scanTimer);
			this.scanTimer = null;
		}
		for (const t of this.workerSleepTimers) clearTimeout(t);
		this.workerSleepTimers.clear();
		log$7.info("[auto-sync] stopped");
	}
	/** 状态快照（管理 API 使用）。 */
	getStatus() {
		return {
			running: !this.stopped,
			activeSyncs: this.activeSyncs,
			queueLength: this.queue.length,
			scanning: this.scanning
		};
	}
	/** 手动触发一轮扫描（管理 API 使用）。不影响定时周期。disabled 时 no-op。 */
	triggerScan() {
		if (!this.config.enabled) {
			log$7.warn("[auto-sync] cannot trigger: scheduler is disabled");
			return;
		}
		log$7.info("[auto-sync] manual scan triggered");
		this.scan();
	}
	/**
	* 一轮扫描：
	*   1. 列出所有 ready 状态的 code-graph
	*   2. 用 inFlight Set 去重（队列内 / 处理中的不再入队）
	*   3. FIFO push 到 queue，worker 会自动消费
	*/
	async scan() {
		if (this.scanning) {
			log$7.debug("[auto-sync] previous scan still running, skip this round");
			return;
		}
		this.scanning = true;
		try {
			log$7.info("[auto-sync] scan started");
			const candidates = this.listSyncCandidates();
			if (candidates.length === 0) {
				log$7.info("[auto-sync] no ready repos");
				return;
			}
			let enqueued = 0;
			for (const row of candidates) {
				if (this.stopped) break;
				if (this.inFlight.has(row.code_graph_id)) continue;
				this.inFlight.add(row.code_graph_id);
				this.queue.push(row);
				enqueued++;
			}
			log$7.info(`[auto-sync] enqueued ${enqueued} repo(s) (queue=${this.queue.length}, active=${this.activeSyncs})`);
		} catch (err) {
			log$7.error(`[auto-sync] scan error: ${err instanceof Error ? err.message : String(err)}`);
		} finally {
			this.scanning = false;
		}
	}
	/**
	* 列出需要同步的 code-graph：只挑 status = ready 的。
	* 已在队列或 worker 处理中的仓库由 scan() 里的 inFlight Set 去重，不重复入队；
	* 单仓库的同步节奏天然由 max(sync 耗时, scanIntervalMs) 决定，无需额外冷却。
	*/
	listSyncCandidates() {
		const syncedRefs = this.store.listSyncedCodeGraphs();
		if (syncedRefs.length === 0) return [];
		const candidates = [];
		for (const ref of syncedRefs) try {
			const row = this.store.getCodeGraph(ref.service_id, ref.team_id, ref.code_graph_id);
			if (!row) continue;
			if (row.status !== "ready") continue;
			candidates.push(row);
		} catch (err) {
			log$7.warn(`[auto-sync] failed to check ${ref.code_graph_id}: ${err instanceof Error ? err.message : String(err)}`);
		}
		return candidates;
	}
	/**
	* 一个常驻 worker：循环 shift 队列执行 sync；空则短睡后重试。
	* stop() 后 loop 自然退出。异常一律吞掉（记录日志），保证 worker 不死。
	*/
	async runWorker(workerIdx) {
		log$7.debug(`[auto-sync] worker#${workerIdx} started`);
		while (!this.stopped) {
			const row = this.queue.shift();
			if (!row) {
				await this.sleep(WORKER_IDLE_POLL_MS);
				continue;
			}
			this.activeSyncs++;
			try {
				await this.syncOne(row);
			} catch (err) {
				log$7.error(`[auto-sync] worker#${workerIdx} unexpected error: ${err instanceof Error ? err.message : String(err)}`);
			} finally {
				this.activeSyncs--;
				this.inFlight.delete(row.code_graph_id);
			}
		}
		log$7.debug(`[auto-sync] worker#${workerIdx} exiting`);
	}
	/** 对单个 code-graph 执行 sync（复用 CodeGraphService.sync 的判别联合）。 */
	async syncOne(row) {
		const startMs = Date.now();
		log$7.info(`[auto-sync] sync ${row.code_graph_id} (${row.repo_url}@${row.branch})`);
		try {
			const result = await Promise.resolve(this.cgService.sync(row.service_id, row.team_id, row.code_graph_id, void 0));
			const durationMs = Date.now() - startMs;
			switch (result.kind) {
				case "ok":
					log$7.info(`[auto-sync] sync enqueued for ${row.code_graph_id} (took ${durationMs}ms)`);
					break;
				case "busy":
					log$7.debug(`[auto-sync] skip ${row.code_graph_id}: already ${result.status} (step: ${result.step})`);
					break;
				case "not_found":
					log$7.warn(`[auto-sync] skip ${row.code_graph_id}: not found (may have been deleted)`);
					break;
			}
		} catch (err) {
			log$7.error(`[auto-sync] sync failed for ${row.code_graph_id}: ${err instanceof Error ? err.message : String(err)}`);
		}
	}
	/** setTimeout 版 sleep，stop 时统一清理避免测试环境 timer 泄漏。 */
	sleep(ms) {
		return new Promise((resolve) => {
			const t = setTimeout(() => {
				this.workerSleepTimers.delete(t);
				resolve();
			}, ms);
			this.workerSleepTimers.add(t);
		});
	}
};
function parseBoolean(val, fallback) {
	if (val == null || val.trim() === "") return fallback;
	const v = val.trim().toLowerCase();
	return v === "true" || v === "1" || v === "yes" || v === "on";
}
function clamp$1(val, min, max) {
	return Math.max(min, Math.min(max, val));
}
//#endregion
//#region src/engines/wiki/graph-search.ts
const DEFAULT_MAX_NODES = 200;
function graphMultiHopSearch(graph, seeds, opts) {
	const { hop: maxHop, decay, minScore } = opts;
	const maxNodes = opts.maxNodes ?? DEFAULT_MAX_NODES;
	const seedSet = /* @__PURE__ */ new Set();
	const best = /* @__PURE__ */ new Map();
	for (const s of seeds) {
		if (!graph.hasNode(s.id)) continue;
		seedSet.add(s.id);
		const prev = best.get(s.id);
		if (!prev || s.score > prev.score) best.set(s.id, {
			id: s.id,
			score: s.score,
			hop: 0
		});
	}
	if (maxHop > 0) {
		let frontier = [...best.values()];
		let capped = best.size >= maxNodes;
		for (let h = 1; h <= maxHop && frontier.length > 0 && !capped; h++) {
			const nextFrontier = [];
			outer: for (const cur of frontier) {
				const viaLabel = graph.getNodeAttribute(cur.id, "label") ?? cur.id;
				for (const nb of graph.neighbors(cur.id)) {
					if (seedSet.has(nb)) continue;
					const nbScore = cur.score * decay;
					if (nbScore < minScore) continue;
					const existing = best.get(nb);
					if (existing && existing.score >= nbScore) continue;
					const hit = {
						id: nb,
						score: nbScore,
						hop: existing?.hop ?? h,
						via: viaLabel
					};
					best.set(nb, hit);
					if (!existing) nextFrontier.push(hit);
					if (best.size >= maxNodes) {
						capped = true;
						break outer;
					}
				}
			}
			frontier = nextFrontier;
		}
	}
	const out = [];
	for (const hit of best.values()) if (hit.score >= minScore) out.push(hit);
	out.sort((a, b) => b.score - a.score);
	return out;
}
//#endregion
//#region src/engines/wiki/manager.ts
/**
* Wiki Source Manager — 管理文档源的注册、扫描、索引、查询生命周期
*
* 摄取走 ingest-v2/ 引擎。
*
* 索引存储（设计 006）：BM25 全文检索、知识图谱、页元数据不再常驻内存，改存每个
* wiki 私有的 `index.db`（SQLite：wiki_fts + page_meta + graph_edge）。写走独立事务连接
* （重建三表），读走 LRU 连接池；内存与 wiki 总数解耦，根治 MiniSearch 全量常驻的 OOM。
* 图谱小，查询时从 graph_edge 临时构建内存 graphology 实例做多跳 BFS（复用现有算法）。
*/
const log$6 = createLogger("wiki-mgr");
function extractFrontmatter(content) {
	const fmMatch = content.match(/^---\n([\s\S]*?)\n---/);
	const fm = fmMatch ? fmMatch[1] : "";
	const titleMatch = fm.match(/^title:\s*["']?(.+?)["']?\s*$/m);
	const typeMatch = fm.match(/^type:\s*["']?(.+?)["']?\s*$/m);
	const descMatch = fm.match(/^description:\s*["']?(.+?)["']?\s*$/m);
	const sources = [];
	const sourcesBlockMatch = fm.match(/^sources:\s*\n((?:\s+-\s+.+\n?)*)/m);
	if (sourcesBlockMatch) for (const line of sourcesBlockMatch[1].split("\n")) {
		const itemMatch = line.match(/^\s+-\s+["']?(.+?)["']?\s*$/);
		if (itemMatch) sources.push(itemMatch[1]);
	}
	else {
		const inlineMatch = fm.match(/^sources:\s*\[([^\]]*)\]/m);
		if (inlineMatch) for (const item of inlineMatch[1].split(",")) {
			const trimmed = item.trim().replace(/^["']|["']$/g, "");
			if (trimmed) sources.push(trimmed);
		}
	}
	let title = titleMatch ? titleMatch[1].trim() : "";
	if (!title) {
		const headingMatch = content.match(/^#\s+(.+)$/m);
		title = headingMatch ? headingMatch[1].trim() : "";
	}
	return {
		title,
		type: typeMatch ? typeMatch[1].trim().toLowerCase() : "other",
		sources,
		description: descMatch ? descMatch[1].trim() : ""
	};
}
function extractWikilinks(content) {
	const links = [];
	const regex = /\[\[([^\]|]+?)(?:\|[^\]]+?)?\]\]/g;
	let match;
	while ((match = regex.exec(content)) !== null) links.push(match[1].trim());
	return links;
}
/**
* 节流 onProgress：阶段切换立即发；同阶段仅在 percent 上升且距上次 ≥ minIntervalMs
* （或已到 extracting 末段 percent≥90）时发送，避免多源并发打爆 Panel。
*/
function createThrottledProgressFn(onProgress, minIntervalMs = 500) {
	if (!onProgress) return void 0;
	let lastPhase;
	let lastPercent = -1;
	let lastEmitAt = 0;
	return (p) => {
		const now = Date.now();
		if (!(p.phase !== lastPhase)) {
			if (p.percent <= lastPercent) return;
			if (!(p.phase === "extracting" && p.percent >= 90) && now - lastEmitAt < minIntervalMs) return;
		}
		lastPhase = p.phase;
		lastPercent = p.percent;
		lastEmitAt = now;
		onProgress(p);
	};
}
/** 图谱中不参与建边/展示的页类型（如内部 query 页）。 */
const HIDDEN_TYPES = new Set(["query"]);
/**
* 解析页间 wikilink，产出有向边（source → target）用于写入 graph_edge。
* 只在 visible（非 hidden 类型）页之间建边，过滤自环与无法解析的坏链接，(source,target) 去重。
*/
function resolveEdges(pages) {
	const visible = pages.filter((p) => !HIDDEN_TYPES.has(p.type));
	const out = [];
	if (visible.length === 0) return out;
	const nodeIds = new Set(visible.map((p) => p.id));
	const titleSlugToId = /* @__PURE__ */ new Map();
	for (const p of visible) {
		const ts = slugify(p.title);
		if (ts && !titleSlugToId.has(ts)) titleSlugToId.set(ts, p.id);
	}
	const seen = /* @__PURE__ */ new Set();
	for (const page of visible) for (const targetRaw of page.links) {
		const targetId = resolveTarget(targetRaw, nodeIds, titleSlugToId);
		if (!targetId || targetId === page.id) continue;
		const key = `${page.id}\u0000${targetId}`;
		if (seen.has(key)) continue;
		seen.add(key);
		out.push({
			source: page.id,
			target: targetId
		});
	}
	return out;
}
/**
* 从 page_meta + graph_edge 构建内存 PageGraph（读路径）。
* 节点 = 非 hidden 类型的页；边 = graph_edge 有向边，公共 view 无向去重。
*/
function buildPageGraphFromDb(metaById, edgeRows) {
	const graph = new Graph({
		multi: false,
		type: "undirected"
	});
	const outAdj = /* @__PURE__ */ new Map();
	const inAdj = /* @__PURE__ */ new Map();
	const degree = /* @__PURE__ */ new Map();
	const visible = [];
	for (const m of metaById.values()) if (!HIDDEN_TYPES.has(m.type)) visible.push(m);
	for (const m of visible) {
		outAdj.set(m.id, /* @__PURE__ */ new Set());
		inAdj.set(m.id, /* @__PURE__ */ new Set());
		degree.set(m.id, 0);
		graph.addNode(m.id, {
			label: m.title,
			type: m.type,
			path: m.relPath
		});
	}
	const seenEdges = /* @__PURE__ */ new Set();
	const edges = [];
	for (const { source_id: s, target_id: t } of edgeRows) {
		if (!outAdj.has(s) || !inAdj.has(t)) continue;
		outAdj.get(s).add(t);
		inAdj.get(t).add(s);
		const key = [s, t].sort().join(":::");
		if (seenEdges.has(key)) continue;
		seenEdges.add(key);
		edges.push({
			source: s,
			target: t,
			weight: 1
		});
		if (!graph.hasEdge(s, t)) graph.addEdge(s, t, { weight: 1 });
		degree.set(s, (degree.get(s) ?? 0) + 1);
		degree.set(t, (degree.get(t) ?? 0) + 1);
	}
	return {
		view: {
			nodes: visible.map((m) => ({
				id: m.id,
				label: m.title,
				type: m.type,
				path: m.relPath,
				linkCount: degree.get(m.id) ?? 0,
				community: 0
			})),
			edges,
			communities: []
		},
		graph,
		outAdj,
		inAdj,
		degree
	};
}
function resolveTarget(raw, nodeIds, titleSlugToId) {
	if (nodeIds.has(raw)) return raw;
	const target = slugify(raw.replace(/\.md$/i, ""));
	if (!target) return null;
	const rawLower = raw.toLowerCase();
	for (const id of nodeIds) {
		if (id.toLowerCase() === rawLower) return id;
		if (slugify(id.split("/").pop() ?? id) === target) return id;
	}
	const byTitle = titleSlugToId.get(target);
	if (byTitle) return byTitle;
	return null;
}
const STOP_WORDS = new Set([
	"的",
	"是",
	"了",
	"什么",
	"在",
	"有",
	"和",
	"与",
	"对",
	"从",
	"the",
	"is",
	"a",
	"an",
	"what",
	"how",
	"are",
	"was",
	"were",
	"do",
	"does",
	"did",
	"be",
	"been",
	"being",
	"have",
	"has",
	"had",
	"it",
	"its",
	"in",
	"on",
	"at",
	"to",
	"for",
	"of",
	"with",
	"by",
	"this",
	"that",
	"these",
	"those"
]);
const SNIPPET_CONTEXT = 80;
/**
* 预生成页摘要（写入 page_meta.snippet）：优先 frontmatter description，
* 否则取正文（去 frontmatter/标题）前 SNIPPET_CONTEXT 个字符。
* 正文不入库，检索时直接返回该静态摘要（消费者主要是 AI，无需按 query 动态高亮）。
*/
function makeSnippet(page) {
	if (page.description) return page.description;
	return [...page.content.replace(/^---\n[\s\S]*?\n---\n?/, "").replace(/^#+\s+.*$/gm, "").trim()].slice(0, SNIPPET_CONTEXT).join("").replace(/\n/g, " ").trim();
}
/**
* 分词器：中英文混合处理。
* - 英文：按空格/标点切分，保留完整单词，过滤 stop words
* - 中文：bigram + 单字
*
* 导出供 FTS5 预分词复用（006）与 bm25 评测：写入 FTS5 时把 content/title
* 经此函数分词后以空格拼接存入，查询时对 query 用同一分词，保证中文逻辑一致。
*/
function tokenize(text) {
	const rawTokens = text.toLowerCase().split(/[\s,，。！？、；：""''（）()\-_/\\·~～…\[\]【】{}《》<>]+/).filter((t) => t.length > 0);
	const result = [];
	for (const token of rawTokens) {
		const hasCJK = /[\u4e00-\u9fff\u3400-\u4dbf]/.test(token);
		const hasLatin = /[a-z]/.test(token);
		if (hasCJK && hasLatin) {
			const parts = token.split(/(?<=[a-z0-9])(?=[\u4e00-\u9fff])|(?<=[\u4e00-\u9fff])(?=[a-z0-9])/);
			for (const part of parts) if (/[\u4e00-\u9fff]/.test(part) && part.length > 1) {
				const chars = [...part];
				for (let i = 0; i < chars.length - 1; i++) result.push(chars[i] + chars[i + 1]);
				result.push(part);
			} else if (part.length > 0 && !STOP_WORDS.has(part)) result.push(part);
		} else if (hasCJK && token.length > 1) {
			const chars = [...token];
			for (let i = 0; i < chars.length - 1; i++) result.push(chars[i] + chars[i + 1]);
			result.push(token);
		} else if (!STOP_WORDS.has(token) && token.length > 0) result.push(token);
	}
	return result;
}
/**
* FTS5 检索：query → tokenize → 每 token 加 `*` 前缀 → OR 连接 → MATCH。
* bm25() 越负越相关，取负转成"越大越相关"的正分，供图扩展的 decay/minScore 使用。
* title_tok 权重 5.0、content_tok 1.0（对齐原 MiniSearch boost title×5）。
*/
function ftsSearch(db, query, limit) {
	const toks = tokenize(query);
	if (toks.length === 0) return [];
	const expr = toks.map((t) => `"${t.replace(/"/g, "\"\"")}"*`).join(" OR ");
	return db.prepare("SELECT page_id, bm25(wiki_fts, 5.0, 1.0) AS score FROM wiki_fts WHERE wiki_fts MATCH ? ORDER BY score LIMIT ?").all(expr, limit).map((r) => ({
		id: r.page_id,
		score: -r.score
	}));
}
/** 事务内重建三张索引表（wiki_fts + page_meta + graph_edge）。由 withWriteDb 调用。 */
function writeIndex(db, pages) {
	db.prepare("DELETE FROM wiki_fts").run();
	db.prepare("DELETE FROM page_meta").run();
	db.prepare("DELETE FROM graph_edge").run();
	const insFts = db.prepare("INSERT INTO wiki_fts(page_id, title_tok, content_tok) VALUES (?,?,?)");
	const insMeta = db.prepare("INSERT INTO page_meta(page_id, title, type, rel_path, snippet) VALUES (?,?,?,?,?)");
	const insEdge = db.prepare("INSERT OR IGNORE INTO graph_edge(source_id, target_id) VALUES (?,?)");
	for (const p of pages) {
		insFts.run(p.id, tokenize(p.title).join(" "), tokenize(p.content).join(" "));
		insMeta.run(p.id, p.title, p.type, p.relPath, makeSnippet(p));
	}
	for (const e of resolveEdges(pages)) insEdge.run(e.source, e.target);
}
/** 从读连接加载读模型：页元数据表 + 图（graph_edge 构建的内存图）。 */
function loadReadModel(db) {
	const metaRows = db.prepare("SELECT page_id, title, type, rel_path, snippet FROM page_meta ORDER BY page_id").all();
	const metaById = /* @__PURE__ */ new Map();
	for (const r of metaRows) metaById.set(r.page_id, {
		id: r.page_id,
		title: r.title ?? "",
		type: r.type ?? "other",
		relPath: r.rel_path ?? "",
		snippet: r.snippet ?? ""
	});
	return {
		pg: buildPageGraphFromDb(metaById, db.prepare("SELECT source_id, target_id FROM graph_edge").all()),
		metaById
	};
}
const HOP_LIMIT = 5;
const DEFAULT_LIMIT = 20;
const DEFAULT_HOP = 0;
const DEFAULT_DECAY = .5;
const DEFAULT_MIN_SCORE = .1;
const RELATED_CAP = 10;
const EXPANSION_CAP = 200;
/**
* Build the `related` field for one result page (PRD FR-1).
*
* Out-link (this → other), in-link (other → this), or both. Same neighbour
* keeps a single entry. Sort by neighbour degree descending, cap at RELATED_CAP.
*/
function buildRelated(pageId, pg, metaById) {
	const out = pg.outAdj.get(pageId) ?? /* @__PURE__ */ new Set();
	const inn = pg.inAdj.get(pageId) ?? /* @__PURE__ */ new Set();
	const all = new Set([...out, ...inn]);
	const items = [];
	for (const nbId of all) {
		const nbMeta = metaById.get(nbId);
		if (!nbMeta) continue;
		const isOut = out.has(nbId);
		const isIn = inn.has(nbId);
		const direction = isOut && isIn ? "both" : isOut ? "out" : "in";
		items.push({
			title: nbMeta.title,
			path: nbMeta.relPath,
			type: nbMeta.type,
			direction
		});
	}
	items.sort((a, b) => {
		const da = pg.degree.get(idFromPath(a.path)) ?? 0;
		return (pg.degree.get(idFromPath(b.path)) ?? 0) - da;
	});
	return items.slice(0, RELATED_CAP);
}
function idFromPath(relPath) {
	return relPath.replace(/^wiki\//, "").replace(/\.md$/, "");
}
function clamp(n, lo, hi) {
	if (Number.isNaN(n)) return lo;
	return Math.min(Math.max(n, lo), hi);
}
/**
* Build inter-result wikilink edges (PRD FR-2).
*
* Only edges where both endpoints are in `resultIds`. Undirected dedup
* via sorted-pair key. Self-loops were already excluded at graph-build time.
*/
function buildResultLinks(resultIds, pg, metaById) {
	const inResults = new Set(resultIds);
	const seen = /* @__PURE__ */ new Set();
	const links = [];
	for (const id of resultIds) {
		const meta = metaById.get(id);
		if (!meta) continue;
		const out = pg.outAdj.get(id) ?? /* @__PURE__ */ new Set();
		for (const target of out) {
			if (!inResults.has(target)) continue;
			const key = [id, target].sort().join(":::");
			if (seen.has(key)) continue;
			seen.add(key);
			const targetMeta = metaById.get(target);
			links.push({
				source: meta.relPath,
				target: targetMeta ? targetMeta.relPath : target,
				weight: 1
			});
		}
	}
	return links;
}
function initWikiProject(projectPath) {
	for (const dir of [
		"raw/sources",
		"wiki/entities",
		"wiki/concepts",
		"wiki/sources",
		"wiki/comparisons",
		"wiki/synthesis",
		".llm-wiki"
	]) mkdirSync$1(join$1(projectPath, dir), { recursive: true });
	const defaultFiles = [
		["wiki/schema.md", `---\ntype: schema\ntitle: Wiki Schema\n---\n\n${DEFAULT_SCHEMA}\n`],
		["wiki/purpose.md", `---\ntype: purpose\ntitle: Wiki Purpose\n---\n\n${DEFAULT_PURPOSE}\n`],
		["wiki/index.md", "---\ntype: index\ntitle: Index\n---\n\n# Index\n\n## Entities\n\n## Concepts\n\n## Sources\n"]
	];
	for (const [rel, content] of defaultFiles) {
		const full = join$1(projectPath, rel);
		if (!existsSync$1(full)) writeFileSync$1(full, content, "utf-8");
	}
}
/**
* 增量抽取（设计 003 §3.6 + wiki-ingest-optimization）：
* 阶段1 并行 LLM 抽取 → 已删源级联清理 → 阶段2 串行 merge 落盘 → overview。
* 不在此更新 source 表 / 不重建索引——那些交由 ingest() 在同一事务内完成（强一致）。
* 全部失败检测不在此 throw，由上层 WikiSourceManager.ingest 写事务后判定。
*
* 导出供编排层单测（进度相位 / skipped / 全失败不 throw）。
*/
async function runIngestIncremental(projectPath, oldStates, llmConfig, onProgress, globalLlmLimit) {
	const { extractSource, commitCandidates, scanExistingPages } = await import("./ingest-v2-B4yjztx2.mjs");
	const report = createThrottledProgressFn(onProgress);
	const sourcesDir = join$1(projectPath, "raw", "sources");
	if (!existsSync$1(sourcesDir)) {
		log$6.warn("runIngest: raw/sources 不存在，跳过", { projectPath });
		return {
			results: [],
			processed: [],
			deletedSources: [...oldStates.keys()]
		};
	}
	const disk = findMdFiles(sourcesDir).map((abs) => {
		const content = readFileSync$1(abs, "utf-8");
		return {
			abs,
			filename: relative$1(sourcesDir, abs).replace(/\\/g, "/"),
			sha256: sha256(content),
			size: Buffer.byteLength(content, "utf-8")
		};
	});
	const { toIngest, skipped, deleted } = classifySources(disk, oldStates);
	const skippedCount = skipped.length;
	const toIngestSet = new Set(toIngest);
	const toIngestDisk = disk.filter((d) => toIngestSet.has(d.filename));
	log$6.info("runIngest 增量分类", {
		projectPath,
		disk: disk.length,
		toIngest: toIngest.length,
		skipped: skipped.length,
		deleted: deleted.length
	});
	const existingPages = scanExistingPages(projectPath);
	const wikiLimit = pLimit(getIngestConcurrency());
	report?.({
		phase: "extracting",
		total: toIngestDisk.length,
		completed: 0,
		failed: 0,
		skipped: skippedCount,
		percent: 0
	});
	let completed = 0;
	let failed = 0;
	const tasks = toIngestDisk.map((d) => wikiLimit(async () => {
		const t0 = Date.now();
		try {
			const candidates = await withSpan("ingest-source", async (span) => {
				span.setAttribute("source.name", d.filename);
				const run = () => extractSource(projectPath, d.abs, llmConfig, existingPages);
				return globalLlmLimit ? globalLlmLimit(run) : run();
			});
			completed++;
			report?.({
				phase: "extracting",
				total: toIngestDisk.length,
				completed,
				failed,
				skipped: skippedCount,
				percent: Math.round((completed + failed) / Math.max(toIngestDisk.length, 1) * 90)
			});
			log$6.info("runIngest 单源抽取完成", {
				source: d.filename,
				candidates: candidates.size,
				ms: Date.now() - t0
			});
			return {
				...d,
				ok: true,
				candidates,
				error: null
			};
		} catch (err) {
			failed++;
			report?.({
				phase: "extracting",
				total: toIngestDisk.length,
				completed,
				failed,
				skipped: skippedCount,
				percent: Math.round((completed + failed) / Math.max(toIngestDisk.length, 1) * 90)
			});
			log$6.error("runIngest 单源抽取失败", {
				source: d.filename,
				ms: Date.now() - t0,
				error: String(err)
			});
			return {
				...d,
				ok: false,
				candidates: /* @__PURE__ */ new Map(),
				error: String(err)
			};
		}
	}));
	const extractResults = await Promise.all(tasks);
	if (deleted.length > 0) try {
		const { deleteSourceFiles } = await import("./cascade-Cq0KJ3OW.mjs");
		await deleteSourceFiles(projectPath, deleted.map((fn) => join$1(sourcesDir, fn)), { logReason: "wiki/ingest/removed-source" });
	} catch (err) {
		log$6.warn("已删源级联清理失败", { error: String(err) });
	}
	report?.({
		phase: "merging",
		total: toIngestDisk.length,
		completed,
		failed,
		skipped: skippedCount,
		percent: 90
	});
	const successResults = extractResults.filter((r) => r.ok);
	const allCandidates = successResults.map((r) => ({
		sourceFilename: r.filename,
		candidates: r.candidates
	}));
	let llm;
	if (allCandidates.length > 0) try {
		const { createLlmClient } = await import("./llm-CFNAYq4F.mjs");
		llm = createLlmClient(llmConfig);
	} catch (err) {
		log$6.error("创建 LLM client 失败（阶段2 merge/overview 将降级，source 状态仍会落库）", { error: String(err) });
	}
	const { written, mergeErrors } = await commitCandidates(projectPath, allCandidates, llm, {
		globalLlmLimit,
		skipLog: allCandidates.length === 0
	});
	if (mergeErrors.length > 0) log$6.warn("阶段2 合并部分页失败", {
		count: mergeErrors.length,
		errors: mergeErrors
	});
	const processed = extractResults.map((r) => {
		if (!r.ok) return {
			filename: r.filename,
			sha256: r.sha256,
			size: r.size,
			ok: false,
			error: r.error
		};
		const sourcePages = [...r.candidates.keys()];
		const allMergeFailed = sourcePages.length > 0 && sourcePages.every((p) => mergeErrors.some((e) => e.source === r.filename && e.relPath === p)) && !sourcePages.some((p) => written.includes(p));
		return {
			filename: r.filename,
			sha256: r.sha256,
			size: r.size,
			ok: !allMergeFailed,
			error: allMergeFailed ? "all candidates merge failed" : null
		};
	});
	report?.({
		phase: "indexing",
		total: toIngestDisk.length,
		completed,
		failed,
		skipped: skippedCount,
		percent: 98
	});
	if (successResults.length > 0) if (!llm) log$6.warn("overview 跳过：LLM client 不可用（不影响摄取）");
	else try {
		const { generateOverview } = await import("./overview--8cKByeL.mjs");
		const runOverview = () => generateOverview(projectPath, llm);
		await (globalLlmLimit ? globalLlmLimit(runOverview) : runOverview());
	} catch (err) {
		log$6.warn("overview 生成失败（不影响摄取）", { error: String(err) });
	}
	const results = extractResults.map((r) => {
		if (!r.ok) return {
			source: r.filename,
			filesWritten: [],
			error: r.error
		};
		const filesWritten = [...r.candidates.keys()].filter((p) => written.includes(p));
		return {
			source: r.filename,
			filesWritten,
			error: null
		};
	});
	const okCount = processed.filter((p) => p.ok).length;
	log$6.info("runIngest 全部完成", {
		total: results.length,
		ok: okCount,
		failed: results.length - okCount,
		written: written.length
	});
	return {
		results,
		processed,
		deletedSources: deleted
	};
}
function findMdFiles(dir) {
	const files = [];
	for (const entry of readdirSync$1(dir)) {
		const full = join$1(dir, entry);
		if (statSync$1(full).isDirectory()) files.push(...findMdFiles(full));
		else if (entry.endsWith(".md") || entry.endsWith(".txt")) files.push(full);
	}
	return files;
}
function createWikiSourceManager(dataDir) {
	const sources = /* @__PURE__ */ new Map();
	const stateFile = join$1(dataDir, "wiki-sources.json");
	mkdirSync$1(dataDir, { recursive: true });
	function persist() {
		writeFileSync$1(stateFile, JSON.stringify(Object.fromEntries(sources.entries()), null, 2), "utf-8");
	}
	function loadState() {
		if (!existsSync$1(stateFile)) return;
		try {
			const raw = JSON.parse(readFileSync$1(stateFile, "utf-8"));
			for (const [name, state] of Object.entries(raw)) {
				if (state.status === "scanning") {
					state.status = "error";
					state.error = "Restart";
				}
				sources.set(name, state);
			}
		} catch {}
	}
	function scanWikiDir(projectPath) {
		const wikiDir = join$1(projectPath, "wiki");
		if (!existsSync$1(wikiDir)) throw new Error(`wiki/ not found: ${wikiDir}`);
		const pages = [];
		scanRecursive(wikiDir, wikiDir, pages);
		return pages;
	}
	function scanRecursive(baseDir, dir, pages) {
		for (const entry of readdirSync$1(dir)) {
			const full = join$1(dir, entry);
			if (statSync$1(full).isDirectory()) {
				if (entry !== "media") scanRecursive(baseDir, full, pages);
			} else if (entry.endsWith(".md")) try {
				const content = readFileSync$1(full, "utf-8");
				const rel = full.slice(baseDir.length + 1);
				const id = rel.replace(/\.md$/, "").replace(/\\/g, "/");
				const fm = extractFrontmatter(content);
				pages.push({
					id,
					title: fm.title || basename$1(entry, ".md").replace(/-/g, " "),
					type: fm.type,
					path: full,
					relPath: `wiki/${rel}`,
					content,
					sources: fm.sources,
					links: extractWikilinks(content),
					description: fm.description
				});
			} catch {}
		}
	}
	/** 重建 wiki 的 index.db 索引（幂等建库 → 事务重建三表 → 驱逐读连接防 stale）。 */
	function rebuildIndex(name, pages) {
		const state = sources.get(name);
		if (!state) throw new Error(`rebuildIndex: unknown wiki ${name}`);
		initIndexDb(state.path);
		withWriteDb(state.path, (db) => writeIndex(db, pages));
		evictWikiDb(name);
	}
	function searchInternal(name, query, limit, options) {
		const state = sources.get(name);
		if (!state) return {
			results: [],
			links: [],
			count: 0
		};
		let db;
		try {
			db = getReadDb(name, state.path);
		} catch {
			return {
				results: [],
				links: [],
				count: 0
			};
		}
		const hop = clamp(options.hop ?? DEFAULT_HOP, 0, HOP_LIMIT);
		const decay = clamp(options.decay ?? DEFAULT_DECAY, 0, 1);
		const minScore = options.minScore ?? DEFAULT_MIN_SCORE;
		const finalLimit = limit > 0 ? limit : DEFAULT_LIMIT;
		const seedPoolSize = Math.max(finalLimit, hop > 0 ? finalLimit * 2 : finalLimit);
		const rawSeeds = ftsSearch(db, query, seedPoolSize);
		if (rawSeeds.length === 0) return {
			results: [],
			links: [],
			count: 0
		};
		const { pg, metaById } = loadReadModel(db);
		let hits;
		if (hop === 0) hits = rawSeeds.slice(0, finalLimit).map((s) => ({
			id: s.id,
			score: s.score,
			hop: 0
		}));
		else {
			hits = graphMultiHopSearch(pg.graph, rawSeeds, {
				hop,
				decay,
				minScore,
				maxNodes: EXPANSION_CAP
			});
			hits = hits.slice(0, finalLimit);
		}
		const results = [];
		const resultIds = [];
		for (const hit of hits) {
			const meta = metaById.get(hit.id);
			if (!meta) continue;
			const result = {
				path: meta.relPath,
				title: meta.title,
				snippet: meta.snippet,
				score: hit.score,
				type: meta.type,
				hop: hit.hop,
				related: buildRelated(meta.id, pg, metaById)
			};
			if (hit.hop > 0 && hit.via) result.via = hit.via;
			results.push(result);
			resultIds.push(meta.id);
		}
		return {
			results,
			links: buildResultLinks(resultIds, pg, metaById),
			count: results.length
		};
	}
	loadState();
	log$6.info("Restoring wiki indexes", { count: sources.size });
	let restored = 0;
	let failed = 0;
	for (const [name, state] of sources.entries()) {
		if (state.status !== "ready") {
			log$6.debug("Skip non-ready wiki source", {
				name,
				status: state.status
			});
			continue;
		}
		const wikiDir = join$1(state.path, "wiki");
		if (!existsSync$1(wikiDir)) {
			log$6.warn("Wiki dir missing on disk; mark error and skip restore", {
				name,
				path: state.path
			});
			state.status = "error";
			state.error = `wiki dir not found: ${wikiDir}`;
			failed++;
			continue;
		}
		try {
			const pages = scanWikiDir(state.path);
			rebuildIndex(name, pages);
			restored++;
			log$6.info("Restored wiki index", {
				name,
				pageCount: pages.length
			});
		} catch (err) {
			failed++;
			log$6.error("Failed to restore wiki index", {
				name,
				error: err instanceof Error ? err.message : String(err)
			});
			state.status = "error";
			state.error = err instanceof Error ? err.message : String(err);
		}
	}
	log$6.info("Wiki restore complete", {
		restored,
		failed,
		total: sources.size
	});
	function register(config) {
		const existing = sources.get(config.name);
		if (existing) return existing;
		const state = {
			name: config.name,
			path: config.path,
			status: "scanning"
		};
		sources.set(config.name, state);
		try {
			const pages = scanWikiDir(config.path);
			rebuildIndex(config.name, pages);
			state.status = "ready";
			state.pageCount = pages.length;
			state.lastSyncAt = (/* @__PURE__ */ new Date()).toISOString();
		} catch (err) {
			state.status = "error";
			state.error = String(err);
		}
		persist();
		return state;
	}
	function sync(name) {
		const state = sources.get(name);
		if (!state) throw new Error(`Not found: ${name}`);
		state.status = "scanning";
		const t0 = Date.now();
		try {
			const pages = scanWikiDir(state.path);
			rebuildIndex(name, pages);
			state.status = "ready";
			state.pageCount = pages.length;
			state.lastSyncAt = (/* @__PURE__ */ new Date()).toISOString();
			state.error = void 0;
			log$6.info("sync 完成（索引已重建）", {
				name,
				pageCount: pages.length,
				ms: Date.now() - t0
			});
		} catch (err) {
			state.status = "error";
			state.error = String(err);
			log$6.error("sync 失败", {
				name,
				path: state.path,
				error: String(err)
			});
		}
		persist();
		return state;
	}
	function init(config) {
		initWikiProject(config.path);
		return register(config);
	}
	async function ingest(name, llmConfig, opts) {
		const state = sources.get(name);
		if (!state) throw new Error(`Not found: ${name}`);
		const projectPath = state.path;
		initIndexDb(projectPath);
		let oldStates = /* @__PURE__ */ new Map();
		try {
			oldStates = readSourceStates(getReadDb(name, projectPath));
		} catch {}
		const outcome = await withSpan("wiki-ingest", async (span) => {
			span.setAttribute("wiki.name", name);
			return runIngestIncremental(projectPath, oldStates, llmConfig, opts?.onProgress, opts?.globalLlmLimit);
		});
		state.status = "scanning";
		const t0 = Date.now();
		try {
			const pages = scanWikiDir(projectPath);
			withWriteDb(projectPath, (db) => {
				writeIndex(db, pages);
				for (const p of outcome.processed) recordSourceIngestResult(db, p);
				if (outcome.deletedSources.length > 0) deleteSources(db, outcome.deletedSources);
			});
			evictWikiDb(name);
			const attempted = outcome.processed.length;
			const failed = outcome.processed.filter((p) => !p.ok);
			if (attempted > 0 && failed.length === attempted) {
				const first = failed[0];
				throw new Error(`all source documents failed to ingest${first ? `; first failure: ${first.filename}: ${first.error ?? "unknown"}` : ""}`);
			}
			state.status = "ready";
			state.pageCount = pages.length;
			state.lastSyncAt = (/* @__PURE__ */ new Date()).toISOString();
			state.error = void 0;
			log$6.info("ingest 完成（增量抽取 + 索引/源状态同事务重建）", {
				name,
				pageCount: pages.length,
				extracted: outcome.processed.length,
				failed: failed.length,
				ms: Date.now() - t0
			});
		} catch (err) {
			state.status = "error";
			state.error = String(err);
			log$6.error("ingest 失败", {
				name,
				path: projectPath,
				error: String(err)
			});
			persist();
			throw err;
		}
		persist();
		return outcome.results;
	}
	return {
		register,
		sync,
		init,
		ingest,
		get: (name) => sources.get(name),
		list: () => [...sources.values()],
		remove: (name) => {
			const state = sources.get(name);
			sources.delete(name);
			evictWikiDb(name);
			if (state) {}
			persist();
		},
		search: (name, query, limit, options) => searchInternal(name, query, limit ?? DEFAULT_LIMIT, options ?? {}),
		graph: (name) => {
			const state = sources.get(name);
			if (!state) return {
				nodes: [],
				edges: [],
				communities: []
			};
			try {
				return loadReadModel(getReadDb(name, state.path)).pg.view;
			} catch {
				return {
					nodes: [],
					edges: [],
					communities: []
				};
			}
		},
		readPage: (name, relPath) => {
			const state = sources.get(name);
			if (!state) return null;
			if (relPath.startsWith("raw/")) {
				const fullPath = join$1(state.path, relPath);
				if (!fullPath.startsWith(join$1(state.path, "raw"))) return null;
				try {
					return readFileSync$1(fullPath, "utf-8");
				} catch {}
				if (!relPath.endsWith(".md")) try {
					return readFileSync$1(fullPath + ".md", "utf-8");
				} catch {}
				return null;
			}
			const cleanPath = relPath.replace(/^wiki\//, "");
			const base = join$1(state.path, "wiki");
			let fullPath = join$1(base, cleanPath);
			if (!fullPath.startsWith(base)) return null;
			try {
				return readFileSync$1(fullPath, "utf-8");
			} catch {}
			if (!cleanPath.endsWith(".md")) try {
				return readFileSync$1(fullPath + ".md", "utf-8");
			} catch {}
			return null;
		},
		getPages: (name) => {
			const state = sources.get(name);
			if (!state) return [];
			try {
				return scanWikiDir(state.path);
			} catch {
				return [];
			}
		}
	};
}
//#endregion
//#region src/engines/code/bridge.ts
/**
* CodeGraph Bridge — 封装 @colbymchenry/codegraph 的核心 API。
*
* 将 CodeGraph 实例 + ToolHandler 包装为简洁的调用接口，
* 上层 API 只需调 bridge 方法即可，不直接依赖 codegraph 内部结构。
*/
const log$5 = createLogger("bridge");
let _codegraphModule = null;
let _toolsModule = null;
/**
* 解析 ToolHandler 所在的 mcp/tools 模块路径。
*
* npm 主包的入口是 npm-sdk.js，它在运行时 require 平台包
* （如 @colbymchenry/codegraph-linux-x64）的 lib/dist/index.js。
* mcp/tools.js 在平台包的 lib/dist/mcp/ 下。必须从 npm 主包的路径解析：
* pnpm 的 strict 隔离不会把主包的 optional dependency 暴露给 KS 根目录。
*/
function resolveToolsPath() {
	const appRequire = createRequire(import.meta.url);
	const platform = `${process.platform}-${process.arch}`;
	const toolsSpecifier = `@colbymchenry/codegraph-${platform}/lib/dist/mcp/tools.js`;
	try {
		return createRequire(appRequire.resolve("@colbymchenry/codegraph")).resolve(toolsSpecifier);
	} catch {
		throw new Error(`codegraph: platform package @colbymchenry/codegraph-${platform} not installed or missing mcp/tools.js. Run: pnpm add @colbymchenry/codegraph`);
	}
}
async function loadModules() {
	if (_codegraphModule) return extractExports();
	const toolsPath = resolveToolsPath();
	log$5.info("Loading codegraph from npm package");
	_codegraphModule = await import("@colbymchenry/codegraph");
	_toolsModule = await import(pathToFileURL(toolsPath).href);
	log$5.info("Loaded codegraph from npm package", {
		indexKeys: Object.keys(_codegraphModule),
		toolsKeys: Object.keys(_toolsModule)
	});
	return extractExports();
}
/**
* CJS 包在 ESM 下 `import()` 会被包成 `{ default: module.exports }`；
* CJS 模式（tsx）下 `import()` 被转成 `require()`，返回的是展开对象。
* 这里统一展开，两种模式都能正确取到导出。
*/
function unwrapCjs(mod) {
	const m = mod;
	return m && typeof m === "object" && m.default && typeof m.default === "object" ? m.default : mod;
}
function extractExports() {
	const cg = unwrapCjs(_codegraphModule);
	const tools = unwrapCjs(_toolsModule);
	return {
		CodeGraph: cg.CodeGraph,
		ToolHandler: tools.ToolHandler,
		isInitialized: cg.isInitialized,
		getCodeGraphDir: cg.getCodeGraphDir
	};
}
/**
* 打开一个已存在的 codegraph 索引。
*/
async function openIndex(projectPath) {
	log$5.info("openIndex", { projectPath });
	const { CodeGraph, ToolHandler } = await loadModules();
	const cg = await CodeGraph.open(projectPath);
	const stats = cg.getStats();
	log$5.info("openIndex complete", {
		projectPath,
		stats
	});
	const handler = new ToolHandler(cg);
	if (typeof handler.setDefaultProjectHint === "function") handler.setDefaultProjectHint(projectPath);
	return {
		cg,
		handler,
		projectRoot: projectPath
	};
}
/**
* 对一个项目目录进行全量索引。
*/
async function indexProject(projectPath) {
	log$5.info("indexProject start", { projectPath });
	const { CodeGraph, ToolHandler, isInitialized } = await loadModules();
	const initialized = isInitialized(projectPath);
	log$5.debug("isInitialized check", {
		projectPath,
		initialized
	});
	let cg;
	if (initialized) {
		log$5.info("Re-indexing existing project");
		cg = await CodeGraph.open(projectPath);
		await cg.indexAll();
	} else {
		log$5.info("First-time init + index");
		cg = await CodeGraph.init(projectPath, { index: true });
	}
	const stats = cg.getStats();
	log$5.info("indexProject complete", {
		projectPath,
		stats
	});
	const handler = new ToolHandler(cg);
	if (typeof handler.setDefaultProjectHint === "function") handler.setDefaultProjectHint(projectPath);
	log$5.debug("ToolHandler created", { availableTools: Object.keys(handler.tools || handler._tools || {}) });
	return {
		cg,
		handler,
		projectRoot: projectPath
	};
}
/**
* 增量同步（只处理变化的文件）。
*/
async function syncIndex(instance) {
	log$5.info("syncIndex start", { projectRoot: instance.projectRoot });
	const changed = (await instance.cg.sync())?.filesChanged ?? 0;
	log$5.info("syncIndex complete", { changed });
	return { changed };
}
/**
* 执行 codegraph MCP 工具（复用 ToolHandler 的格式化输出）。
*/
async function executeTool(instance, toolName, params) {
	log$5.info("executeTool", {
		toolName,
		params,
		projectRoot: instance.projectRoot
	});
	log$5.debug("handler state", {
		handlerType: typeof instance.handler,
		handlerKeys: Object.keys(instance.handler),
		hasExecute: typeof instance.handler.execute === "function"
	});
	const result = await instance.handler.execute(toolName, params);
	log$5.debug("executeTool raw result", {
		toolName,
		resultKeys: Object.keys(result || {}),
		contentLength: result?.content?.length,
		content0: result?.content?.[0],
		isError: result?.isError
	});
	const text = result.content?.[0]?.text ?? "";
	const isError = result.isError ?? false;
	log$5.info("executeTool response", {
		toolName,
		isError,
		textLength: text.length,
		textPreview: text.slice(0, 200)
	});
	return {
		text,
		isError
	};
}
/**
* 获取索引统计信息。
*/
function getStats(instance) {
	const stats = instance.cg.getStats();
	log$5.debug("getStats", { stats });
	return stats;
}
/**
* 关闭索引（释放 SQLite 连接）。
*/
function closeIndex(instance) {
	log$5.info("closeIndex", { projectRoot: instance.projectRoot });
	try {
		instance.cg.close?.();
	} catch {}
}
//#endregion
//#region src/source-fetcher/git-fetcher.ts
/**
* GitSourceFetcher — 基于 simple-git 的源码拉取实现。
*
* simple-git 内部用 child_process.spawn + args 数组，不走 shell，从原理上消除 shell 注入。
*
* 安全防护（002 §4-5）：
*   - R1 git hooks：clone/fetch 本就不拉取远端 .git/hooks（hooks 为本地态），故不额外
*     配置 core.hooksPath（加固版 git 会拒绝该配置，需 allowUnsafeHooksPath）。
*   - R2 SSRF：只允许 public HTTPS + 内网/环回地址黑名单（对齐项目 security_rules）。
*   - Bug 修复（方案 A）：增量 sync 的 git clean 排除 .codegraph/，避免删掉 codegraph 索引库。
*/
/**
* 内网 / 环回 / link-local 地址黑名单（标准网段）：
*   - 10. / 172.16-31. / 192.168.  → RFC1918 私有网段
*   - 169.254.                     → link-local（含云元数据 169.254.169.254）
*   - 127. / 0. / localhost / ::1  → 环回
*   - fe80:                        → IPv6 link-local
*
* 该黑名单可通过环境变量 KNOWLEDGE_SSRF_CHECK=off 关闭（见 GitSourceFetcher 构造）。
*/
const PRIVATE_ADDR_RE = /^(10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.|169\.254\.|127\.|0\.|localhost$|::1$|fe80:)/i;
/**
* 读取 SSRF 私网黑名单开关。默认开启；
* 当 KNOWLEDGE_SSRF_CHECK 为 off/false/0/no（大小写不敏感）时关闭。
*/
function ssrfCheckEnabledFromEnv() {
	const raw = process.env.KNOWLEDGE_SSRF_CHECK;
	if (raw == null || raw.trim() === "") return true;
	const v = raw.trim().toLowerCase();
	return !(v === "off" || v === "false" || v === "0" || v === "no");
}
var GitSourceFetcher = class {
	supportedType = "git";
	/** SSRF 私网黑名单校验开关（https-only 协议校验始终生效，不受此开关影响）。 */
	ssrfCheck;
	constructor(opts) {
		this.ssrfCheck = opts?.ssrfCheck ?? ssrfCheckEnabledFromEnv();
	}
	/**
	* 本地开发模式（fork 补丁）：KNOWLEDGE_ALLOW_LOCAL_REPO=on 时放开
	*   ① https:// 强制（允许 http:// 本地 Gitea）
	*   ② SSRF 私网黑名单（允许 127./localhost/192.168. 本机/内网仓）
	* 便于在本机 / 内网 git 服务（如 Gitea）上构建工程知识库，无需 public HTTPS。
	* 默认关闭（行为与上游一致）；仅开发环境显式打开。
	*/
	allowLocalRepo() {
		const raw = process.env.KNOWLEDGE_ALLOW_LOCAL_REPO;
		if (raw == null || raw.trim() === "") return false;
		const v = raw.trim().toLowerCase();
		return v === "on" || v === "true" || v === "1" || v === "yes";
	}
	validate(sourceUrl) {
		if (this.allowLocalRepo()) {
			if (!this.extractHost(sourceUrl)) throw new Error(`invalid repo_url: cannot parse host from ${sourceUrl}`);
			return;
		}
		if (!sourceUrl.startsWith("https://")) throw new Error("first version only supports public HTTPS repos; SSH/private repo support coming soon");
		const host = this.extractHost(sourceUrl);
		if (!host) throw new Error(`invalid repo_url: cannot parse host from ${sourceUrl}`);
		if (this.ssrfCheck && this.isPrivateAddress(host)) throw new Error(`repo_url must not point to private/loopback address: ${host}`);
	}
	async fetch(sourceUrl, branch, localPath) {
		this.validate(sourceUrl);
		await simpleGit().clone(sourceUrl, localPath, {
			"--depth": 1,
			"--branch": branch
		});
		return {
			localPath,
			version: await this.headCommit(localPath),
			sourceType: "git"
		};
	}
	async sync(sourceUrl, branch, localPath) {
		this.validate(sourceUrl);
		const git = simpleGit(localPath);
		await git.fetch("origin", branch, { "--depth": 1 });
		await git.reset(ResetMode.HARD, [`origin/${branch}`]);
		await git.clean(CleanOptions.FORCE + CleanOptions.RECURSIVE, ["-e", ".codegraph"]);
		return {
			localPath,
			version: await this.headCommit(localPath),
			sourceType: "git"
		};
	}
	async headCommit(localPath) {
		try {
			return (await simpleGit(localPath).revparse(["HEAD"])).trim().slice(0, 12);
		} catch {
			return null;
		}
	}
	extractHost(url) {
		try {
			return new URL(url).hostname;
		} catch {
			return "";
		}
	}
	isPrivateAddress(host) {
		return PRIVATE_ADDR_RE.test(host);
	}
};
//#endregion
//#region src/source-fetcher/registry.ts
var SourceFetcherRegistry = class {
	fetchers = /* @__PURE__ */ new Map();
	constructor() {
		this.register(new GitSourceFetcher());
	}
	register(fetcher) {
		this.fetchers.set(fetcher.supportedType, fetcher);
	}
	/** 根据 sourceUrl 自动探测协议类型，返回对应 fetcher；未注册则 throw。 */
	resolve(sourceUrl) {
		const type = this.detectType(sourceUrl);
		const fetcher = this.fetchers.get(type);
		if (!fetcher) throw new Error(`unsupported source type: ${type} (${sourceUrl})`);
		return fetcher;
	}
	detectType(url) {
		if (url.startsWith("git@") || url.startsWith("ssh://") || url.startsWith("https://") || url.startsWith("http://")) return "git";
		if (url.startsWith("file://") || url.startsWith("/") || url.startsWith("./")) return "local";
		if (url.startsWith("ftp://")) return "ftp";
		return "git";
	}
};
//#endregion
//#region src/module.ts
/**
* Knowledge Module Factory — assembles store / services / engines / workers / restart recovery.
*
* Outputs `KnowledgeModule` with all dependencies wired up for the Hono server.
* Real code-graph worker: git clone/fetch + codegraph indexing.
* Real wiki worker: LLM ingest via wiki engine.
*/
const log$4 = createLogger("knowledge-module");
/** 进程级全局 LLM 并发信号量（跨所有 wiki 的 extract + merge）。 */
const globalLlmLimit = pLimit(getGlobalLlmConcurrency());
/**
* Create Knowledge Module (assembly entry point).
* - Initialize Store / Service / engines
* - Mark interrupted tasks as failed
* - Async restore synced instances
*/
function createKnowledgeModule(config) {
	const { dataDir, db, llmConfig } = config;
	const store = new SqliteKnowledgeStore(db);
	const llmBindingStore = createLlmBindingStore(db);
	const resolveLlm = (serviceId) => resolveLlmConfig(serviceId, llmBindingStore.get(serviceId), llmConfig);
	const _poolMap = /* @__PURE__ */ new Map();
	const instancePool = {
		get(id) {
			return _poolMap.get(id);
		},
		set(id, inst) {
			_poolMap.set(id, inst);
		},
		delete(id) {
			_poolMap.delete(id);
		},
		async loadIfMissing(id, dir) {
			if (_poolMap.has(id)) return _poolMap.get(id);
			try {
				const instance = await openIndex(dir);
				_poolMap.set(id, instance);
				log$4.info(`[code-graph] lazy-loaded instance ${id}`);
				return instance;
			} catch (err) {
				log$4.warn(`[code-graph] lazy-load failed ${id}: ${err instanceof Error ? err.message : String(err)}`);
				return;
			}
		}
	};
	const wikiMgr = createWikiSourceManager(join(dataDir, "_wiki_engines"));
	const fetcherRegistry = new SourceFetcherRegistry();
	const realCodeWorker = async (ctx) => {
		const { dir, repoUrl, branch, codeGraphId, setInternalStatus } = ctx;
		const fetcher = fetcherRegistry.resolve(repoUrl);
		const isExistingRepo = existsSync(join(dir, ".git"));
		let didIncrementalSync = false;
		let version = null;
		if (isExistingRepo) try {
			setInternalStatus("fetching");
			version = (await fetcher.sync(repoUrl, branch, dir)).version;
			setInternalStatus("indexing");
			let instance = instancePool.get(codeGraphId);
			if (!instance) instance = await openIndex(dir);
			await syncIndex(instance);
			instancePool.set(codeGraphId, instance);
			didIncrementalSync = true;
		} catch (err) {
			log$4.warn(`[code-graph] incremental sync failed for ${codeGraphId}, falling back to fresh clone: ${err instanceof Error ? err.message : String(err)}`);
			try {
				rmSync(dir, {
					recursive: true,
					force: true
				});
			} catch {}
		}
		if (!didIncrementalSync) {
			mkdirSync(dir, { recursive: true });
			setInternalStatus("cloning");
			version = (await fetcher.fetch(repoUrl, branch, dir)).version;
			setInternalStatus("indexing");
			const instance = await indexProject(dir);
			instancePool.set(codeGraphId, instance);
		}
		const commitHash = version ?? void 0;
		const instance = instancePool.get(codeGraphId);
		const rawStats = instance ? getStats(instance) : void 0;
		return {
			commitHash,
			stats: rawStats ? {
				files: rawStats.fileCount ?? rawStats.files ?? 0,
				nodes: rawStats.nodeCount ?? rawStats.nodes ?? 0,
				edges: rawStats.edgeCount ?? rawStats.edges ?? 0
			} : void 0
		};
	};
	const realWikiWorker = async (ctx) => {
		const { wikiId, serviceId, teamId, dir, setInternalStatus, ingestRunId } = ctx;
		setInternalStatus("ingesting");
		const effectiveLlm = resolveLlm(serviceId);
		const onProgress = config.tmcCallbackUrl ? buildProgressFn(config.tmcCallbackUrl, wikiId, serviceId, teamId, ingestRunId) : void 0;
		wikiMgr.init({
			name: wikiId,
			path: dir
		});
		await wikiMgr.ingest(wikiId, {
			protocol: effectiveLlm.protocol,
			provider: effectiveLlm.provider,
			apiKey: effectiveLlm.apiKey,
			model: effectiveLlm.model,
			customEndpoint: effectiveLlm.baseUrl,
			maxContextSize: effectiveLlm.maxTokens,
			timeoutMs: effectiveLlm.timeoutMs,
			stream: effectiveLlm.stream ?? false
		}, {
			onProgress,
			globalLlmLimit
		});
		setInternalStatus("rebuilding-index");
		return { pageCount: wikiMgr.getPages(wikiId).length };
	};
	const callbackConfig = config.tmcCallbackUrl ? {
		tmcCallbackUrl: config.tmcCallbackUrl,
		resolveLlm
	} : void 0;
	const sharedQueue = new BuildQueue();
	const wikiService = new WikiService({
		store,
		dataRoot: dataDir,
		worker: config.wikiWorker ?? realWikiWorker,
		queue: sharedQueue,
		logger: {
			info: log$4.info.bind(log$4),
			warn: log$4.warn.bind(log$4),
			error: log$4.error.bind(log$4)
		},
		callbackConfig
	});
	const cgService = new CodeGraphService({
		store,
		dataRoot: dataDir,
		worker: config.codeWorker ?? realCodeWorker,
		queue: sharedQueue,
		logger: {
			info: log$4.info.bind(log$4),
			warn: log$4.warn.bind(log$4),
			error: log$4.error.bind(log$4)
		},
		callbackConfig,
		releaseInstance: (codeGraphId) => {
			const inst = instancePool.get(codeGraphId);
			if (inst) closeIndex(inst);
			instancePool.delete(codeGraphId);
		}
	});
	const interrupted = store.markInterruptedAsFailed();
	if (interrupted > 0) log$4.info(`marked ${interrupted} interrupted tasks as failed`);
	(async () => {
		try {
			const allSynced = store.listSyncedCodeGraphs();
			for (const row of allSynced) {
				const dir = join(dataDir, row.service_id, row.team_id, row.code_graph_id);
				try {
					const instance = await openIndex(dir);
					instancePool.set(row.code_graph_id, instance);
					const rawStats = getStats(instance);
					if (rawStats) {
						const statsJson = JSON.stringify({
							files: rawStats.fileCount ?? rawStats.files ?? 0,
							nodes: rawStats.nodeCount ?? rawStats.nodes ?? 0,
							edges: rawStats.edgeCount ?? rawStats.edges ?? 0
						});
						store.updateCodeGraphStatus(row.service_id, row.code_graph_id, { stats_json: statsJson });
					}
					log$4.info(`[code-graph] restored ${row.code_graph_id}`);
				} catch (err) {
					log$4.warn(`[code-graph] failed to restore ${row.code_graph_id}: ${err instanceof Error ? err.message : String(err)}`);
				}
			}
			log$4.info(`[code-graph] ${allSynced.length} synced instances restored`);
		} catch (err) {
			log$4.warn(`[code-graph] restore scan failed: ${err instanceof Error ? err.message : String(err)}`);
		}
		try {
			const allSyncedWikis = store.listSyncedWikis();
			for (const row of allSyncedWikis) {
				const dir = join(dataDir, row.service_id, row.team_id, row.wiki_id);
				try {
					wikiMgr.init({
						name: row.wiki_id,
						path: dir
					});
					const pages = wikiMgr.getPages(row.wiki_id);
					if (pages.length > 0) store.updateWikiStatus(row.service_id, row.wiki_id, { page_count: pages.length });
					log$4.info(`[wiki] restored index ${row.wiki_id} (${pages.length} pages)`);
				} catch (err) {
					log$4.warn(`[wiki] failed to restore ${row.wiki_id}: ${err instanceof Error ? err.message : String(err)}`);
				}
			}
		} catch (err) {
			log$4.warn(`[wiki] restore scan failed: ${err instanceof Error ? err.message : String(err)}`);
		}
	})();
	const autoSyncConfig = resolveAutoSyncConfig();
	const autoSyncScheduler = new AutoSyncScheduler({
		store,
		cgService,
		config: autoSyncConfig
	});
	autoSyncScheduler.start();
	return {
		wikiService,
		cgService,
		wikiMgr,
		store,
		instancePool,
		llmBindingStore,
		autoSyncScheduler,
		autoSyncConfig
	};
}
//#endregion
//#region src/api-helpers.ts
/**
* Whitelist for id segments that get concatenated into filesystem paths
* (`data/{service_id}/{team_id}/{resource_id}/`). Even with auth disabled
* (001 Q2), this MUST be enforced to prevent path traversal (001 R5):
* only `A-Za-z0-9_-`, non-empty, bounded length.
*/
const ID_SEGMENT_PATTERN = /^[A-Za-z0-9_-]+$/;
const ID_SEGMENT_MAX = 200;
/** True if `id` is a safe path segment (whitelist + bounded). */
function isValidIdSegment(id) {
	return typeof id === "string" && id.length > 0 && id.length <= ID_SEGMENT_MAX && ID_SEGMENT_PATTERN.test(id);
}
/**
* Validate the `service_id` tenant identity taken from the `x-tdai-service-id`
* header. Returns the value or null when missing/malformed (route → 400).
* service_id 自报（内网信任，001 Q2/Q7），统一走 header（= 内核 x-tdai-service-id）。
*/
function extractServiceId(headerValue) {
	return isValidIdSegment(headerValue) ? headerValue : null;
}
/**
* Extract IdFields: `service_id` from the `x-tdai-service-id` header,
* `team_id` from the request body. Both required AND valid path segments (R5);
* the rest are optional body fields.
* @returns IdFields or null (when service_id/team_id missing or malformed).
*/
function extractIdFields(serviceIdHeader, body) {
	const serviceId = extractServiceId(serviceIdHeader);
	if (serviceId === null) return null;
	const teamId = body.team_id;
	if (!isValidIdSegment(teamId)) return null;
	const fields = {
		service_id: serviceId,
		team_id: teamId
	};
	if (typeof body.user_id === "string" && body.user_id) fields.user_id = body.user_id;
	if (typeof body.agent_id === "string" && body.agent_id) fields.agent_id = body.agent_id;
	if (typeof body.task_id === "string" && body.task_id) fields.task_id = body.task_id;
	return fields;
}
function wrapOk(data, requestId) {
	return {
		code: 0,
		message: "ok",
		...requestId ? { request_id: requestId } : {},
		data
	};
}
function wrapError(code, message, requestId) {
	return {
		code,
		message,
		...requestId ? { request_id: requestId } : {},
		data: null
	};
}
function toExternalVersion(v) {
	return String(v);
}
function toWikiDetail(row) {
	return {
		wiki_id: row.wiki_id,
		team_id: row.team_id,
		name: row.name,
		service_url: row.service_url ?? null,
		summary: row.summary ?? null,
		status: row.status,
		internal_status: row.internal_status,
		sync_error: row.sync_error,
		version: toExternalVersion(row.version),
		owner_user_id: row.owner_user_id,
		page_count: row.page_count ?? null,
		last_sync_at: row.last_sync_at,
		created_at: row.created_at,
		updated_at: row.updated_at
	};
}
function toCodeGraphDetail(row) {
	let stats = null;
	if (row.stats_json) try {
		stats = JSON.parse(row.stats_json);
	} catch {}
	return {
		code_graph_id: row.code_graph_id,
		team_id: row.team_id,
		repo_name: row.repo_name,
		repo_url: row.repo_url,
		branch: row.branch,
		commit_hash: row.commit_hash,
		service_url: row.service_url ?? null,
		summary: row.summary ?? null,
		status: row.status,
		sync_error: row.sync_error,
		version: toExternalVersion(row.version),
		owner_user_id: row.owner_user_id,
		stats,
		last_sync_at: row.last_sync_at,
		created_at: row.created_at,
		updated_at: row.updated_at
	};
}
//#endregion
//#region src/routes/wiki.ts
/**
* Wiki Routes — 15 endpoints (Hono rewrite).
*
* Asset (5): create / get / list / delete / ingest
* File (8): raw/{ls,read,write,rm} + page/{ls,read,write,rm}
* Derived (2): graph / search
*
* All POST, unified ApiResponseEnvelope.
* Routes are defined WITHOUT /v2 prefix — the prefix is applied once at server.ts mount level.
*
* Multi-tenancy (001): `service_id` is REQUIRED via the `x-tdai-service-id` header on
* EVERY endpoint (unified with the kernel routing key). id-only endpoints resolve
* `getById(service_id, wiki_id)` so a foreign tenant's resource is never exposed (R1).
* service_id / wiki_id are validated as safe path segments before use (R5).
*
* 细粒度 ingest progress 不在 KS 暴露：由 Panel 收 ingest_progress 回调并在 wiki/get 聚合。
*/
/** Handle WriteOutcome error codes → HTTP response. Returns Response if handled, null otherwise. */
function maybeWriteError(outcome) {
	if (outcome === null) return Response.json(wrapError(404, "wiki not found"), { status: 404 });
	if (outcome === "processing") return Response.json(wrapError(409, "wiki is processing; cannot write/delete"), { status: 409 });
	if (outcome === "invalid_path") return Response.json(wrapError(400, "invalid path: traversal detected"), { status: 400 });
	if (outcome === "forbidden_path") return Response.json(wrapError(400, "forbidden path (structural file or outside wiki/)"), { status: 400 });
	if (outcome === "too_large") return Response.json(wrapError(413, "content exceeds size limit"), { status: 413 });
	return null;
}
function createWikiRoutes(deps) {
	const app = new Hono();
	const { wikiService, wikiMgr, publicBaseUrl } = deps;
	app.post("/get", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const wikiId = body.wiki_id;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		const row = wikiService.getById(serviceId, wikiId);
		if (!row) return c.json(wrapError(404, "wiki not found"), 404);
		return c.json(wrapOk(toWikiDetail(row)));
	});
	app.post("/ingest", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const wikiId = body.wiki_id;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		const requesterUserId = typeof body.user_id === "string" && body.user_id ? body.user_id : void 0;
		const row = wikiService.getById(serviceId, wikiId);
		if (!row) return c.json(wrapError(404, "wiki not found"), 404);
		const sources = wikiService.rawLs(serviceId, row.team_id, wikiId);
		if (!sources || sources.length === 0) return c.json(wrapError(400, "wiki has no source files, upload before ingest"), 400);
		const result = wikiService.ingest(serviceId, row.team_id, wikiId, requesterUserId);
		if (result.kind === "not_found") return c.json(wrapError(404, "wiki not found"), 404);
		if (result.kind === "busy") return c.json({
			code: 409,
			message: "busy",
			data: {
				status: result.status,
				step: result.step
			}
		}, 409);
		return c.json(wrapOk({
			wiki_id: result.row.wiki_id,
			status: result.row.status
		}), 202);
	});
	app.post("/delete", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const wikiIds = body.wiki_ids;
		if (!Array.isArray(wikiIds) || wikiIds.length === 0) return c.json(wrapError(400, "wiki_ids is required (non-empty array)"), 400);
		if (wikiIds.length > 100) return c.json(wrapError(400, "wiki_ids exceeds max 100"), 400);
		const result = {
			deleted_ids: [],
			failed: []
		};
		for (const id of wikiIds) {
			if (!isValidIdSegment(id)) {
				result.failed.push({
					id: String(id),
					reason: "invalid id"
				});
				continue;
			}
			const row = wikiService.getById(serviceId, id);
			if (!row) {
				result.failed.push({
					id,
					reason: "not found"
				});
				continue;
			}
			if (wikiService.delete(serviceId, row.team_id, id)) {
				try {
					wikiMgr.remove(id);
				} catch (err) {
					console.warn(`[wiki] wikiMgr.remove(${id}) failed:`, err);
				}
				result.deleted_ids.push(id);
			} else result.failed.push({
				id,
				reason: "delete failed"
			});
		}
		return c.json(wrapOk(result));
	});
	app.post("/update-meta", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const wikiId = body.wiki_id;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		const patch = {};
		if (typeof body.name === "string" && body.name) patch.name = body.name;
		if (body.summary !== void 0) patch.summary = typeof body.summary === "string" ? body.summary : null;
		if (!patch.name && patch.summary === void 0) return c.json(wrapError(400, "at least one of name/summary must be provided"), 400);
		const updated = wikiService.updateMeta(serviceId, wikiId, patch);
		if (!updated) return c.json(wrapError(404, "wiki not found"), 404);
		return c.json(wrapOk(toWikiDetail(updated)));
	});
	app.post("/create", async (c) => {
		const body = await c.req.json();
		const ids = extractIdFields(c.req.header("x-tdai-service-id"), body);
		if (!ids) return c.json(wrapError(400, "x-tdai-service-id header and team_id are required"), 400);
		const name = body.name;
		if (typeof name !== "string" || !name) return c.json(wrapError(400, "name is required"), 400);
		const { row, existed } = wikiService.create({
			service_id: ids.service_id,
			team_id: ids.team_id,
			name,
			owner_user_id: ids.user_id,
			user_id: ids.user_id,
			agent_id: ids.agent_id,
			task_id: ids.task_id
		});
		if (!existed && publicBaseUrl) {
			const serviceUrl = publicBaseUrl;
			const updated = wikiService.updateServiceUrl(ids.service_id, row.wiki_id, serviceUrl);
			if (updated) return c.json(wrapOk(toWikiDetail(updated)), 201);
		}
		return c.json(wrapOk(toWikiDetail(row)), existed ? 200 : 201);
	});
	app.post("/list", async (c) => {
		const body = await c.req.json();
		const ids = extractIdFields(c.req.header("x-tdai-service-id"), body);
		if (!ids) return c.json(wrapError(400, "x-tdai-service-id header and team_id are required"), 400);
		const status = typeof body.status === "string" ? body.status : void 0;
		const limit = typeof body.limit === "number" ? body.limit : 20;
		const offset = typeof body.offset === "number" ? body.offset : 0;
		const items = wikiService.list(ids.service_id, ids.team_id, {
			syncStatus: status,
			limit,
			offset
		});
		const total = wikiService.count(ids.service_id, ids.team_id, status ? { syncStatus: status } : void 0);
		return c.json(wrapOk({
			items: items.map(toWikiDetail),
			total
		}));
	});
	app.post("/raw/ls", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const wikiId = body.wiki_id;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		const row = wikiService.getById(serviceId, wikiId);
		if (!row) return c.json(wrapError(404, "wiki not found"), 404);
		const items = wikiService.rawLs(serviceId, row.team_id, wikiId);
		if (items === null) return c.json(wrapError(404, "wiki not found"), 404);
		return c.json(wrapOk({ items }));
	});
	app.post("/raw/read", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const filenames = body.filenames;
		if (!Array.isArray(filenames) || filenames.length === 0) return c.json(wrapError(400, "filenames is required (non-empty array)"), 400);
		if (!filenames.every((s) => typeof s === "string")) return c.json(wrapError(400, "filenames must be string[]"), 400);
		const wikiId = body.wiki_id;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		const row = wikiService.getById(serviceId, wikiId);
		if (!row) return c.json(wrapError(404, "wiki not found"), 404);
		try {
			const result = wikiService.rawReadMany(serviceId, row.team_id, wikiId, filenames);
			const err = maybeWriteError(result);
			if (err) return err;
			return c.json(wrapOk({ items: result }));
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			return c.json(wrapError(400, msg), 400);
		}
	});
	app.post("/raw/write", async (c) => {
		const body = await c.req.json();
		const ids = extractIdFields(c.req.header("x-tdai-service-id"), body);
		if (!ids) return c.json(wrapError(400, "x-tdai-service-id header and team_id are required"), 400);
		const wikiId = body.wiki_id;
		const files = body.files;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		if (!Array.isArray(files) || files.length === 0) return c.json(wrapError(400, "files is required (non-empty array)"), 400);
		const MAX_FILE_SIZE = 512 * 1024;
		const MAX_FILES = 10;
		const MAX_TOTAL = 5 * 1024 * 1024;
		if (files.length > MAX_FILES) return c.json(wrapError(413, `too many files (max ${MAX_FILES})`), 413);
		let totalSize = 0;
		const validated = [];
		for (const item of files) {
			if (!item || typeof item !== "object") return c.json(wrapError(400, "files items must be {filename, content}"), 400);
			const r = item;
			if (typeof r.filename !== "string" || !r.filename) return c.json(wrapError(400, "filename is required for each file"), 400);
			if (typeof r.content !== "string") return c.json(wrapError(400, "content must be string for each file"), 400);
			const size = Buffer.byteLength(r.content, "utf-8");
			if (size > MAX_FILE_SIZE) return c.json(wrapError(413, `file too large: ${r.filename} (max ${MAX_FILE_SIZE} bytes)`), 413);
			totalSize += size;
			validated.push({
				filename: r.filename,
				content: r.content
			});
		}
		if (totalSize > MAX_TOTAL) return c.json(wrapError(413, `total too large (max ${MAX_TOTAL} bytes)`), 413);
		try {
			const result = wikiService.rawWriteMany(ids.service_id, ids.team_id, wikiId, validated, ids.user_id);
			const err = maybeWriteError(result);
			if (err) return err;
			return c.json(wrapOk({ items: result }));
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			return c.json(wrapError(400, msg), 400);
		}
	});
	app.post("/raw/rm", async (c) => {
		const body = await c.req.json();
		const ids = extractIdFields(c.req.header("x-tdai-service-id"), body);
		if (!ids) return c.json(wrapError(400, "x-tdai-service-id header and team_id are required"), 400);
		const wikiId = body.wiki_id;
		const filenames = body.filenames;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		if (!Array.isArray(filenames) || filenames.length === 0) return c.json(wrapError(400, "filenames is required (non-empty array)"), 400);
		if (!filenames.every((s) => typeof s === "string")) return c.json(wrapError(400, "filenames must be string[]"), 400);
		try {
			const result = await wikiService.rawRm(ids.service_id, ids.team_id, wikiId, filenames);
			const err = maybeWriteError(result);
			if (err) return err;
			try {
				wikiMgr.sync(wikiId);
			} catch (e) {
				console.warn(`[wiki] wikiMgr.sync(${wikiId}) failed after raw/rm:`, e);
			}
			return c.json(wrapOk(result));
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			return c.json(wrapError(400, msg), 400);
		}
	});
	app.post("/page/ls", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const wikiId = body.wiki_id;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		const row = wikiService.getById(serviceId, wikiId);
		if (!row) return c.json(wrapError(404, "wiki not found"), 404);
		const items = wikiService.pageLs(serviceId, row.team_id, wikiId);
		if (items === null) return c.json(wrapError(404, "wiki not found"), 404);
		return c.json(wrapOk({ items }));
	});
	app.post("/page/read", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const refs = body.refs;
		if (!Array.isArray(refs) || refs.length === 0) return c.json(wrapError(400, "refs is required (non-empty array)"), 400);
		if (!refs.every((s) => typeof s === "string")) return c.json(wrapError(400, "refs must be string[]"), 400);
		const wikiId = body.wiki_id;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		const row = wikiService.getById(serviceId, wikiId);
		if (!row) return c.json(wrapError(404, "wiki not found"), 404);
		try {
			const result = wikiService.pageReadMany(serviceId, row.team_id, wikiId, refs);
			const err = maybeWriteError(result);
			if (err) return err;
			return c.json(wrapOk({ items: result }));
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			return c.json(wrapError(400, msg), 400);
		}
	});
	app.post("/page/write", async (c) => {
		const body = await c.req.json();
		const ids = extractIdFields(c.req.header("x-tdai-service-id"), body);
		if (!ids) return c.json(wrapError(400, "x-tdai-service-id header and team_id are required"), 400);
		const wikiId = body.wiki_id;
		const pages = body.pages;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		if (!Array.isArray(pages) || pages.length === 0) return c.json(wrapError(400, "pages is required (non-empty array)"), 400);
		const validated = [];
		for (const item of pages) {
			if (!item || typeof item !== "object") return c.json(wrapError(400, "pages items must be {ref, content}"), 400);
			const r = item;
			if (typeof r.ref !== "string" || !r.ref) return c.json(wrapError(400, "ref is required for each page"), 400);
			if (typeof r.content !== "string") return c.json(wrapError(400, "content must be string for each page"), 400);
			validated.push({
				ref: r.ref,
				content: r.content
			});
		}
		try {
			const result = wikiService.pageWriteMany(ids.service_id, ids.team_id, wikiId, validated);
			const err = maybeWriteError(result);
			if (err) return err;
			try {
				wikiMgr.sync(wikiId);
			} catch (e) {
				console.warn(`[wiki] wikiMgr.sync(${wikiId}) failed after page/write:`, e);
			}
			return c.json(wrapOk({ items: result }));
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			return c.json(wrapError(400, msg), 400);
		}
	});
	app.post("/page/rm", async (c) => {
		const body = await c.req.json();
		const ids = extractIdFields(c.req.header("x-tdai-service-id"), body);
		if (!ids) return c.json(wrapError(400, "x-tdai-service-id header and team_id are required"), 400);
		const wikiId = body.wiki_id;
		const refs = body.refs;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		if (!Array.isArray(refs) || refs.length === 0) return c.json(wrapError(400, "refs is required (non-empty array)"), 400);
		if (!refs.every((s) => typeof s === "string")) return c.json(wrapError(400, "refs must be string[]"), 400);
		try {
			const result = await wikiService.pageRm(ids.service_id, ids.team_id, wikiId, refs);
			const err = maybeWriteError(result);
			if (err) return err;
			try {
				wikiMgr.sync(wikiId);
			} catch (e) {
				console.warn(`[wiki] wikiMgr.sync(${wikiId}) failed after page/rm:`, e);
			}
			return c.json(wrapOk(result));
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			return c.json(wrapError(400, msg), 400);
		}
	});
	app.post("/graph", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const wikiId = body.wiki_id;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		const row = wikiService.getById(serviceId, wikiId);
		if (!row) return c.json(wrapError(404, "wiki not found"), 404);
		if (row.status !== "ready") return c.json(wrapOk({
			nodes: [],
			edges: [],
			communities: []
		}));
		const graphData = wikiMgr.graph(wikiId);
		return c.json(wrapOk(graphData));
	});
	app.post("/search", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const query = body.query;
		if (typeof query !== "string" || !query) return c.json(wrapError(400, "query is required"), 400);
		const wikiId = body.wiki_id;
		if (!isValidIdSegment(wikiId)) return c.json(wrapError(400, "wiki_id is required"), 400);
		const row = wikiService.getById(serviceId, wikiId);
		if (!row) return c.json(wrapError(404, "wiki not found"), 404);
		if (row.status !== "ready") return c.json(wrapOk({
			results: [],
			links: [],
			count: 0
		}));
		const limit = typeof body.limit === "number" ? body.limit : 20;
		let hop;
		if (body.hop !== void 0) {
			if (typeof body.hop !== "number" || !Number.isInteger(body.hop) || body.hop < 0 || body.hop > 5) return c.json(wrapError(400, "hop must be an integer in 0..5"), 400);
			hop = body.hop;
		}
		let decay;
		if (body.decay !== void 0) {
			if (typeof body.decay !== "number" || body.decay < 0 || body.decay > 1 || Number.isNaN(body.decay)) return c.json(wrapError(400, "decay must be a number in 0..1"), 400);
			decay = body.decay;
		}
		let minScore;
		if (body.minScore !== void 0) {
			if (typeof body.minScore !== "number" || body.minScore < 0 || Number.isNaN(body.minScore)) return c.json(wrapError(400, "minScore must be a non-negative number"), 400);
			minScore = body.minScore;
		}
		const response = wikiMgr.search(wikiId, query, limit, {
			hop,
			decay,
			minScore
		});
		return c.json(wrapOk(response));
	});
	return app;
}
//#endregion
//#region src/routes/tools.ts
/**
* Tools Routes — Agent self-discovery HTTP endpoints.
*
* Two endpoints for the v7 progressive-exposure pattern:
*   POST /tools/list — discover available tools for a knowledge resource
*   POST /tools/call — execute a tool on a knowledge resource
*
* Tools are defined per resource type (wiki / code-graph). Management operations
* (create/delete/ingest/sync) are NOT exposed — only read-only query tools.
*
* Routes are defined WITHOUT /v3 prefix — prefix applied at server.ts mount level.
*/
/** Wiki tools (7) — read-only query tools for LLM agents. */
const WIKI_TOOLS = [
	{
		name: "get_info",
		description: "获取 wiki 元信息（名称、状态、页面数等）。",
		params: {}
	},
	{
		name: "search",
		description: "BM25 全文搜索 wiki 页面内容。用关键词查找相关文档。",
		params: {
			query: {
				type: "string",
				required: true,
				description: "搜索关键词"
			},
			limit: {
				type: "integer",
				required: false,
				default: 20,
				description: "返回结果数上限"
			}
		}
	},
	{
		name: "list_pages",
		description: "列出所有页面引用（id + title + path）。",
		params: {}
	},
	{
		name: "read_page",
		description: "读取指定页面完整内容。",
		params: { refs: {
			type: "array",
			required: true,
			description: "页面引用数组（id 或路径）"
		} }
	},
	{
		name: "get_graph",
		description: "获取知识图谱结构（nodes, edges, communities）。",
		params: {}
	},
	{
		name: "list_raw",
		description: "列出原始上传文件。",
		params: {}
	},
	{
		name: "read_raw",
		description: "读取指定原始文件内容。",
		params: { filenames: {
			type: "array",
			required: true,
			description: "文件名数组"
		} }
	}
];
/** Code-Graph tools (9) — read-only query tools for LLM agents. */
const CODE_GRAPH_TOOLS = [
	{
		name: "get_info",
		description: "获取 code-graph 元信息（仓库名、状态、统计等）。",
		params: {}
	},
	{
		name: "search",
		description: "按名称快速搜索符号，只返回位置（不含源码）。想直接拿到源码/理解某块代码，请改用 explore。",
		params: {
			query: {
				type: "string",
				required: true,
				description: "符号名或部分名称（如 \"auth\"、\"signIn\"、\"UserService\"）"
			},
			kind: {
				type: "string",
				required: false,
				enum: [
					"function",
					"method",
					"class",
					"interface",
					"type",
					"variable",
					"route",
					"component"
				],
				description: "按节点类型过滤。省略则搜索全部类型（不要传 \"any\"/\"symbol\"/\"file\"，这些不是合法值，会导致零结果）。"
			},
			limit: {
				type: "integer",
				required: false,
				default: 10,
				description: "返回结果数上限"
			}
		}
	},
	{
		name: "explore",
		description: "【首选工具】几乎任何问题都先用它：X 怎么工作、架构、定位 bug、某处在哪。一次调用即按文件分组返回相关符号的完整源码（等价于 Read，返回的文件不要再重复读）。query 可以是自然语言问题，也可以是一组符号/文件名。通常一次就够，无需再 search/get_node/读文件。",
		params: {
			query: {
				type: "string",
				required: true,
				description: "要探索的符号名、文件名或简短代码词（如 \"AuthService loginUser session-manager\"）。可先用 search 找到相关名称。"
			},
			maxFiles: {
				type: "integer",
				required: false,
				default: 12,
				description: "最多返回源码的文件数（默认 12）"
			}
		}
	},
	{
		name: "callers",
		description: "列出调用 <symbol> 的函数。想看完整调用流程请用 explore。",
		params: {
			symbol: {
				type: "string",
				required: true,
				description: "要查调用者的函数/方法/类名"
			},
			limit: {
				type: "integer",
				required: false,
				default: 20,
				description: "返回结果数上限（默认 20）"
			}
		}
	},
	{
		name: "callees",
		description: "列出 <symbol> 调用的函数。想看完整调用流程请用 explore。",
		params: {
			symbol: {
				type: "string",
				required: true,
				description: "要查被调用者的函数/方法/类名"
			},
			limit: {
				type: "integer",
				required: false,
				default: 20,
				description: "返回结果数上限（默认 20）"
			}
		}
	},
	{
		name: "impact",
		description: "列出修改 <symbol> 会影响到的符号。重构前先用它评估影响面。",
		params: {
			symbol: {
				type: "string",
				required: true,
				description: "要做影响分析的符号名"
			},
			depth: {
				type: "integer",
				required: false,
				default: 2,
				description: "依赖遍历层数（默认 2）"
			}
		}
	},
	{
		name: "node",
		description: "【explore 之后的次选】获取单个符号的完整信息：位置、签名、调用链、以及逐字源码（includeCode=true）。名称有重载/多定义时会一次返回全部匹配定义的完整 body；可用 file/line 精确定位某个重载。需要多个相关符号或完整流程时请用 explore。",
		params: {
			symbol: {
				type: "string",
				required: true,
				description: "要查详情的符号名"
			},
			includeCode: {
				type: "boolean",
				required: false,
				default: false,
				description: "是否包含完整源码（默认 false 以节省上下文）"
			},
			file: {
				type: "string",
				required: false,
				description: "可选：用文件路径/文件名消歧重载（如 \"harness.rs\"）"
			},
			line: {
				type: "integer",
				required: false,
				description: "可选：用行号消歧到该位置附近的定义"
			}
		}
	},
	{
		name: "status",
		description: "索引健康检查（文件/节点/边数量）。除非排查问题，一般不需要。",
		params: {}
	},
	{
		name: "files",
		description: "索引到的文件树，含语言与符号数。查看项目结构比 Glob 更快。",
		params: {
			path: {
				type: "string",
				required: false,
				description: "按目录前缀过滤（如 \"src/components\"），不传则返回全部"
			},
			pattern: {
				type: "string",
				required: false,
				description: "按 glob 模式过滤（如 \"*.tsx\"、\"**/*.test.ts\"）"
			},
			format: {
				type: "string",
				required: false,
				default: "tree",
				enum: [
					"tree",
					"flat",
					"grouped"
				],
				description: "输出格式：tree（层级，默认）、flat（平铺列表）、grouped（按语言分组）"
			}
		}
	}
];
/** Agent read-only whitelist — management ops NOT included. */
const WIKI_TOOL_NAMES = new Set(WIKI_TOOLS.map((t) => t.name));
const CODE_GRAPH_TOOL_NAMES = new Set(CODE_GRAPH_TOOLS.map((t) => t.name));
function createToolsRoutes(deps) {
	const app = new Hono();
	const { wikiService, wikiMgr, cgService, instancePool } = deps;
	app.post("/list", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const knowledgeId = body.knowledge_id;
		if (typeof knowledgeId !== "string" || !knowledgeId) return c.json(wrapError(400, "knowledge_id is required"), 400);
		let type;
		let tools;
		let name;
		let summary;
		let status;
		if (isWikiId(knowledgeId)) {
			type = "wiki";
			tools = WIKI_TOOLS;
			const row = wikiService.getById(serviceId, knowledgeId);
			if (!row) return c.json(wrapError(404, "knowledge resource not found"), 404);
			name = row.name;
			summary = row.summary ?? null;
			status = row.status;
		} else if (isCodeGraphId(knowledgeId)) {
			type = "code-graph";
			tools = CODE_GRAPH_TOOLS;
			const row = cgService.getById(serviceId, knowledgeId);
			if (!row) return c.json(wrapError(404, "knowledge resource not found"), 404);
			name = row.repo_name || row.repo_url;
			summary = row.summary ?? null;
			status = row.status;
		} else return c.json(wrapError(400, `invalid knowledge_id format: ${knowledgeId}`), 400);
		return c.json(wrapOk({
			knowledge_id: knowledgeId,
			type,
			name,
			summary,
			status,
			tools: tools.map((t) => ({
				name: t.name,
				description: t.description,
				params: t.params
			}))
		}));
	});
	app.post("/call", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const knowledgeId = body.knowledge_id;
		if (typeof knowledgeId !== "string" || !knowledgeId) return c.json(wrapError(400, "knowledge_id is required"), 400);
		const toolName = body.tool_name;
		if (typeof toolName !== "string" || !toolName) return c.json(wrapError(400, "tool_name is required"), 400);
		const params = body.params;
		if (!params || typeof params !== "object") return c.json(wrapError(400, "params is required (object)"), 400);
		const toolParams = params;
		if (isWikiId(knowledgeId)) {
			if (!WIKI_TOOL_NAMES.has(toolName)) return c.json(wrapError(403, `unknown tool: '${toolName}' for wiki resource '${knowledgeId}'. Use tools/list to discover available tools.`), 403);
			const row = wikiService.getById(serviceId, knowledgeId);
			if (!row) return c.json(wrapError(404, "wiki not found"), 404);
			return executeWikiTool(serviceId, toolName, row, toolParams, wikiService, wikiMgr);
		}
		if (isCodeGraphId(knowledgeId)) {
			if (!CODE_GRAPH_TOOL_NAMES.has(toolName)) return c.json(wrapError(403, `unknown tool: '${toolName}' for code-graph resource '${knowledgeId}'. Use tools/list to discover available tools.`), 403);
			const row = cgService.getById(serviceId, knowledgeId);
			if (!row) return c.json(wrapError(404, "code graph not found"), 404);
			return executeCodeGraphTool(serviceId, toolName, row, toolParams, cgService, instancePool);
		}
		return c.json(wrapError(400, `invalid knowledge_id format: ${knowledgeId}`), 400);
	});
	return app;
}
async function executeWikiTool(serviceId, toolName, row, params, wikiService, wikiMgr) {
	const { wiki_id, team_id } = row;
	switch (toolName) {
		case "get_info": {
			const detail = wikiService.get(serviceId, team_id, wiki_id);
			if (!detail) return Response.json(wrapError(404, "wiki not found"), { status: 404 });
			return Response.json(wrapOk(detail));
		}
		case "search": {
			const query = params.query;
			if (typeof query !== "string" || !query) return Response.json(wrapError(400, "query is required"), { status: 400 });
			if (row.status !== "ready") return Response.json(wrapOk({
				results: [],
				links: [],
				count: 0
			}));
			const limit = typeof params.limit === "number" ? params.limit : 20;
			const response = wikiMgr.search(wiki_id, query, limit);
			return Response.json(wrapOk(response));
		}
		case "list_pages": {
			if (row.status !== "ready") return Response.json(wrapOk({ items: [] }));
			const items = wikiService.pageLs(serviceId, team_id, wiki_id);
			if (items === null) return Response.json(wrapError(404, "wiki not found"), { status: 404 });
			return Response.json(wrapOk({ items }));
		}
		case "read_page": {
			const refs = params.refs;
			if (!Array.isArray(refs) || refs.length === 0) return Response.json(wrapError(400, "refs is required (non-empty array)"), { status: 400 });
			if (row.status !== "ready") return Response.json(wrapOk({ items: [] }));
			const result = wikiService.pageReadMany(serviceId, team_id, wiki_id, refs);
			return Response.json(wrapOk({ items: result }));
		}
		case "get_graph": {
			if (row.status !== "ready") return Response.json(wrapOk({
				nodes: [],
				edges: [],
				communities: []
			}));
			const graphData = wikiMgr.graph(wiki_id);
			return Response.json(wrapOk(graphData));
		}
		case "list_raw": {
			const items = wikiService.rawLs(serviceId, team_id, wiki_id);
			if (items === null) return Response.json(wrapError(404, "wiki not found"), { status: 404 });
			return Response.json(wrapOk({ items }));
		}
		case "read_raw": {
			const filenames = params.filenames;
			if (!Array.isArray(filenames) || filenames.length === 0) return Response.json(wrapError(400, "filenames is required (non-empty array)"), { status: 400 });
			const result = wikiService.rawReadMany(serviceId, team_id, wiki_id, filenames);
			return Response.json(wrapOk({ items: result }));
		}
		default: return Response.json(wrapError(403, `unknown tool: ${toolName}`), { status: 403 });
	}
}
/**
* 对外暴露的 codegraph 查询工具名（不含 get_info，get_info 在调用方特殊处理）。
* 单一真相源：tools.ts 的 CODE_GRAPH_TOOLS、code-graph.ts 的路由注册、
* toCodeGraphToolName 的校验列表，全部从这里来。
*/
const CODEGRAPH_QUERY_TOOL_NAMES = [
	"search",
	"explore",
	"callers",
	"callees",
	"impact",
	"node",
	"status",
	"files"
];
/**
* 把对外暴露的工具名映射为 executeTool 接受的内部工具名。
* 对外统一用短名（node / status / files），内部统一加 codegraph_ 前缀。
*/
function toCodeGraphToolName(externalName) {
	return CODEGRAPH_QUERY_TOOL_NAMES.includes(externalName) ? `codegraph_${externalName}` : void 0;
}
async function executeCodeGraphTool(serviceId, toolName, row, params, cgService, instancePool) {
	const { code_graph_id, team_id } = row;
	if (toolName === "get_info") {
		const detail = cgService.get(serviceId, team_id, code_graph_id);
		if (!detail) return Response.json(wrapError(404, "code graph not found"), { status: 404 });
		return Response.json(wrapOk(detail));
	}
	if (row.status !== "ready") return Response.json(wrapOk({
		text: "",
		isError: false
	}));
	const cgToolName = toCodeGraphToolName(toolName);
	if (!cgToolName) return Response.json(wrapError(403, `unknown tool: ${toolName}`), { status: 403 });
	const toolParams = { code_graph_id };
	for (const [k, v] of Object.entries(params)) toolParams[k] = v;
	let instance = instancePool.get(code_graph_id);
	if (!instance && instancePool.loadIfMissing) {
		const dir = cgService.dirFor(serviceId, team_id, code_graph_id);
		instance = await instancePool.loadIfMissing(code_graph_id, dir);
	}
	if (!instance) return Response.json(wrapError(503, "code graph instance not loaded"), { status: 503 });
	const result = await executeTool(instance, cgToolName, toolParams);
	return Response.json(wrapOk(result), { status: result.isError ? 500 : 200 });
}
//#endregion
//#region src/routes/code-graph.ts
/**
* Code-Graph Routes — 13 endpoints (Hono rewrite).
*
* Management (5): create / list / get / sync / delete
* Query (8): search / explore / callers / callees / impact / node / status / files
*
* Query endpoints delegate to engines/code executeTool, return {text, isError}.
* Routes are defined WITHOUT /v2 prefix — prefix applied at server.ts mount level.
*
* 多租户（001）：`service_id` 每个端点必传于 `x-tdai-service-id` 请求头（与内核路由键统一）。
* id-only 端点用 `getById(service_id, code_graph_id)` 收敛归属，跨租户返回 404（R1）；
* service_id / code_graph_id 先做路径分段白名单校验（R5）。
*/
const QUERY_SPECS = {
	search: { fields: {
		query: {
			kind: "string",
			required: true
		},
		kind: {
			kind: "stringEnum",
			values: [
				"function",
				"method",
				"class",
				"interface",
				"type",
				"variable",
				"route",
				"component"
			],
			passthrough: true
		},
		limit: {
			kind: "int",
			min: 1,
			max: 100,
			default: 10
		}
	} },
	explore: { fields: {
		query: {
			kind: "string",
			required: true
		},
		maxFiles: {
			kind: "int",
			min: 1,
			max: 200,
			default: 12
		}
	} },
	callers: { fields: {
		symbol: {
			kind: "string",
			required: true
		},
		limit: {
			kind: "int",
			min: 1,
			max: 200,
			default: 20
		}
	} },
	callees: { fields: {
		symbol: {
			kind: "string",
			required: true
		},
		limit: {
			kind: "int",
			min: 1,
			max: 200,
			default: 20
		}
	} },
	impact: { fields: {
		symbol: {
			kind: "string",
			required: true
		},
		depth: {
			kind: "int",
			min: 1,
			max: 10,
			default: 2
		}
	} },
	node: { fields: {
		symbol: {
			kind: "string",
			required: true
		},
		includeCode: {
			kind: "boolean",
			default: false
		},
		file: { kind: "string" },
		line: {
			kind: "int",
			min: 1
		}
	} },
	status: { fields: {} },
	files: { fields: {
		path: { kind: "string" },
		pattern: { kind: "string" },
		format: {
			kind: "stringEnum",
			values: [
				"tree",
				"flat",
				"grouped"
			],
			default: "tree",
			passthrough: true
		},
		includeMetadata: {
			kind: "boolean",
			default: true
		},
		maxDepth: {
			kind: "int",
			min: 1
		}
	} }
};
/** Validate query params against spec whitelist + defaults. Returns toolParams or error string. */
function buildToolParams(action, body) {
	const spec = QUERY_SPECS[action];
	if (!spec) return { error: `unknown action: ${action}` };
	const allowed = new Set(["code_graph_id", ...Object.keys(spec.fields)]);
	for (const k of Object.keys(body)) if (!allowed.has(k)) return { error: `unexpected field: ${k}` };
	const params = {};
	for (const [name, rule] of Object.entries(spec.fields)) {
		const raw = body[name];
		if (!(raw !== void 0 && raw !== null)) {
			if (rule.required) return { error: `${name} is required` };
			if ("default" in rule && rule.default !== void 0) {
				if (rule.kind !== "stringEnum" || rule.passthrough !== false) params[name] = rule.default;
			}
			continue;
		}
		switch (rule.kind) {
			case "string":
				if (typeof raw !== "string" || !raw) return { error: `${name} must be non-empty string` };
				params[name] = raw;
				break;
			case "stringEnum":
				if (typeof raw !== "string" || !rule.values.includes(raw)) return { error: `${name} must be one of ${rule.values.join(", ")}` };
				if (rule.passthrough !== false) params[name] = raw;
				break;
			case "boolean":
				if (typeof raw !== "boolean") return { error: `${name} must be boolean` };
				params[name] = raw;
				break;
			case "int":
				if (typeof raw !== "number" || !Number.isFinite(raw) || !Number.isInteger(raw)) return { error: `${name} must be integer` };
				if (rule.min !== void 0 && raw < rule.min) return { error: `${name} must be >= ${rule.min}` };
				if (rule.max !== void 0 && raw > rule.max) return { error: `${name} must be <= ${rule.max}` };
				params[name] = raw;
				break;
		}
	}
	return { params };
}
function createCodeGraphRoutes(deps) {
	const app = new Hono();
	const { cgService, instancePool, publicBaseUrl } = deps;
	app.post("/create", async (c) => {
		const body = await c.req.json();
		const idFields = extractIdFields(c.req.header("x-tdai-service-id"), body);
		if (!idFields) return c.json(wrapError(400, "x-tdai-service-id header and team_id are required"), 400);
		const repoUrl = body.repo_url;
		if (typeof repoUrl !== "string" || !repoUrl) return c.json(wrapError(400, "repo_url is required"), 400);
		const branch = typeof body.branch === "string" && body.branch ? body.branch : "main";
		const repoName = typeof body.repo_name === "string" ? body.repo_name : void 0;
		const { row, existed } = cgService.create({
			service_id: idFields.service_id,
			team_id: idFields.team_id,
			repo_url: repoUrl,
			branch,
			repo_name: repoName,
			owner_user_id: idFields.user_id,
			user_id: idFields.user_id,
			agent_id: idFields.agent_id,
			task_id: idFields.task_id
		});
		if (!existed && publicBaseUrl) {
			const serviceUrl = publicBaseUrl;
			const updated = cgService.updateServiceUrl(idFields.service_id, row.code_graph_id, serviceUrl);
			if (updated) return c.json(wrapOk(toCodeGraphDetail(updated)), 201);
		}
		return c.json(wrapOk(toCodeGraphDetail(row)), existed ? 200 : 201);
	});
	app.post("/list", async (c) => {
		const body = await c.req.json();
		const idFields = extractIdFields(c.req.header("x-tdai-service-id"), body);
		if (!idFields) return c.json(wrapError(400, "x-tdai-service-id header and team_id are required"), 400);
		const status = typeof body.status === "string" ? body.status : void 0;
		const limit = typeof body.limit === "number" ? body.limit : 20;
		const offset = typeof body.offset === "number" ? body.offset : 0;
		const items = cgService.list(idFields.service_id, idFields.team_id, {
			syncStatus: status,
			limit,
			offset
		});
		const total = cgService.count(idFields.service_id, idFields.team_id, status ? { syncStatus: status } : void 0);
		return c.json(wrapOk({
			items: items.map(toCodeGraphDetail),
			total
		}));
	});
	app.post("/get", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const cgId = body.code_graph_id;
		if (!isValidIdSegment(cgId)) return c.json(wrapError(400, "code_graph_id is required"), 400);
		const row = cgService.getById(serviceId, cgId);
		if (!row) return c.json(wrapError(404, "code graph not found"), 404);
		return c.json(wrapOk(toCodeGraphDetail(row)));
	});
	app.post("/update-meta", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const cgId = body.code_graph_id;
		if (!isValidIdSegment(cgId)) return c.json(wrapError(400, "code_graph_id is required"), 400);
		const patch = {};
		if (typeof body.repo_name === "string" && body.repo_name) patch.repo_name = body.repo_name;
		if (body.summary !== void 0) patch.summary = typeof body.summary === "string" ? body.summary : null;
		if (!patch.repo_name && patch.summary === void 0) return c.json(wrapError(400, "at least one of repo_name/summary must be provided"), 400);
		const updated = cgService.updateMeta(serviceId, cgId, patch);
		if (!updated) return c.json(wrapError(404, "code graph not found"), 404);
		return c.json(wrapOk(toCodeGraphDetail(updated)));
	});
	app.post("/sync", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const cgId = body.code_graph_id;
		if (!isValidIdSegment(cgId)) return c.json(wrapError(400, "code_graph_id is required"), 400);
		const requesterUserId = typeof body.user_id === "string" && body.user_id ? body.user_id : void 0;
		const row = cgService.getById(serviceId, cgId);
		if (!row) return c.json(wrapError(404, "code graph not found"), 404);
		const result = cgService.sync(serviceId, row.team_id, cgId, requesterUserId);
		if (result.kind === "not_found") return c.json(wrapError(404, "code graph not found"), 404);
		if (result.kind === "busy") return c.json({
			code: 409,
			message: "busy",
			data: {
				status: result.status,
				step: result.step
			}
		}, 409);
		return c.json(wrapOk({
			code_graph_id: result.row.code_graph_id,
			status: result.row.status
		}), 202);
	});
	app.post("/delete", async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const cgIds = body.code_graph_ids;
		if (!Array.isArray(cgIds) || cgIds.length === 0) return c.json(wrapError(400, "code_graph_ids is required (non-empty array)"), 400);
		if (cgIds.length > 100) return c.json(wrapError(400, "code_graph_ids exceeds max 100"), 400);
		const result = {
			deleted_ids: [],
			failed: []
		};
		for (const id of cgIds) {
			if (!isValidIdSegment(id)) {
				result.failed.push({
					id: String(id),
					reason: "invalid id"
				});
				continue;
			}
			const row = cgService.getById(serviceId, id);
			if (!row) {
				result.failed.push({
					id,
					reason: "not found"
				});
				continue;
			}
			if (cgService.delete(serviceId, row.team_id, id)) result.deleted_ids.push(id);
			else result.failed.push({
				id,
				reason: "delete failed"
			});
		}
		return c.json(wrapOk(result));
	});
	for (const action of CODEGRAPH_QUERY_TOOL_NAMES) app.post(`/${action}`, async (c) => {
		const body = await c.req.json();
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const cgId = body.code_graph_id;
		if (!isValidIdSegment(cgId)) return c.json(wrapError(400, "code_graph_id is required"), 400);
		const row = cgService.getById(serviceId, cgId);
		if (!row) return c.json(wrapError(404, "code graph not found"), 404);
		if (row.status !== "ready") return c.json(wrapOk({
			text: "",
			isError: false
		}));
		let instance = instancePool.get(cgId);
		if (!instance && instancePool.loadIfMissing) {
			const dir = cgService.dirFor(serviceId, row.team_id, cgId);
			instance = await instancePool.loadIfMissing(cgId, dir);
		}
		if (!instance) return c.json(wrapError(503, "code graph instance not loaded"), 503);
		const built = buildToolParams(action, body);
		if ("error" in built) return c.json(wrapError(400, built.error), 400);
		const toolName = toCodeGraphToolName(action);
		if (!toolName) return c.json(wrapError(403, `unknown tool: ${action}`), 403);
		const result = await executeTool(instance, toolName, built.params);
		return c.json(wrapOk(result), result.isError ? 500 : 200);
	});
	return app;
}
//#endregion
//#region src/routes/health.ts
/**
* Health check route.
*/
function createHealthRoutes() {
	const app = new Hono();
	app.get("/health", (c) => {
		return c.json({
			status: "ok",
			timestamp: (/* @__PURE__ */ new Date()).toISOString()
		});
	});
	return app;
}
//#endregion
//#region src/routes/llm-binding.ts
/**
* LLM Binding Routes — internal, per-instance LLM routing config.
*
* Mounted under /v3/internal/llm-binding (prefix applied at server.ts).
* service_id is REQUIRED via the `x-tdai-service-id` header (unified KS convention),
* never in the body.
*
*   POST /set     upsert binding (proxy|byo). Idempotent — re-posting overwrites,
*                 which is how a lost binding is re-bound (TMC startup / manual curl).
*                 `api_key` 可选：不传时保留原值（仅对已存在的记录生效）；首次创建必填。
*   POST /status  read-side binding status (never returns api_key).
*   POST /list    列出所有 binding（不要求 service-id 头）。返回 has_api_key 标志，
*                 不返回 api_key 明文。供 Panel 启动时缓存状态用。
*
* These endpoints are internal (TMC control plane / operator curl). KS trusts the
* internal network like its other routes; no extra auth layer is added here.
*/
function isMode(v) {
	return v === "proxy" || v === "byo";
}
function asOptString(v) {
	return typeof v === "string" && v.length > 0 ? v : void 0;
}
function createLlmBindingRoutes(deps) {
	const app = new Hono();
	const { llmBindingStore } = deps;
	app.post("/set", async (c) => {
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		const body = await c.req.json().catch(() => ({}));
		const mode = body.mode;
		if (!isMode(mode)) return c.json(wrapError(400, "mode must be 'proxy' or 'byo'"), 400);
		const apiKey = asOptString(body.api_key);
		const proxyBaseUrl = asOptString(body.proxy_base_url);
		const baseUrl = asOptString(body.base_url);
		const enabled = body.enabled === void 0 ? void 0 : body.enabled !== false;
		if (mode === "proxy" && !proxyBaseUrl) return c.json(wrapError(400, "proxy mode requires proxy_base_url"), 400);
		if (mode === "byo" && !baseUrl) return c.json(wrapError(400, "byo mode requires base_url"), 400);
		if (!llmBindingStore.get(serviceId)) {
			if (!apiKey) return c.json(wrapError(400, `${mode} mode requires api_key on first set`), 400);
		}
		const row = llmBindingStore.upsert(serviceId, {
			mode,
			proxy_base_url: proxyBaseUrl ?? null,
			api_key: apiKey,
			base_url: baseUrl ?? null,
			enabled
		});
		return c.json(wrapOk({
			service_id: row.service_id,
			mode: row.mode,
			enabled: row.enabled,
			updated_at: row.updated_at
		}));
	});
	app.post("/status", async (c) => {
		const serviceId = c.req.header("x-tdai-service-id");
		if (!isValidIdSegment(serviceId)) return c.json(wrapError(400, "x-tdai-service-id header is required"), 400);
		return c.json(wrapOk(llmBindingStore.status(serviceId)));
	});
	app.post("/list", async (c) => {
		const items = llmBindingStore.listAll().map((r) => ({
			service_id: r.service_id,
			mode: r.mode,
			proxy_base_url: r.proxy_base_url,
			base_url: r.base_url,
			has_api_key: !!r.api_key && r.api_key.length > 0,
			enabled: r.enabled
		}));
		return c.json(wrapOk({ items }));
	});
	return app;
}
//#endregion
//#region src/routes/auto-sync.ts
/**
* Auto-Sync Admin Routes — 调度器状态查询 + 手动触发。
*
* Endpoints:
*   GET  /auto-sync/status   → 当前调度器状态（running/activeSyncs/scanning）+ 配置
*   POST /auto-sync/trigger  → 手动触发一轮全量扫描（fire-and-forget，立即返回）
*
* 路由挂载时无 prefix，由 server.ts 统一加 /v3。
*/
function createAutoSyncRoutes(deps) {
	const app = new Hono();
	const { scheduler, config } = deps;
	app.get("/auto-sync/status", (c) => {
		const status = scheduler.getStatus();
		return c.json(wrapOk({
			...status,
			config: {
				enabled: config.enabled,
				scanIntervalMs: config.scanIntervalMs,
				maxConcurrentSyncs: config.maxConcurrentSyncs
			}
		}));
	});
	app.post("/auto-sync/trigger", (c) => {
		if (!config.enabled) return c.json(wrapOk({
			triggered: false,
			reason: "auto-sync is disabled by KNOWLEDGE_AUTO_SYNC_ENABLED"
		}));
		scheduler.triggerScan();
		return c.json(wrapOk({ triggered: true }));
	});
	return app;
}
//#endregion
//#region src/middleware/response-envelope.ts
const log$3 = createLogger("http");
const MAX_BODY_LOG = 500;
function truncate(s, max) {
	return s.length <= max ? s : s.slice(0, max) + `…[+${s.length - max}]`;
}
/** 提取 request body 的关键字段（避免打全量，只打 ID 类字段便于关联）。 */
function pickReqFields(body) {
	if (!body || typeof body !== "object") return {};
	const b = body;
	const out = {};
	for (const k of [
		"wiki_id",
		"code_graph_id",
		"knowledge_id",
		"wiki_ids",
		"code_graph_ids",
		"knowledge_ids",
		"team_id",
		"repo_url",
		"branch",
		"filename",
		"filenames",
		"refs",
		"tool_name",
		"query",
		"path"
	]) if (k in b) out[k] = b[k];
	return out;
}
function accessLog() {
	return async (c, next) => {
		const t0 = Date.now();
		const route = `${c.req.method} ${c.req.path}`;
		const requestId = c.req.header("x-request-id") || crypto.randomUUID();
		c.set("requestId", requestId);
		let reqBody = void 0;
		if (c.req.method === "POST" || c.req.method === "PUT") try {
			const raw = await c.req.text();
			reqBody = raw ? JSON.parse(raw) : void 0;
			c.req.bodyCache.text = Promise.resolve(raw);
			if (reqBody) c.req.bodyCache.json = Promise.resolve(reqBody);
		} catch {}
		await next();
		const ms = Date.now() - t0;
		const status = c.res.status;
		log$3.info(`${route} → ${status} (${ms}ms)`);
		if (status >= 400) {
			const logExtra = {
				status,
				...pickReqFields(reqBody)
			};
			try {
				const respText = await c.res.text();
				logExtra.responseBody = truncate(respText, MAX_BODY_LOG);
				c.res = new Response(respText, {
					status: c.res.status,
					headers: c.res.headers
				});
			} catch {}
			log$3.warn(`${route} error`, logExtra);
		}
	};
}
//#endregion
//#region src/middleware/error-handler.ts
const log$2 = createLogger("error-handler");
function errorHandler(err, c) {
	const msg = err instanceof Error ? err.message : String(err);
	log$2.error(`unhandled error: ${msg}`);
	return c.json(wrapError(500, msg), 500);
}
//#endregion
//#region src/clickhouse-telemetry.ts
const log$1 = createLogger("clickhouse-telemetry");
const REQUEST_BODY_MAX_BYTES = 512;
const MAX_BUFFER_ROWS = 1e4;
const RETAINED_BUFFER_ROWS = 5e3;
const SENSITIVE_KEY_PATTERN = /(?:authorization|api[_-]?key|access[_-]?token|password|secret|token|credential)/i;
function toClickHouseTimestamp(date) {
	return new Date(date.getTime() + 480 * 60 * 1e3).toISOString().replace("T", " ").replace("Z", "");
}
function header(headers, name) {
	return headers.get(name)?.trim() ?? "";
}
function parseTurnSeq(value) {
	const parsed = Number.parseInt(value, 10);
	return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}
function redact(value) {
	if (Array.isArray(value)) return value.map(redact);
	if (!value || typeof value !== "object") return value;
	const output = {};
	for (const [key, child] of Object.entries(value)) output[key] = SENSITIVE_KEY_PATTERN.test(key) ? "[REDACTED]" : redact(child);
	return output;
}
function truncateUtf8(value, maxBytes) {
	const bytes = Buffer.from(value, "utf8");
	if (bytes.length <= maxBytes) return value;
	return bytes.subarray(0, maxBytes).toString("utf8").replace(/\uFFFD$/u, "");
}
function sanitizeToolCallBody(rawBody) {
	if (!rawBody) return {
		body: "",
		toolName: ""
	};
	try {
		const parsed = JSON.parse(rawBody);
		const rawToolName = typeof parsed.tool_name === "string" ? parsed.tool_name : parsed.name;
		const toolName = typeof rawToolName === "string" ? rawToolName.slice(0, 128) : "";
		return {
			body: truncateUtf8(JSON.stringify(redact(parsed)), REQUEST_BODY_MAX_BYTES),
			toolName
		};
	} catch {
		return {
			body: "[UNPARSEABLE_BODY]",
			toolName: ""
		};
	}
}
function buildKnowledgeToolCallRow(input) {
	const { body, toolName } = sanitizeToolCallBody(input.rawBody);
	return {
		timestamp: toClickHouseTimestamp(input.timestamp ?? /* @__PURE__ */ new Date()),
		session_key: header(input.headers, "x-conversation-id"),
		turn_seq: parseTurnSeq(header(input.headers, "x-tdai-turn-seq")),
		space_id: header(input.headers, "x-tdai-space-id"),
		user_id: header(input.headers, "x-tdai-user-id"),
		team_id: header(input.headers, "x-tdai-team-id"),
		agent_id: header(input.headers, "x-tdai-agent-id"),
		agent_source: header(input.headers, "x-tdai-agent-source") || "unknown",
		kind: "bridge_call",
		bridge_source: "knowledge-service",
		initiated_tool: toolName,
		executed_endpoint: toolName ? `tools/call/${toolName}` : "tools/call",
		request_body: body,
		request_body_hash: body ? createHash("sha256").update(body).digest("hex").slice(0, 16) : "",
		upstream_status: input.status,
		elapsed_ms: Math.max(0, Math.round(input.elapsedMs)),
		source_tag: "knowledge",
		host: process.env.HOSTNAME ?? ""
	};
}
function quoteIdentifier(identifier) {
	return `\`${identifier}\``;
}
function qualifiedTable(config) {
	return `${quoteIdentifier(config.database)}.${quoteIdentifier(config.table)}`;
}
function tableDdl(config) {
	const ttl = config.ttlDays > 0 ? `TTL toDateTime(timestamp) + INTERVAL ${config.ttlDays} DAY` : "";
	return [
		`CREATE TABLE IF NOT EXISTS ${qualifiedTable(config)} (`,
		"  timestamp DateTime64(3, 'Asia/Shanghai'),",
		"  session_key String,",
		"  turn_seq UInt32 DEFAULT 0,",
		"  space_id String,",
		"  user_id String,",
		"  team_id String DEFAULT '',",
		"  agent_id String DEFAULT '',",
		"  agent_source LowCardinality(String),",
		"  kind LowCardinality(String),",
		"  bridge_source LowCardinality(String) DEFAULT '',",
		"  initiated_tool String,",
		"  executed_endpoint String,",
		"  request_body String CODEC(ZSTD(3)),",
		"  request_body_hash FixedString(16) DEFAULT '',",
		"  upstream_status UInt16 DEFAULT 0,",
		"  elapsed_ms UInt32 DEFAULT 0,",
		"  source_tag LowCardinality(String) DEFAULT 'proxy',",
		"  host LowCardinality(String)",
		") ENGINE = MergeTree()",
		"ORDER BY (space_id, session_key, timestamp)",
		ttl
	].filter(Boolean).join("\n");
}
var DisabledKnowledgeTelemetry = class {
	async initialize() {}
	recordToolCall() {}
	async shutdown() {}
};
var ClickHouseKnowledgeTelemetry = class {
	buffer = [];
	timer = null;
	flushing = null;
	constructor(config, fetchImpl = fetch) {
		this.config = config;
		this.fetchImpl = fetchImpl;
	}
	async initialize() {
		try {
			await this.executeQuery(tableDdl(this.config));
			log$1.info("ClickHouse knowledge telemetry initialized", {
				database: this.config.database,
				table: this.config.table
			});
		} catch (err) {
			log$1.warn("ClickHouse telemetry table initialization failed; requests remain available", { error: err instanceof Error ? err.message : String(err) });
		}
		this.timer = setInterval(() => void this.flush(), this.config.flushIntervalMs);
		this.timer.unref?.();
	}
	recordToolCall(input) {
		try {
			this.buffer.push(buildKnowledgeToolCallRow(input));
			this.trimBuffer();
			if (this.buffer.length >= this.config.flushThreshold) this.flush();
		} catch {}
	}
	async shutdown() {
		if (this.timer) clearInterval(this.timer);
		this.timer = null;
		await this.flush();
		if (this.buffer.length > 0) await this.flush();
	}
	async flush() {
		if (this.flushing) return this.flushing;
		if (this.buffer.length === 0) return;
		const rows = this.buffer.splice(0);
		this.flushing = this.insertRows(rows).catch((err) => {
			this.buffer.unshift(...rows);
			this.trimBuffer();
			log$1.warn("ClickHouse telemetry flush failed", {
				rows: rows.length,
				error: err instanceof Error ? err.message : String(err)
			});
		}).finally(() => {
			this.flushing = null;
		});
		return this.flushing;
	}
	trimBuffer() {
		if (this.buffer.length <= MAX_BUFFER_ROWS) return;
		const dropped = this.buffer.length - RETAINED_BUFFER_ROWS;
		this.buffer = this.buffer.slice(-RETAINED_BUFFER_ROWS);
		log$1.warn("ClickHouse telemetry buffer overflow", { dropped });
	}
	async insertRows(rows) {
		const payload = rows.map((row) => JSON.stringify(row)).join("\n");
		await this.executeQuery(`INSERT INTO ${qualifiedTable(this.config)} FORMAT JSONEachRow`, payload);
	}
	async executeQuery(query, data = "") {
		const url = new URL(this.config.url);
		url.searchParams.set("query", query);
		const headers = { "content-type": "text/plain; charset=utf-8" };
		if (this.config.user) headers["x-clickhouse-user"] = this.config.user;
		if (this.config.password) headers["x-clickhouse-key"] = this.config.password;
		const response = await this.fetchImpl(url, {
			method: "POST",
			headers,
			body: data,
			signal: AbortSignal.timeout(this.config.requestTimeoutMs)
		});
		if (!response.ok) {
			const detail = truncateUtf8(await response.text(), 256);
			throw new Error(`ClickHouse HTTP ${response.status}: ${detail}`);
		}
	}
};
function createKnowledgeTelemetry(config, fetchImpl) {
	return config.enabled ? new ClickHouseKnowledgeTelemetry(config, fetchImpl) : new DisabledKnowledgeTelemetry();
}
function createKnowledgeTelemetryMiddleware(telemetry) {
	return async (c, next) => {
		const startedAt = performance.now();
		const bodyCache = c.req.bodyCache;
		let bodyPromise;
		if (bodyCache.text !== void 0) bodyPromise = Promise.resolve(bodyCache.text).catch(() => "");
		else if (bodyCache.json !== void 0) bodyPromise = Promise.resolve(bodyCache.json).then((body) => JSON.stringify(body)).catch(() => "");
		else try {
			bodyPromise = c.req.raw.clone().text().catch(() => "");
		} catch {
			bodyPromise = Promise.resolve("");
		}
		await next();
		telemetry.recordToolCall({
			headers: c.req.raw.headers,
			rawBody: await bodyPromise,
			status: c.res.status,
			elapsedMs: performance.now() - startedAt
		});
	};
}
//#endregion
//#region src/server.ts
/**
* Hono HTTP server entry point.
*
* Mounts all routes under /v3 prefix (applied once here, not per-route).
* Health check at /health (no prefix).
* Swagger UI at /docs.
*/
initTelemetry();
const log = createLogger("server");
function createApp() {
	const config = loadConfig();
	const knowledgeTelemetry = createKnowledgeTelemetry(config.clickhouse);
	const { db } = createDb({ path: config.dbPath });
	const knowledgeModule = createKnowledgeModule({
		dataDir: config.dataDir,
		db,
		llmConfig: config.llm,
		tmcCallbackUrl: config.tmcCallbackUrl
	});
	const app = new Hono();
	app.use("*", accessLog());
	app.onError(errorHandler);
	app.route("/", createHealthRoutes());
	const api = new Hono();
	api.use("/tools/call", createKnowledgeTelemetryMiddleware(knowledgeTelemetry));
	api.route("/wiki", createWikiRoutes({
		wikiService: knowledgeModule.wikiService,
		wikiMgr: knowledgeModule.wikiMgr,
		publicBaseUrl: config.publicBaseUrl
	}));
	api.route("/code-graph", createCodeGraphRoutes({
		cgService: knowledgeModule.cgService,
		instancePool: knowledgeModule.instancePool,
		publicBaseUrl: config.publicBaseUrl
	}));
	api.route("/tools", createToolsRoutes({
		wikiService: knowledgeModule.wikiService,
		wikiMgr: knowledgeModule.wikiMgr,
		cgService: knowledgeModule.cgService,
		instancePool: knowledgeModule.instancePool
	}));
	api.route("/internal/llm-binding", createLlmBindingRoutes({ llmBindingStore: knowledgeModule.llmBindingStore }));
	api.route("/", createAutoSyncRoutes({
		scheduler: knowledgeModule.autoSyncScheduler,
		config: knowledgeModule.autoSyncConfig
	}));
	app.route(config.apiPrefix, api);
	const openapiPath = join(dirname(fileURLToPath(import.meta.url)), "..", "openapi.yaml");
	try {
		const openapiContent = readFileSync(openapiPath, "utf-8");
		app.get("/openapi.json", (c) => {
			return c.body(openapiContent, 200, { "Content-Type": "application/yaml" });
		});
		app.use("/docs", swaggerUI({ url: "/openapi.json" }));
		log.info("Swagger UI mounted at /docs");
	} catch {
		log.warn("OpenAPI spec not found at openapi.yaml, skipping Swagger UI");
	}
	return {
		app,
		config,
		knowledgeModule,
		knowledgeTelemetry
	};
}
async function startServer() {
	const { app, config, knowledgeTelemetry } = createApp();
	await knowledgeTelemetry.initialize();
	log.info(`Starting knowledge service on port ${config.port}`);
	log.info(`Data dir: ${config.dataDir}`);
	log.info(`DB path: ${config.dbPath}`);
	log.info(`API prefix: ${config.apiPrefix}`);
	log.info(`ClickHouse telemetry: ${config.clickhouse.enabled ? "enabled" : "disabled"}`);
	const server = serve({
		fetch: app.fetch,
		port: config.port
	}, (info) => {
		log.info(`Knowledge service listening on http://localhost:${info.port}`);
	});
	let shuttingDown = false;
	const shutdown = async (signal) => {
		if (shuttingDown) return;
		shuttingDown = true;
		log.info(`Received ${signal}, shutting down`);
		await knowledgeTelemetry.shutdown();
		server.close(() => process.exit(0));
	};
	process.once("SIGTERM", () => void shutdown("SIGTERM"));
	process.once("SIGINT", () => void shutdown("SIGINT"));
}
if (import.meta.url === `file://${process.argv[1]}`) startServer().catch((err) => {
	log.error("Knowledge service failed to start", { error: err instanceof Error ? err.message : String(err) });
	process.exitCode = 1;
});
//#endregion
export { createApp };
