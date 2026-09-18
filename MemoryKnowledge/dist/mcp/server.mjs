import { t as createLogger } from "../logger-CcNfQhS0.mjs";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
//#region src/mcp/tools.ts
const MCP_TOOLS = [
	{
		name: "code_search",
		description: "Quick symbol search by name in a code graph. Returns locations only (no code); use code_explore to get source.",
		inputSchema: {
			type: "object",
			properties: {
				code_graph_id: {
					type: "string",
					description: "The code graph ID (cg-...)"
				},
				query: {
					type: "string",
					description: "Symbol name or partial name (e.g. \"auth\", \"signIn\", \"UserService\")"
				},
				kind: {
					type: "string",
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
					description: "Optional node-kind filter. Omit to search all kinds (do NOT pass \"any\"/\"symbol\"/\"file\" — not valid, yields zero results)."
				},
				limit: {
					type: "integer",
					minimum: 1,
					maximum: 100,
					description: "Max results (default: 10)"
				}
			},
			required: ["code_graph_id", "query"]
		},
		endpoint: "/code-graph/search"
	},
	{
		name: "code_explore",
		description: "Explore files in a code graph matching a query.",
		inputSchema: {
			type: "object",
			properties: {
				code_graph_id: {
					type: "string",
					description: "The code graph ID (cg-...)"
				},
				query: {
					type: "string",
					description: "Search query"
				},
				maxFiles: {
					type: "integer",
					minimum: 1,
					maximum: 200,
					description: "Max files to return (default: 12)"
				}
			},
			required: ["code_graph_id", "query"]
		},
		endpoint: "/code-graph/explore"
	},
	{
		name: "code_callers",
		description: "Find all callers of a symbol in a code graph.",
		inputSchema: {
			type: "object",
			properties: {
				code_graph_id: {
					type: "string",
					description: "The code graph ID (cg-...)"
				},
				symbol: {
					type: "string",
					description: "Symbol name to find callers for"
				},
				limit: {
					type: "integer",
					minimum: 1,
					maximum: 200,
					description: "Max results (default: 20)"
				}
			},
			required: ["code_graph_id", "symbol"]
		},
		endpoint: "/code-graph/callers"
	},
	{
		name: "code_callees",
		description: "Find all callees (functions called by) a symbol in a code graph.",
		inputSchema: {
			type: "object",
			properties: {
				code_graph_id: {
					type: "string",
					description: "The code graph ID (cg-...)"
				},
				symbol: {
					type: "string",
					description: "Symbol name to find callees for"
				},
				limit: {
					type: "integer",
					minimum: 1,
					maximum: 200,
					description: "Max results (default: 20)"
				}
			},
			required: ["code_graph_id", "symbol"]
		},
		endpoint: "/code-graph/callees"
	},
	{
		name: "code_impact",
		description: "Analyze the impact of changing a symbol (dependency chain).",
		inputSchema: {
			type: "object",
			properties: {
				code_graph_id: {
					type: "string",
					description: "The code graph ID (cg-...)"
				},
				symbol: {
					type: "string",
					description: "Symbol name to analyze impact for"
				},
				depth: {
					type: "integer",
					minimum: 1,
					maximum: 10,
					description: "Analysis depth (default: 2)"
				}
			},
			required: ["code_graph_id", "symbol"]
		},
		endpoint: "/code-graph/impact"
	},
	{
		name: "code_node",
		description: "Get detailed information about a specific symbol node in a code graph.",
		inputSchema: {
			type: "object",
			properties: {
				code_graph_id: {
					type: "string",
					description: "The code graph ID (cg-...)"
				},
				symbol: {
					type: "string",
					description: "Symbol name"
				},
				includeCode: {
					type: "boolean",
					description: "Include source code (default: false)"
				},
				file: {
					type: "string",
					description: "File path to disambiguate"
				},
				line: {
					type: "integer",
					minimum: 1,
					description: "Line number to disambiguate"
				}
			},
			required: ["code_graph_id", "symbol"]
		},
		endpoint: "/code-graph/node"
	},
	{
		name: "code_status",
		description: "Get the indexing status of a code graph.",
		inputSchema: {
			type: "object",
			properties: { code_graph_id: {
				type: "string",
				description: "The code graph ID (cg-...)"
			} },
			required: ["code_graph_id"]
		},
		endpoint: "/code-graph/status"
	},
	{
		name: "code_files",
		description: "List files in a code graph, optionally filtered by path or pattern.",
		inputSchema: {
			type: "object",
			properties: {
				code_graph_id: {
					type: "string",
					description: "The code graph ID (cg-...)"
				},
				path: {
					type: "string",
					description: "Path prefix filter"
				},
				pattern: {
					type: "string",
					description: "Glob pattern filter"
				},
				format: {
					type: "string",
					enum: ["tree", "flat"],
					description: "Output format (default: tree)"
				},
				includeMetadata: {
					type: "boolean",
					description: "Include file metadata (default: true)"
				},
				maxDepth: {
					type: "integer",
					minimum: 1,
					description: "Max tree depth"
				}
			},
			required: ["code_graph_id"]
		},
		endpoint: "/code-graph/files"
	},
	{
		name: "wiki_search",
		description: "Search wiki pages by keyword (BM25 full-text search). Optional graph multi-hop expansion (PRD: hop, decay, minScore) walks [[wikilink]] edges from BM25 seeds to surface graph-related pages whose body doesn't match the query directly. Each result also carries `related` (neighbour pages) and the response includes `links` (edges between results) for relationship visualisation.",
		inputSchema: {
			type: "object",
			properties: {
				wiki_id: {
					type: "string",
					description: "The wiki ID (wiki-...)"
				},
				query: {
					type: "string",
					description: "Search query"
				},
				limit: {
					type: "integer",
					description: "Max results (default: 20)"
				},
				hop: {
					type: "integer",
					minimum: 0,
					maximum: 5,
					description: "Graph expansion depth. 0 = pure BM25 (default), >0 = walk wikilink edges from seeds."
				},
				decay: {
					type: "number",
					minimum: 0,
					maximum: 1,
					description: "Per-hop score decay factor when hop>0 (default 0.5)."
				},
				minScore: {
					type: "number",
					minimum: 0,
					description: "Minimum score threshold; nodes below this are dropped (default 0.1)."
				}
			},
			required: ["wiki_id", "query"]
		},
		endpoint: "/wiki/search"
	},
	{
		name: "wiki_read",
		description: "Read wiki page content by reference (page id or path).",
		inputSchema: {
			type: "object",
			properties: {
				wiki_id: {
					type: "string",
					description: "The wiki ID (wiki-...)"
				},
				refs: {
					type: "array",
					items: { type: "string" },
					description: "Page references (ids or relative paths, without .md)"
				}
			},
			required: ["wiki_id", "refs"]
		},
		endpoint: "/wiki/page/read"
	},
	{
		name: "wiki_list",
		description: "List all wiki pages with metadata (title, type, path).",
		inputSchema: {
			type: "object",
			properties: { wiki_id: {
				type: "string",
				description: "The wiki ID (wiki-...)"
			} },
			required: ["wiki_id"]
		},
		endpoint: "/wiki/page/ls"
	},
	{
		name: "wiki_graph",
		description: "Get the wiki knowledge graph (nodes, edges, communities).",
		inputSchema: {
			type: "object",
			properties: { wiki_id: {
				type: "string",
				description: "The wiki ID (wiki-...)"
			} },
			required: ["wiki_id"]
		},
		endpoint: "/wiki/graph"
	}
];
//#endregion
//#region src/mcp/http-client.ts
/**
* HTTP client — forwards MCP tool calls to the Hono knowledge API.
*
* Each MCP tool maps to a POST endpoint on the knowledge service.
* The client sends the tool arguments as JSON body and returns the
* ApiResponseEnvelope data field (or error).
*/
const log$1 = createLogger("mcp-http");
/**
* Call a knowledge API endpoint.
* @param endpoint Path without /v3 prefix (e.g. "/wiki/search", "/code-graph/search")
* @param body Request body
* @returns The ApiResponseEnvelope data field on success, or throws on error.
*/
async function callApi(opts, endpoint, body) {
	const url = `${opts.baseUrl.replace(/\/$/, "")}/v3${endpoint}`;
	const headers = { "Content-Type": "application/json" };
	if (opts.token) headers["Authorization"] = `Bearer ${opts.token}`;
	log$1.debug(`POST ${url}`);
	let resp;
	try {
		resp = await fetch(url, {
			method: "POST",
			headers,
			body: JSON.stringify(body)
		});
	} catch (err) {
		const msg = err instanceof Error ? err.message : String(err);
		log$1.error(`fetch failed for ${endpoint}: ${msg}`);
		throw err;
	}
	let json;
	try {
		json = await resp.json();
	} catch (err) {
		const msg = err instanceof Error ? err.message : String(err);
		log$1.error(`JSON parse failed for ${endpoint} (status=${resp.status}): ${msg}`);
		throw new Error(`API error: ${resp.status} (invalid JSON)`);
	}
	if (resp.status >= 400 || json.code !== 0) {
		log$1.warn(`API error on ${endpoint}: status=${resp.status} code=${json.code} message="${json.message}"`);
		throw new Error(json.message || `API error: ${resp.status}`);
	}
	return json.data;
}
//#endregion
//#region src/mcp/server.ts
/**
* MCP stdio server — exposes knowledge query tools to LLM agents.
*
* Runs as a separate process with stdio transport. When an agent calls a tool,
* the server forwards the request to the Hono HTTP API via callApi().
*
* Usage:
*   KNOWLEDGE_API_URL=http://localhost:8421 node dist/mcp/server.js
*
* The agent connects via stdio; the server translates tool calls to HTTP
* requests against the knowledge service.
*/
const log = createLogger("mcp-server");
function createMcpServer(httpOpts) {
	const toolMap = /* @__PURE__ */ new Map();
	for (const tool of MCP_TOOLS) toolMap.set(tool.name, tool);
	const server = new Server({
		name: "knowledge-mcp",
		version: "0.1.0"
	}, { capabilities: { tools: {} } });
	server.setRequestHandler(ListToolsRequestSchema, async () => {
		return { tools: MCP_TOOLS.map((t) => ({
			name: t.name,
			description: t.description,
			inputSchema: t.inputSchema
		})) };
	});
	server.setRequestHandler(CallToolRequestSchema, async (request) => {
		const { name, arguments: args } = request.params;
		const tool = toolMap.get(name);
		if (!tool) {
			log.warn(`Unknown tool requested: "${name}"`);
			return {
				content: [{
					type: "text",
					text: `Unknown tool: ${name}`
				}],
				isError: true
			};
		}
		const body = args ?? {};
		try {
			const data = await callApi(httpOpts, tool.endpoint, body);
			if (data && typeof data === "object" && "text" in data && "isError" in data) {
				const result = data;
				return {
					content: [{
						type: "text",
						text: result.text || "(empty result)"
					}],
					isError: result.isError
				};
			}
			return {
				content: [{
					type: "text",
					text: JSON.stringify(data, null, 2)
				}],
				isError: false
			};
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			log.error(`tool ${name} failed: ${msg}`);
			return {
				content: [{
					type: "text",
					text: `Error: ${msg}`
				}],
				isError: true
			};
		}
	});
	return server;
}
if (import.meta.url === `file://${process.argv[1]}`) {
	const baseUrl = process.env.KNOWLEDGE_API_URL || "http://localhost:8421";
	const token = process.env.KNOWLEDGE_API_TOKEN;
	log.info(`MCP server starting, API URL: ${baseUrl}`);
	const server = createMcpServer({
		baseUrl,
		token
	});
	const transport = new StdioServerTransport();
	server.connect(transport).then(() => {
		log.info("MCP server connected via stdio");
	}).catch((err) => {
		log.error(`Failed to connect: ${err instanceof Error ? err.message : String(err)}`);
		process.exit(1);
	});
}
//#endregion
export { createMcpServer };
