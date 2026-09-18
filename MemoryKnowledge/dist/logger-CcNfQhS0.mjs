//#region src/logger.ts
const LEVEL_PRIORITY = {
	debug: 0,
	info: 1,
	warn: 2,
	error: 3
};
const LOG_LEVEL = process.env.LOG_LEVEL || "debug";
function ts() {
	return (/* @__PURE__ */ new Date()).toISOString().replace("T", " ").slice(0, 23);
}
function shouldLog(level) {
	return LEVEL_PRIORITY[level] >= LEVEL_PRIORITY[LOG_LEVEL];
}
function format(level, tag, msg, data) {
	const prefix = `${ts()} [${level.toUpperCase().padEnd(5)}] [${tag}]`;
	if (data !== void 0) return `${prefix} ${msg} ${JSON.stringify(data, null, 0)}`;
	return `${prefix} ${msg}`;
}
function createLogger(tag) {
	return {
		debug(msg, data) {
			if (shouldLog("debug")) console.log(format("debug", tag, msg, data));
		},
		info(msg, data) {
			if (shouldLog("info")) console.log(format("info", tag, msg, data));
		},
		warn(msg, data) {
			if (shouldLog("warn")) console.warn(format("warn", tag, msg, data));
		},
		error(msg, data) {
			if (shouldLog("error")) console.error(format("error", tag, msg, data));
		}
	};
}
createLogger("app");
//#endregion
export { createLogger as t };
