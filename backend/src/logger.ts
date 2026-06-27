type LogLevel = "debug" | "info" | "warn" | "error";

const levelPriority: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3
};

let minLevel: LogLevel = "info";

export function setLogLevel(level: LogLevel) {
  minLevel = level;
}

function formatLevel(level: LogLevel): string {
  return level.toUpperCase().padEnd(5);
}

function timestamp(): string {
  return new Date().toISOString();
}

export function createLogger(name: string) {
  function log(level: LogLevel, message: string, meta?: unknown) {
    if (levelPriority[level] < levelPriority[minLevel]) return;
    const parts = [`[${timestamp()}]`, formatLevel(level), `[${name}]`, message];
    if (meta !== undefined) {
      parts.push(JSON.stringify(meta));
    }
    const line = parts.join(" ");
    if (level === "error") console.error(line);
    else if (level === "warn") console.warn(line);
    else console.log(line);
  }

  return {
    debug: (message: string, meta?: unknown) => log("debug", message, meta),
    info: (message: string, meta?: unknown) => log("info", message, meta),
    warn: (message: string, meta?: unknown) => log("warn", message, meta),
    error: (message: string, meta?: unknown) => log("error", message, meta)
  };
}
