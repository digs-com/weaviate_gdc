export const levels = ["error", "warn", "info", "debug"] as const;

export type LogLevels = typeof levels;
export type LogLevel = LogLevels[number];
