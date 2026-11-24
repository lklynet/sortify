import { createLogger } from "./logger.js";
import { defaultSettings } from "./config.js";
import type { SubsonicConnection } from "./subsonic-client.js";
import type { AppSettings, OperationLogEntry, ScanProgress, WorkerState } from "./types.js";

export const log = createLogger("server");
export const workerTickMs = 60_000;

export const appState = {
  settings: { ...defaultSettings },
  operationLogs: [] as OperationLogEntry[],
  scanJobs: new Map<string, ScanProgress>(),
  activeScanJobId: null as string | null,
  workerConnection: null as SubsonicConnection | null,
  previousRecommendedPlaylistNamesBySlug: new Map<string, string>(),
  workerState: {
    running: false,
    weeklyPlaylistCount: 3,
    lastRunAt: null,
    lastRunWeek: null,
    nextRunAt: null,
    error: null
  } as WorkerState
};
