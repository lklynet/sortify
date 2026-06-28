import honker from "@russellthehippo/honker-node";
import { createLogger } from "./logger.js";

const log = createLogger("scheduler");

let hdb: honker.Database | null = null;
let scansQueue: honker.Queue | null = null;
let cyclesQueue: honker.Queue | null = null;

export const schedulePresets: Record<string, { label: string; cron: string }> = {
  "every-12-hours": { label: "Every 12 hours", cron: "0 */12 * * *" },
  daily: { label: "Daily", cron: "0 0 * * *" },
  "every-3-days": { label: "Every 3 days", cron: "0 0 */3 * *" },
  "twice-weekly": { label: "Twice weekly", cron: "0 0 * * 0,3" },
  weekly: { label: "Weekly", cron: "0 0 * * 0" }
};

export const defaultSchedulePreset = "weekly";

export function bootstrap(dbPath: string, cron?: string): honker.Database {
  hdb = honker.open(dbPath, null, "polling");
  scansQueue = hdb.queue("scans", { visibilityTimeoutS: 7200, maxAttempts: 1 });
  cyclesQueue = hdb.queue("cycles", { visibilityTimeoutS: 7200, maxAttempts: 1 });
  const schedule = cron ?? schedulePresets[defaultSchedulePreset].cron;
  hdb.scheduler().add({
    name: "weekly-cycle",
    queue: "cycles",
    schedule,
    payload: {}
  });
  log.info("scheduler initialized", { dbPath, schedule });
  return hdb;
}

export function rescheduleCycle(cron: string) {
  if (!hdb) return;
  hdb.scheduler().remove("weekly-cycle");
  hdb.scheduler().add({
    name: "weekly-cycle",
    queue: "cycles",
    schedule: cron,
    payload: {}
  });
  log.info("cycle schedule updated", { cron });
}

export function enqueueScan(connection: {
  baseUrl: string;
  username: string;
  password: string;
}): number {
  return scansQueue!.enqueue(connection);
}

export function claimScheduledCycles(): honker.Job[] {
  return cyclesQueue!.claimBatch("primary", 1);
}

export function tryLockCycle(owner: string, ttlS = 3600): honker.Lock | null {
  return hdb!.tryLock("worker-cycle", owner, ttlS);
}

export function schedulerTick(): honker.ScheduledFire[] {
  return hdb!.scheduler().tick(Math.floor(Date.now() / 1000));
}
