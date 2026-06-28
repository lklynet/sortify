import honker from "@russellthehippo/honker-node";
import { createLogger } from "./logger.js";

const log = createLogger("scheduler");

export const CYCLE_FREQUENCIES: Record<string, { label: string; cron: string[] }> = {
  "daily": { label: "Daily", cron: ["0 0 * * *"] },
  "every-2-days": { label: "Every 2 days", cron: ["0 0 */2 * *"] },
  "twice-weekly": { label: "Twice weekly", cron: ["0 0 * * 3", "0 0 * * 0"] },
  "weekly": { label: "Weekly (Sunday)", cron: ["0 0 * * 0"] },
  "monthly": { label: "Monthly (1st)", cron: ["0 0 1 * *"] }
};

let hdb: honker.Database | null = null;
let scansQueue: honker.Queue | null = null;
let cyclesQueue: honker.Queue | null = null;

export function bootstrap(dbPath: string, frequency = "weekly"): honker.Database {
  hdb = honker.open(dbPath, null, "polling");
  scansQueue = hdb.queue("scans", { visibilityTimeoutS: 7200, maxAttempts: 1 });
  cyclesQueue = hdb.queue("cycles", { visibilityTimeoutS: 7200, maxAttempts: 1 });
  configureSchedule(frequency);
  return hdb;
}

export function configureSchedule(frequency: string) {
  if (!hdb) return;
  const scheduler = hdb.scheduler();
  scheduler.remove("weekly-cycle");
  for (const config of Object.values(CYCLE_FREQUENCIES)) {
    for (const cron of config.cron) {
      scheduler.remove(`cycle-${cron.replace(/\s+/g, "-")}`);
    }
  }
  const config = CYCLE_FREQUENCIES[frequency] ?? CYCLE_FREQUENCIES["weekly"];
  for (const cron of config.cron) {
    scheduler.add({ name: `cycle-${cron.replace(/\s+/g, "-")}`, queue: "cycles", schedule: cron, payload: {} });
  }
  log.info("schedule configured", { frequency, crons: config.cron });
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
