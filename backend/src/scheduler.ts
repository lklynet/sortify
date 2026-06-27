import honker from "@russellthehippo/honker-node";
import { createLogger } from "./logger.js";

const log = createLogger("scheduler");

let hdb: honker.Database | null = null;
let scansQueue: honker.Queue | null = null;
let cyclesQueue: honker.Queue | null = null;

export function bootstrap(dbPath: string): honker.Database {
  hdb = honker.open(dbPath, null, "polling");
  scansQueue = hdb.queue("scans", { visibilityTimeoutS: 7200, maxAttempts: 1 });
  cyclesQueue = hdb.queue("cycles", { visibilityTimeoutS: 7200, maxAttempts: 1 });
  hdb.scheduler().add({
    name: "weekly-cycle",
    queue: "cycles",
    schedule: "0 0 * * 0",
    payload: {}
  });
  log.info("scheduler initialized", { dbPath, schedule: "weekly sunday midnight" });
  return hdb;
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
