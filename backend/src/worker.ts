import { schedulePresets, tryLockCycle, rescheduleCycle, enqueueScan, claimScheduledCycles, defaultSchedulePreset, schedulerTick } from "./scheduler.js";
import { randomUUID } from "node:crypto";
import { sendWeeklyDigestEmail, isSmtpConfigured } from "./email.js";
import { refreshRecommendedPlaylists, syncSubsonicPlaylists } from "./playlists.js";
import { syncWorkerConnectionFromSettings, appendLog, persistSettings, smtpConfigFromSettings } from "./settings.js";
import { hashSeed } from "./playlist-generator.js";
import { buildSubsonicStreamUrl, getSubsonicConnection, uploadNavidromePlaylistArtwork, navidromeLogin } from "./subsonic-client.js";
import { renderPlaylistArtwork, sanitizeFilename } from "./artwork-renderer.js";
import { deleteGeneratedArtworkFile, isoWeekKey, scanLibrary } from "./scan.js";
import { generatedArtworkDir, playlistArtworkRenderVersion } from "./config.js";
import { appState, log, workerTickMs } from "./state.js";
import type { PlaylistRecord } from "./types.js";

export function weeklyStartIso(now = new Date()): string {
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + (8 - day));
  date.setUTCHours(0, 0, 0, 0);
  return date.toISOString();
}

export function nextCycleStartIso(now = new Date()): string {
  const preset = schedulePresets[appState.settings.cycleSchedule];
  if (!preset) return weeklyStartIso();
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0, 0));
  if (appState.settings.cycleSchedule === "every-12-hours") {
    const cutoff = new Date(date);
    cutoff.setUTCHours(new Date(now.toUTCString()).getUTCHours() < 12 ? 12 : 24);
    if (cutoff <= now) cutoff.setUTCDate(cutoff.getUTCDate() + 1);
    return cutoff.toISOString();
  }
  if (appState.settings.cycleSchedule === "daily") {
    date.setUTCDate(date.getUTCDate() + 1);
    return date.toISOString();
  }
  if (appState.settings.cycleSchedule === "every-3-days") {
    const day = date.getUTCDate();
    const next = day + (3 - (day % 3 || 3));
    date.setUTCDate(next);
    return date.toISOString();
  }
  if (appState.settings.cycleSchedule === "twice-weekly") {
    const day = date.getUTCDay();
    const nextDay = day === 0 ? 3 : day < 3 ? 3 : 7;
    date.setUTCDate(date.getUTCDate() + (nextDay - day));
    return date.toISOString();
  }
  return weeklyStartIso();
}

export function setWorkerScheduleHints() {
  appState.workerState.nextRunAt = appState.workerState.running ? nextCycleStartIso() : null;
}

export async function runWorkerCycle(trigger: "start" | "weekly"): Promise<void> {
  if (!appState.workerConnection) {
    throw new Error("Worker is missing Navidrome connection settings");
  }
  appState.workerState.error = null;
  try {
    appendLog({
      level: "info",
      scope: "system",
      message: "Worker cycle started",
      meta: { trigger, weeklyPlaylistCount: appState.workerState.weeklyPlaylistCount }
    });
    await scanLibrary(appState.workerConnection);
    const { candidates, sourceTracks } = await refreshRecommendedPlaylists(appState.workerState.weeklyPlaylistCount, randomUUID());
    await syncSubsonicPlaylists(appState.workerConnection);
    appState.workerState.lastRunAt = new Date().toISOString();
    appState.workerState.lastRunWeek = isoWeekKey();
    setWorkerScheduleHints();
    persistSettings();
    const emailMeta: Record<string, unknown> = {};
    const emailConfig = smtpConfigFromSettings();
    if (appState.settings.smtpEnabled && isSmtpConfigured(emailConfig)) {
      const emailResult = await sendWeeklyDigestEmail(emailConfig, { candidates, sourceTracks });
      emailMeta.emailSent = emailResult.sent;
      if (emailResult.error) {
        emailMeta.emailError = emailResult.error;
        appendLog({
          level: "warn",
          scope: "system",
          message: "Weekly refresh email failed to send",
          meta: { error: emailResult.error }
        });
      }
    }
    appendLog({
      level: "info",
      scope: "system",
      message: "Worker cycle completed",
      meta: { trigger, lastRunAt: appState.workerState.lastRunAt, week: appState.workerState.lastRunWeek, ...emailMeta }
    });
  } catch (error) {
    appState.workerState.error = error instanceof Error ? error.message : "Worker cycle failed";
    persistSettings();
    appendLog({
      level: "error",
      scope: "system",
      message: "Worker cycle failed",
      meta: { trigger, error: appState.workerState.error }
    });
    throw error;
  }
}

export async function runScheduleLoop() {
  while (appState.workerState.running) {
    try {
      schedulerTick();
      const cycleJobs = claimScheduledCycles();
      for (const job of cycleJobs) {
        const lock = tryLockCycle("primary", 3600);
        if (!lock) {
          job.ack();
          continue;
        }
        try {
          await runWorkerCycle("weekly");
        } catch {
          // error already logged in runWorkerCycle
        }
        lock.release();
        job.ack();
      }
    } catch {
      // tick/claim failures are transient
    }
    await new Promise((resolve) => setTimeout(resolve, workerTickMs));
  }
}

export function startWorkerScheduler() {
  appState.workerState.running = true;
  setWorkerScheduleHints();
  persistSettings();
  void runScheduleLoop();
}

export function stopWorkerScheduler() {
  appState.workerState.running = false;
  appState.workerState.nextRunAt = null;
}

export function resumeWorkerSchedulerFromPersistence() {
  if (!appState.workerState.running) {
    setWorkerScheduleHints();
    return;
  }
  if (!appState.workerConnection) {
    appState.workerState.running = false;
    appState.workerState.error = "Worker could not resume because Navidrome settings are incomplete";
    setWorkerScheduleHints();
    persistSettings();
    appendLog({
      level: "warn",
      scope: "system",
      message: "Worker resume skipped",
      meta: { reason: "missing_connection_settings" }
    });
    return;
  }
  appState.workerState.error = null;
  setWorkerScheduleHints();
  startWorkerScheduler();
  appendLog({
    level: "info",
    scope: "system",
    message: "Worker resumed from persisted state",
    meta: { weeklyPlaylistCount: appState.workerState.weeklyPlaylistCount, source: appState.workerConnection.baseUrl }
  });
}

export function workerSnapshot() {
  return {
    running: appState.workerState.running,
    weeklyPlaylistCount: appState.workerState.weeklyPlaylistCount,
    lastRunAt: appState.workerState.lastRunAt,
    lastRunWeek: appState.workerState.lastRunWeek,
    nextRunAt: appState.workerState.nextRunAt,
    error: appState.workerState.error,
    connectionConfigured: Boolean(appState.workerConnection)
  };
}
