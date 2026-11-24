import express from "express";
import crypto from "node:crypto";
import { appState, log } from "../state.js";
import { dbFile } from "../config.js";
import { persistSettings, appendLog, settingsSnapshot, smtpConfigFromSettings, syncWorkerConnectionFromSettings, clamp } from "../settings.js";
import { sendSmtpEmail, isSmtpConfigured, type SmtpConfig } from "../email.js";
import { schedulePresets, rescheduleCycle } from "../scheduler.js";
import { stopWorkerScheduler, setWorkerScheduleHints } from "../worker.js";
import type { OperationLogEntry, AppSettings } from "../types.js";

const router = express.Router();

router.get("/api/settings", (_req, res) => {
  res.json(settingsSnapshot());
});

router.patch("/api/settings", (req, res) => {
  const body = (req.body ?? {}) as Record<string, unknown>;
  if (body.navidromeUrl !== undefined) {
    appState.settings.navidromeUrl = String(body.navidromeUrl ?? "").trim().replace(/\/+$/, "");
  }
  if (body.navidromeUsername !== undefined) {
    appState.settings.navidromeUsername = String(body.navidromeUsername ?? "").trim();
  }
  if (body.navidromePassword !== undefined) {
    appState.settings.navidromePassword = String(body.navidromePassword ?? "").trim();
  }
  if (body.lastFmApiKey !== undefined) {
    appState.settings.lastFmApiKey = String(body.lastFmApiKey ?? "").trim();
  }
  if (body.weeklyPlaylistCount !== undefined) {
    appState.settings.weeklyPlaylistCount = clamp(Number(body.weeklyPlaylistCount), 0, 8);
    appState.workerState.weeklyPlaylistCount = appState.settings.weeklyPlaylistCount;
  }
  if (body.maxTracksPerPlaylist !== undefined) {
    appState.settings.maxTracksPerPlaylist = clamp(Number(body.maxTracksPerPlaylist), 5, 100);
  }
  if (body.cycleSchedule !== undefined) {
    const preset = String(body.cycleSchedule).trim();
    if (schedulePresets[preset]) {
      appState.settings.cycleSchedule = preset;
      rescheduleCycle(schedulePresets[preset].cron);
    }
  }
  if (body.smtpEnabled !== undefined) {
    appState.settings.smtpEnabled = Boolean(body.smtpEnabled);
  }
  if (body.smtpHost !== undefined) {
    appState.settings.smtpHost = String(body.smtpHost ?? "").trim();
  }
  if (body.smtpPort !== undefined) {
    const port = Number(body.smtpPort);
    if (Number.isFinite(port)) {
      appState.settings.smtpPort = clamp(Math.round(port), 1, 65535);
    }
  }
  if (body.smtpSecure !== undefined) {
    appState.settings.smtpSecure = Boolean(body.smtpSecure);
  }
  if (body.smtpUser !== undefined) {
    appState.settings.smtpUser = String(body.smtpUser ?? "").trim();
  }
  if (body.smtpPassword !== undefined) {
    appState.settings.smtpPassword = String(body.smtpPassword ?? "").trim();
  }
  if (body.smtpFrom !== undefined) {
    appState.settings.smtpFrom = String(body.smtpFrom ?? "").trim();
  }
  if (body.smtpTo !== undefined) {
    appState.settings.smtpTo = String(body.smtpTo ?? "").trim();
  }
  if (body.smtpTlsInsecure !== undefined) {
    appState.settings.smtpTlsInsecure = Boolean(body.smtpTlsInsecure);
  }
  syncWorkerConnectionFromSettings();
  if (appState.workerState.running && !appState.workerConnection) {
    stopWorkerScheduler();
    appState.workerState.error = "Worker stopped because Navidrome settings are incomplete";
  } else if (appState.workerState.running) {
    setWorkerScheduleHints();
    appState.workerState.error = null;
  }
  persistSettings();
  appendLog({
    level: "info",
    scope: "system",
    message: "Settings updated",
    meta: { weeklyPlaylistCount: appState.settings.weeklyPlaylistCount }
  });
  res.json(settingsSnapshot());
});

router.post("/api/settings/smtp/test", async (req, res) => {
  const body = (req.body ?? {}) as Record<string, unknown>;
  const base = smtpConfigFromSettings();
  const trimmedString = (value: unknown): string => (typeof value === "string" ? value.trim() : "");
  const portValue = Number(body.smtpPort);
  const config: SmtpConfig = {
    host: trimmedString(body.smtpHost) || base.host,
    port: body.smtpPort !== undefined && body.smtpPort !== "" && Number.isFinite(portValue) ? clamp(Math.round(portValue), 1, 65535) : base.port,
    secure: typeof body.smtpSecure === "boolean" ? body.smtpSecure : base.secure,
    user: trimmedString(body.smtpUser) || base.user,
    password: trimmedString(body.smtpPassword) || base.password,
    from: trimmedString(body.smtpFrom) || base.from,
    to: trimmedString(body.smtpTo) || base.to,
    tlsInsecure: typeof body.smtpTlsInsecure === "boolean" ? body.smtpTlsInsecure : base.tlsInsecure
  };
  if (!isSmtpConfigured(config)) {
    res.status(400).json({ error: "SMTP is not fully configured. Host, user, and recipient are required." });
    return;
  }
  const dateLabel = new Date().toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" });
  const testHtml = `
<!DOCTYPE html>
<html>
<body style="margin:0;padding:24px;background:#f4f4f5;color:#18181b;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
  <div style="max-width:560px;margin:0 auto;">
    <h1 style="font-size:20px;margin:0 0 8px;">Sortify SMTP test</h1>
    <p style="margin:0 0 16px;color:#52525b;font-size:14px;">This is a test message sent from Sortify on ${dateLabel}. If you can read this, your SMTP appState.settings work.</p>
  </div>
</body>
</html>`;
  try {
    await sendSmtpEmail(config, "Sortify: SMTP test email", testHtml);
    appendLog({
      level: "info",
      scope: "system",
      message: "SMTP test email sent",
      meta: { to: config.to }
    });
    res.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : "SMTP send failed";
    appendLog({
      level: "warn",
      scope: "system",
      message: "SMTP test email failed",
      meta: { to: config.to, error: message }
    });
    res.status(502).json({ error: `Test email failed: ${message}` });
  }
});


export default router;
