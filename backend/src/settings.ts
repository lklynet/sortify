import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID, scryptSync } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { dbFile, port, settingsKeyFile, encryptedSettingPrefix } from "./config.js";
import { appState, log } from "./state.js";
import { db, readAllSettingsStmt, upsertSettingStmt } from "./db.js";
import { schedulePresets, rescheduleCycle, defaultSchedulePreset } from "./scheduler.js";
import type { AppSettings, OperationLogEntry } from "./types.js";
export function smtpConfigFromSettings() {
  return {
    host: appState.settings.smtpHost,
    port: appState.settings.smtpPort,
    secure: appState.settings.smtpSecure,
    user: appState.settings.smtpUser,
    password: appState.settings.smtpPassword,
    from: appState.settings.smtpFrom,
    to: appState.settings.smtpTo,
    tlsInsecure: appState.settings.smtpTlsInsecure
  };
}

export function appendLog(entry: Omit<OperationLogEntry, "id" | "timestamp">) {
  const logEntry: OperationLogEntry = {
    id: randomUUID(),
    timestamp: new Date().toISOString(),
    ...entry
  };
  appState.operationLogs.push(logEntry);
  if (appState.operationLogs.length > 800) {
    appState.operationLogs.shift();
  }
}

export function getSettingsEncryptionKey() {
  const configuredKey = process.env.SORTIFY_SETTINGS_KEY?.trim();
  if (configuredKey) {
    return scryptSync(configuredKey, "sortify-settings", 32);
  }
  fs.mkdirSync(path.dirname(settingsKeyFile), { recursive: true });
  if (fs.existsSync(settingsKeyFile)) {
    const storedKey = Buffer.from(fs.readFileSync(settingsKeyFile, "utf8").trim(), "base64");
    if (storedKey.length !== 32) {
      throw new Error(`Invalid appState.settings key file: ${settingsKeyFile}`);
    }
    return storedKey;
  }
  const generatedKey = randomBytes(32);
  fs.writeFileSync(settingsKeyFile, generatedKey.toString("base64"), { mode: 0o600 });
  fs.chmodSync(settingsKeyFile, 0o600);
  return generatedKey;
}

const settingsEncryptionKey = getSettingsEncryptionKey();

export function encryptSettingValue(value: string) {
  if (!value) {
    return "";
  }
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", settingsEncryptionKey, iv);
  const encrypted = Buffer.concat([cipher.update(value, "utf8"), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return [
    encryptedSettingPrefix,
    iv.toString("base64"),
    authTag.toString("base64"),
    encrypted.toString("base64")
  ].join(":");
}

export function decryptSettingValue(value: string) {
  if (!value) {
    return { value: "", needsMigration: false };
  }
  if (!value.startsWith(`${encryptedSettingPrefix}:`)) {
    return { value, needsMigration: true };
  }
  const [prefix, ivBase64, authTagBase64, encryptedBase64] = value.split(":");
  if (!prefix || !ivBase64 || !authTagBase64 || !encryptedBase64) {
    throw new Error("Invalid encrypted setting payload");
  }
  const decipher = createDecipheriv(
    "aes-256-gcm",
    settingsEncryptionKey,
    Buffer.from(ivBase64, "base64")
  );
  decipher.setAuthTag(Buffer.from(authTagBase64, "base64"));
  const decrypted = Buffer.concat([
    decipher.update(Buffer.from(encryptedBase64, "base64")),
    decipher.final()
  ]);
  return { value: decrypted.toString("utf8"), needsMigration: false };
}



export function loadPersistedSettings() {
  const rows = readAllSettingsStmt.all() as Array<{ key: string; value: string }>;
  const persisted = new Map(rows.map((row) => [row.key, row.value]));
  const read = (key: string) => persisted.get(key);
  let secretsNeedMigration = false;
  appState.settings.navidromeUrl = read("navidromeUrl") ?? appState.settings.navidromeUrl;
  appState.settings.navidromeUsername = read("navidromeUsername") ?? appState.settings.navidromeUsername;
  const persistedNavidromePassword = read("navidromePassword");
  if (persistedNavidromePassword !== undefined) {
    const decrypted = decryptSettingValue(persistedNavidromePassword);
    appState.settings.navidromePassword = decrypted.value;
    secretsNeedMigration = secretsNeedMigration || decrypted.needsMigration;
  }
  const persistedLastFmApiKey = read("lastFmApiKey");
  if (persistedLastFmApiKey !== undefined) {
    const decrypted = decryptSettingValue(persistedLastFmApiKey);
    appState.settings.lastFmApiKey = decrypted.value;
    secretsNeedMigration = secretsNeedMigration || decrypted.needsMigration;
  }
  const weekly = Number.parseInt(read("weeklyPlaylistCount") ?? "", 10);
  if (!Number.isNaN(weekly)) {
    appState.settings.weeklyPlaylistCount = clamp(weekly, 0, 8);
  }
  const maxTracks = Number.parseInt(read("maxTracksPerPlaylist") ?? "", 10);
  if (!Number.isNaN(maxTracks)) {
    appState.settings.maxTracksPerPlaylist = clamp(maxTracks, 5, 100);
  }
  const savedSchedule = read("cycleSchedule");
  if (savedSchedule && schedulePresets[savedSchedule]) {
    appState.settings.cycleSchedule = savedSchedule;
  }
  const persistedSmtpEnabled = read("smtpEnabled");
  if (persistedSmtpEnabled !== undefined) {
    appState.settings.smtpEnabled = persistedSmtpEnabled === "true";
  }
  appState.settings.smtpHost = (read("smtpHost") ?? "").trim() || appState.settings.smtpHost;
  const persistedSmtpPort = Number.parseInt(read("smtpPort") ?? "", 10);
  if (!Number.isNaN(persistedSmtpPort)) {
    appState.settings.smtpPort = clamp(persistedSmtpPort, 1, 65535);
  }
  const persistedSmtpSecure = read("smtpSecure");
  if (persistedSmtpSecure !== undefined) {
    appState.settings.smtpSecure = persistedSmtpSecure === "true";
  }
  appState.settings.smtpUser = (read("smtpUser") ?? "").trim() || appState.settings.smtpUser;
  const persistedSmtpPassword = read("smtpPassword");
  if (persistedSmtpPassword !== undefined) {
    const decrypted = decryptSettingValue(persistedSmtpPassword);
    appState.settings.smtpPassword = decrypted.value;
    secretsNeedMigration = secretsNeedMigration || decrypted.needsMigration;
  }
  appState.settings.smtpFrom = (read("smtpFrom") ?? "").trim() || appState.settings.smtpFrom;
  appState.settings.smtpTo = (read("smtpTo") ?? "").trim() || appState.settings.smtpTo;
  const persistedSmtpTlsInsecure = read("smtpTlsInsecure");
  if (persistedSmtpTlsInsecure !== undefined) {
    appState.settings.smtpTlsInsecure = persistedSmtpTlsInsecure === "true";
  }
  const workerRunning = read("workerRunning");
  if (workerRunning !== undefined) {
    appState.workerState.running = workerRunning === "true";
  }
  appState.workerState.lastRunAt = read("workerLastRunAt") || null;
  appState.workerState.lastRunWeek = read("workerLastRunWeek") || null;
  appState.workerState.error = read("workerError") || null;
  appState.workerState.weeklyPlaylistCount = appState.settings.weeklyPlaylistCount;
  if (secretsNeedMigration) {
    persistSettings();
  }
}

export function persistSettings() {
  const transaction = db.transaction(() => {
    upsertSettingStmt.run("navidromeUrl", appState.settings.navidromeUrl);
    upsertSettingStmt.run("navidromeUsername", appState.settings.navidromeUsername);
    upsertSettingStmt.run("navidromePassword", encryptSettingValue(appState.settings.navidromePassword));
    upsertSettingStmt.run("lastFmApiKey", encryptSettingValue(appState.settings.lastFmApiKey));
    upsertSettingStmt.run("weeklyPlaylistCount", String(appState.settings.weeklyPlaylistCount));
    upsertSettingStmt.run("maxTracksPerPlaylist", String(appState.settings.maxTracksPerPlaylist));
    upsertSettingStmt.run("cycleSchedule", appState.settings.cycleSchedule);
    upsertSettingStmt.run("smtpEnabled", String(appState.settings.smtpEnabled));
    upsertSettingStmt.run("smtpHost", appState.settings.smtpHost);
    upsertSettingStmt.run("smtpPort", String(appState.settings.smtpPort));
    upsertSettingStmt.run("smtpSecure", String(appState.settings.smtpSecure));
    upsertSettingStmt.run("smtpUser", appState.settings.smtpUser);
    upsertSettingStmt.run("smtpPassword", encryptSettingValue(appState.settings.smtpPassword));
    upsertSettingStmt.run("smtpFrom", appState.settings.smtpFrom);
    upsertSettingStmt.run("smtpTo", appState.settings.smtpTo);
    upsertSettingStmt.run("smtpTlsInsecure", String(appState.settings.smtpTlsInsecure));
    upsertSettingStmt.run("workerRunning", String(appState.workerState.running));
    upsertSettingStmt.run("workerLastRunAt", appState.workerState.lastRunAt ?? "");
    upsertSettingStmt.run("workerLastRunWeek", appState.workerState.lastRunWeek ?? "");
    upsertSettingStmt.run("workerError", appState.workerState.error ?? "");
  });
  transaction();
}

loadPersistedSettings();
rescheduleCycle(schedulePresets[appState.settings.cycleSchedule]?.cron ?? schedulePresets[defaultSchedulePreset].cron);
log.info("settings loaded", { navidromeConfigured: Boolean(appState.settings.navidromeUrl && appState.settings.navidromeUsername && appState.settings.navidromePassword), workerRunning: appState.workerState.running, schedule: appState.settings.cycleSchedule });

export function syncWorkerConnectionFromSettings() {
  if (appState.settings.navidromeUrl && appState.settings.navidromeUsername && appState.settings.navidromePassword) {
    appState.workerConnection = {
      baseUrl: appState.settings.navidromeUrl,
      username: appState.settings.navidromeUsername,
      password: appState.settings.navidromePassword
    };
    return;
  }
  appState.workerConnection = null;
}

syncWorkerConnectionFromSettings();



export function settingsSnapshot() {
  return {
    navidromeUrl: appState.settings.navidromeUrl,
    navidromeUsername: appState.settings.navidromeUsername,
    navidromePassword: "",
    hasNavidromePassword: Boolean(appState.settings.navidromePassword),
    lastFmApiKey: "",
    hasLastFmApiKey: Boolean(appState.settings.lastFmApiKey),
    weeklyPlaylistCount: appState.settings.weeklyPlaylistCount,
    maxTracksPerPlaylist: appState.settings.maxTracksPerPlaylist,
    cycleSchedule: appState.settings.cycleSchedule,
    smtpEnabled: appState.settings.smtpEnabled,
    smtpHost: appState.settings.smtpHost,
    smtpPort: appState.settings.smtpPort,
    smtpSecure: appState.settings.smtpSecure,
    smtpUser: appState.settings.smtpUser,
    smtpPassword: "",
    hasSmtpPassword: Boolean(appState.settings.smtpPassword),
    smtpFrom: appState.settings.smtpFrom,
    smtpTo: appState.settings.smtpTo,
    smtpTlsInsecure: appState.settings.smtpTlsInsecure
  };
}



export function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}