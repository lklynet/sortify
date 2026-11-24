import { config } from "dotenv";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defaultSchedulePreset } from "./scheduler.js";
import type { AppSettings } from "./types.js";

const envCandidates = [path.resolve(process.cwd(), ".env"), path.resolve(process.cwd(), "../.env")];
for (const envFile of envCandidates) {
  if (fs.existsSync(envFile)) {
    config({ path: envFile });
    break;
  }
}

if (!process.env.FONTCONFIG_FILE || !process.env.FONTCONFIG_PATH) {
  const fontConfigCandidates = [
    "/opt/homebrew/etc/fonts/fonts.conf",
    "/usr/local/etc/fonts/fonts.conf",
    "/etc/fonts/fonts.conf"
  ];
  const fontConfigFile = fontConfigCandidates.find((candidate) => fs.existsSync(candidate));
  if (fontConfigFile) {
    process.env.FONTCONFIG_FILE ??= fontConfigFile;
    process.env.FONTCONFIG_PATH ??= path.dirname(fontConfigFile);
  }
}
export const port = Number(process.env.PORT ?? 3001);
export const dbFile = process.env.SORTIFY_DB_PATH ?? path.resolve(process.cwd(), "data", "sortify.db");
export const serverDir = path.dirname(fileURLToPath(import.meta.url));
export const frontendDistDir = [
  path.resolve(process.cwd(), "frontend", "dist"),
  path.resolve(process.cwd(), "../frontend", "dist"),
  path.resolve(serverDir, "../../frontend", "dist")
].find((candidate) => fs.existsSync(path.join(candidate, "index.html")));
export const defaultSubsonicUrl = process.env.SUBSONIC_URL ?? "";
export const defaultSubsonicUser = process.env.SUBSONIC_USER ?? "";
export const defaultSubsonicPassword = process.env.SUBSONIC_PASSWORD ?? "";
export const defaultLastFmApiKey = process.env.LASTFM_API_KEY ?? "";
export const defaultMaxTracksPerPlaylist = Math.max(5, Math.min(100, Number(process.env.MAX_TRACKS_PER_PLAYLIST ?? 20)));
export const defaultSmtpEnabled = process.env.SMTP_ENABLED === "true";
export const defaultSmtpHost = process.env.SMTP_HOST ?? "";
export const defaultSmtpPort = Math.max(1, Math.min(65535, Number(process.env.SMTP_PORT ?? 587)));
export const defaultSmtpSecure = process.env.SMTP_SECURE === "true";
export const defaultSmtpUser = process.env.SMTP_USER ?? "";
export const defaultSmtpPassword = process.env.SMTP_PASSWORD ?? "";
export const defaultSmtpFrom = process.env.SMTP_FROM ?? "";
export const defaultSmtpTo = process.env.SMTP_TO ?? "";
export const defaultSmtpTlsInsecure = process.env.SMTP_TLS_INSECURE === "true";
export const settingsKeyFile = process.env.SORTIFY_SETTINGS_KEY_FILE ?? path.resolve(path.dirname(dbFile), ".sortify-settings.key");
export const generatedArtworkDir = process.env.SORTIFY_ARTWORK_PATH ?? path.resolve(path.dirname(dbFile), "artwork");
export const playlistArtworkRenderVersion = "v18";
export const audioAnalysisScriptFile = path.resolve(serverDir, "../scripts/analyze_track.py");
export const audioAnalysisVersion = "audio-v2";
export const audioAnalysisEnabled = process.env.AUDIO_ANALYSIS_ENABLED !== "false";
export const audioAnalysisSampleSeconds = Math.max(30, Math.min(180, Number(process.env.AUDIO_ANALYSIS_SAMPLE_SECONDS ?? 90)));
export const audioAnalysisTimeoutMs = Math.max(10_000, Math.min(90_000, Number(process.env.AUDIO_ANALYSIS_TIMEOUT_MS ?? 25_000)));
export const encryptedSettingPrefix = "enc-v1";

export const defaultSettings: AppSettings = {
  navidromeUrl: defaultSubsonicUrl,
  navidromeUsername: defaultSubsonicUser,
  navidromePassword: defaultSubsonicPassword,
  lastFmApiKey: defaultLastFmApiKey,
  weeklyPlaylistCount: 3,
  maxTracksPerPlaylist: defaultMaxTracksPerPlaylist,
  cycleSchedule: defaultSchedulePreset,
  smtpEnabled: defaultSmtpEnabled,
  smtpHost: defaultSmtpHost,
  smtpPort: defaultSmtpPort,
  smtpSecure: defaultSmtpSecure,
  smtpUser: defaultSmtpUser,
  smtpPassword: defaultSmtpPassword,
  smtpFrom: defaultSmtpFrom,
  smtpTo: defaultSmtpTo,
  smtpTlsInsecure: defaultSmtpTlsInsecure
};
