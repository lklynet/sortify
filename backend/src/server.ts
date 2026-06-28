import cors from "cors";
import Database from "better-sqlite3";
import { config } from "dotenv";
import express from "express";
import { spawn } from "node:child_process";
import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID, scryptSync } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { deriveBaseTags, mergeRankedTagSources, normalizeTag, uniqueTags } from "./tags.js";
import { generateCandidates, hashSeed, seededRandom, limitTracksByArtist, tagsFromTrack, audioVectorFromTrack, maxArtistPerPlaylist, maxArtistFallbackPerPlaylist, type PlaylistCandidate } from "./playlist-generator.js";
import { buildSubsonicStreamUrl, getSubsonicConnection, subsonicRequest, navidromeLogin, uploadNavidromePlaylistArtwork, fetchSubsonicSongs, type SubsonicConnection } from "./subsonic-client.js";
import { renderPlaylistArtwork, isUsableArtworkImage, sanitizeFilename } from "./artwork-renderer.js";
import { fetchLastFmTags, fetchLastFmArtistImage } from "./lastfm.js";
import { fetchMusicBrainzTags } from "./musicbrainz.js";
import { bootstrap as bootstrapScheduler, enqueueScan, claimScheduledCycles, tryLockCycle, schedulerTick, configureSchedule, CYCLE_FREQUENCIES } from "./scheduler.js";
import { createLogger } from "./logger.js";

type TrackRecord = {
  id: number;
  path: string;
  title: string | null;
  artist: string | null;
  album: string | null;
  year: number | null;
  duration: number | null;
  tags_json: string;
  audio_features_json: string;
  audio_vector_json: string;
  analysis_version: string | null;
  analysis_updated_at: string | null;
  play_count: number;
  favorite: number;
};

type PlaylistRecord = {
  id: number;
  slug: string;
  name: string;
  description: string | null;
  mode: "dynamic" | "pinned" | "locked";
  selected: number;
  sync_target: "subsonic";
  is_recommended: number;
  updated_at: string;
  artwork_path: string;
  artwork_attribution: string;
  artwork_attribution_url: string;
  artwork_signature: string;
  artwork_updated_at: string;
};

type ScanProgress = {
  id: string;
  status: "queued" | "running" | "completed" | "failed";
  libraryPath: string;
  startedAt: string;
  finishedAt: string | null;
  scannedFiles: number;
  processedFiles: number;
  updatedTracks: number;
  errors: number;
  message: string | null;
};

type WorkerState = {
  running: boolean;
  weeklyPlaylistCount: number;
  lastRunAt: string | null;
  lastRunWeek: string | null;
  nextRunAt: string | null;
  error: string | null;
};

type AppSettings = {
  navidromeUrl: string;
  navidromeUsername: string;
  navidromePassword: string;
  lastFmApiKey: string;
  weeklyPlaylistCount: number;
  maxTracksPerPlaylist: number;
  cycleFrequency: string;
};

type OperationLogEntry = {
  id: string;
  timestamp: string;
  level: "info" | "warn" | "error";
  scope: "scan" | "playlist" | "system";
  message: string;
  meta?: Record<string, unknown>;
};

type AudioAnalysis = {
  version: string;
  durationSampled: number;
  bpm: number | null;
  energy: number;
  danceability: number;
  valence: number;
  acousticness: number;
  instrumentalness: number;
  brightness: number;
  rhythmicDensity: number;
  loudness: number;
  key?: string | null;
  keyConfidence?: number;
  camelotKey?: string | null;
  moodTags: string[];
};

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

const app = express();
const port = Number(process.env.PORT ?? 3001);
const dbFile = process.env.SORTIFY_DB_PATH ?? path.resolve(process.cwd(), "data", "sortify.db");
const serverDir = path.dirname(fileURLToPath(import.meta.url));
const frontendDistDir = [
  path.resolve(process.cwd(), "frontend", "dist"),
  path.resolve(process.cwd(), "../frontend", "dist"),
  path.resolve(serverDir, "../../frontend", "dist")
].find((candidate) => fs.existsSync(path.join(candidate, "index.html")));
const defaultSubsonicUrl = process.env.SUBSONIC_URL ?? "";
const defaultSubsonicUser = process.env.SUBSONIC_USER ?? "";
const defaultSubsonicPassword = process.env.SUBSONIC_PASSWORD ?? "";
const defaultLastFmApiKey = process.env.LASTFM_API_KEY ?? "";
const defaultMaxTracksPerPlaylist = Math.max(5, Math.min(100, Number(process.env.MAX_TRACKS_PER_PLAYLIST ?? 20)));
const settingsKeyFile = process.env.SORTIFY_SETTINGS_KEY_FILE ?? path.resolve(path.dirname(dbFile), ".sortify-settings.key");
const generatedArtworkDir = process.env.SORTIFY_ARTWORK_PATH ?? path.resolve(path.dirname(dbFile), "artwork");
const playlistArtworkRenderVersion = "v18";
const audioAnalysisScriptFile = path.resolve(serverDir, "../scripts/analyze_track.py");
const audioAnalysisVersion = "audio-v2";
const audioAnalysisEnabled = process.env.AUDIO_ANALYSIS_ENABLED !== "false";
const audioAnalysisSampleSeconds = Math.max(30, Math.min(180, Number(process.env.AUDIO_ANALYSIS_SAMPLE_SECONDS ?? 90)));
const audioAnalysisTimeoutMs = Math.max(10_000, Math.min(90_000, Number(process.env.AUDIO_ANALYSIS_TIMEOUT_MS ?? 25_000)));
const encryptedSettingPrefix = "enc-v1";
const log = createLogger("server");
const scanJobs = new Map<string, ScanProgress>();
const operationLogs: OperationLogEntry[] = [];
let activeScanJobId: string | null = null;
let workerConnection: SubsonicConnection | null = null;
let previousRecommendedPlaylistNamesBySlug = new Map<string, string>();
const workerTickMs = 60_000;
const workerState: WorkerState = {
  running: false,
  weeklyPlaylistCount: 3,
  lastRunAt: null,
  lastRunWeek: null,
  nextRunAt: null,
  error: null
};
const settings: AppSettings = {
  navidromeUrl: defaultSubsonicUrl,
  navidromeUsername: defaultSubsonicUser,
  navidromePassword: defaultSubsonicPassword,
  lastFmApiKey: defaultLastFmApiKey,
  weeklyPlaylistCount: 3,
  maxTracksPerPlaylist: defaultMaxTracksPerPlaylist,
  cycleFrequency: "weekly"
};

function appendLog(entry: Omit<OperationLogEntry, "id" | "timestamp">) {
  const logEntry: OperationLogEntry = {
    id: randomUUID(),
    timestamp: new Date().toISOString(),
    ...entry
  };
  operationLogs.push(logEntry);
  if (operationLogs.length > 800) {
    operationLogs.shift();
  }
}

function getSettingsEncryptionKey() {
  const configuredKey = process.env.SORTIFY_SETTINGS_KEY?.trim();
  if (configuredKey) {
    return scryptSync(configuredKey, "sortify-settings", 32);
  }
  fs.mkdirSync(path.dirname(settingsKeyFile), { recursive: true });
  if (fs.existsSync(settingsKeyFile)) {
    const storedKey = Buffer.from(fs.readFileSync(settingsKeyFile, "utf8").trim(), "base64");
    if (storedKey.length !== 32) {
      throw new Error(`Invalid settings key file: ${settingsKeyFile}`);
    }
    return storedKey;
  }
  const generatedKey = randomBytes(32);
  fs.writeFileSync(settingsKeyFile, generatedKey.toString("base64"), { mode: 0o600 });
  fs.chmodSync(settingsKeyFile, 0o600);
  return generatedKey;
}

const settingsEncryptionKey = getSettingsEncryptionKey();

function encryptSettingValue(value: string) {
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

function decryptSettingValue(value: string) {
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

fs.mkdirSync(path.dirname(dbFile), { recursive: true });
fs.mkdirSync(generatedArtworkDir, { recursive: true });
const db = new Database(dbFile);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(`
CREATE TABLE IF NOT EXISTS tracks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  title TEXT,
  artist TEXT,
  album TEXT,
  album_artist TEXT,
  year INTEGER,
  duration REAL,
  tags_json TEXT NOT NULL DEFAULT '[]',
  audio_features_json TEXT NOT NULL DEFAULT '{}',
  audio_vector_json TEXT NOT NULL DEFAULT '[]',
  analysis_version TEXT NOT NULL DEFAULT '',
  analysis_updated_at TEXT,
  play_count INTEGER NOT NULL DEFAULT 0,
  favorite INTEGER NOT NULL DEFAULT 0,
  last_scanned_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS playlists (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slug TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  description TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT '',
  mode TEXT NOT NULL DEFAULT 'live',
  selected INTEGER NOT NULL DEFAULT 0,
  sync_target TEXT NOT NULL DEFAULT 'subsonic',
  is_recommended INTEGER NOT NULL DEFAULT 1,
  artwork_path TEXT NOT NULL DEFAULT '',
  artwork_attribution TEXT NOT NULL DEFAULT '',
  artwork_attribution_url TEXT NOT NULL DEFAULT '',
  artwork_signature TEXT NOT NULL DEFAULT '',
  artwork_updated_at TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS playlist_tracks (
  playlist_id INTEGER NOT NULL,
  track_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  PRIMARY KEY (playlist_id, track_id),
  FOREIGN KEY (playlist_id) REFERENCES playlists(id) ON DELETE CASCADE,
  FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS app_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS playlist_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  playlist_id INTEGER NOT NULL,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  track_ids_json TEXT NOT NULL,
  snapshot_week TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (playlist_id) REFERENCES playlists(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_playlist_snapshots_playlist ON playlist_snapshots(playlist_id, snapshot_week);
`);

const trackColumns = db.prepare("PRAGMA table_info(tracks)").all() as Array<{ name: string }>;
const hasTrackColumn = (columnName: string) => trackColumns.some((column) => column.name === columnName);
if (!hasTrackColumn("audio_features_json")) {
  db.exec("ALTER TABLE tracks ADD COLUMN audio_features_json TEXT NOT NULL DEFAULT '{}'");
}
if (!hasTrackColumn("audio_vector_json")) {
  db.exec("ALTER TABLE tracks ADD COLUMN audio_vector_json TEXT NOT NULL DEFAULT '[]'");
}
if (!hasTrackColumn("analysis_version")) {
  db.exec("ALTER TABLE tracks ADD COLUMN analysis_version TEXT NOT NULL DEFAULT ''");
}
if (!hasTrackColumn("analysis_updated_at")) {
  db.exec("ALTER TABLE tracks ADD COLUMN analysis_updated_at TEXT");
}

const playlistColumns = db.prepare("PRAGMA table_info(playlists)").all() as Array<{ name: string }>;
const hasColumn = (columnName: string) => playlistColumns.some((column) => column.name === columnName);
if (!hasColumn("updated_at")) {
  db.exec("ALTER TABLE playlists ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''");
}
if (!hasColumn("mode")) {
  db.exec("ALTER TABLE playlists ADD COLUMN mode TEXT NOT NULL DEFAULT 'live'");
}
if (!hasColumn("is_recommended")) {
  db.exec("ALTER TABLE playlists ADD COLUMN is_recommended INTEGER NOT NULL DEFAULT 1");
}
if (!hasColumn("artwork_path")) {
  db.exec("ALTER TABLE playlists ADD COLUMN artwork_path TEXT NOT NULL DEFAULT ''");
}
if (!hasColumn("artwork_attribution")) {
  db.exec("ALTER TABLE playlists ADD COLUMN artwork_attribution TEXT NOT NULL DEFAULT ''");
}
if (!hasColumn("artwork_attribution_url")) {
  db.exec("ALTER TABLE playlists ADD COLUMN artwork_attribution_url TEXT NOT NULL DEFAULT ''");
}
if (!hasColumn("artwork_signature")) {
  db.exec("ALTER TABLE playlists ADD COLUMN artwork_signature TEXT NOT NULL DEFAULT ''");
}
if (!hasColumn("artwork_updated_at")) {
  db.exec("ALTER TABLE playlists ADD COLUMN artwork_updated_at TEXT NOT NULL DEFAULT ''");
}
db.exec("UPDATE playlists SET mode = 'locked' WHERE mode = 'frozen'");
db.exec("UPDATE playlists SET mode = 'dynamic' WHERE mode NOT IN ('dynamic', 'pinned', 'locked')");
db.exec("UPDATE playlists SET updated_at = CASE WHEN updated_at = '' THEN created_at ELSE updated_at END");
log.info("database initialized", { dbFile: dbFile });

app.use(cors());
app.use(express.json({ limit: "2mb" }));
app.use("/generated-artwork", express.static(generatedArtworkDir));

const readTracksStmt = db.prepare(`
SELECT id, path, title, artist, album, year, duration, tags_json, audio_features_json, audio_vector_json, analysis_version, analysis_updated_at
FROM tracks
ORDER BY last_scanned_at DESC, id DESC
LIMIT ?
`);

const upsertTrackStmt = db.prepare(`
INSERT INTO tracks (path, title, artist, album, album_artist, year, duration, tags_json, audio_features_json, audio_vector_json, analysis_version, analysis_updated_at, last_scanned_at)
VALUES (@path, @title, @artist, @album, @album_artist, @year, @duration, @tags_json, @audio_features_json, @audio_vector_json, @analysis_version, @analysis_updated_at, @last_scanned_at)
ON CONFLICT(path) DO UPDATE SET
  title = excluded.title,
  artist = excluded.artist,
  album = excluded.album,
  album_artist = excluded.album_artist,
  year = excluded.year,
  duration = excluded.duration,
  tags_json = excluded.tags_json,
  audio_features_json = excluded.audio_features_json,
  audio_vector_json = excluded.audio_vector_json,
  analysis_version = excluded.analysis_version,
  analysis_updated_at = excluded.analysis_updated_at,
  last_scanned_at = excluded.last_scanned_at
`);
const readTrackScanStateStmt = db.prepare(`
SELECT path, title, artist, album, year, duration, tags_json, audio_features_json, audio_vector_json, analysis_version, analysis_updated_at
FROM tracks
`);
const deleteTrackByPathStmt = db.prepare("DELETE FROM tracks WHERE path = ?");

const markPlaylistsNotRecommendedStmt = db.prepare("UPDATE playlists SET is_recommended = 0 WHERE mode = 'dynamic'");
const findPlaylistBySlugStmt = db.prepare(
  `SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at,
          artwork_path, artwork_attribution, artwork_attribution_url, artwork_signature, artwork_updated_at
   FROM playlists WHERE slug = ?`
);
const insertPlaylistStmt = db.prepare(`
INSERT INTO playlists (slug, name, description, created_at, updated_at, mode, selected, sync_target, is_recommended)
VALUES (@slug, @name, @description, @created_at, @updated_at, @mode, @selected, @sync_target, @is_recommended)
`);
const updatePlaylistMetadataStmt = db.prepare(`
UPDATE playlists
SET name = @name,
    description = @description,
    updated_at = @updated_at,
    is_recommended = @is_recommended
WHERE id = @id
`);
const refreshProtectedPlaylistStmt = db.prepare(`
UPDATE playlists
SET updated_at = @updated_at,
    is_recommended = 1
WHERE id = @id
`);
const updatePlaylistArtworkStmt = db.prepare(`
UPDATE playlists
SET artwork_path = @artwork_path,
    artwork_attribution = @artwork_attribution,
    artwork_attribution_url = @artwork_attribution_url,
    artwork_signature = @artwork_signature,
    artwork_updated_at = @artwork_updated_at
WHERE id = @id
`);
const deletePlaylistByIdStmt = db.prepare("DELETE FROM playlists WHERE id = ?");
const deletePlaylistTracksByPlaylistIdStmt = db.prepare("DELETE FROM playlist_tracks WHERE playlist_id = ?");
const insertPlaylistTrackStmt = db.prepare(`
INSERT INTO playlist_tracks (playlist_id, track_id, position)
VALUES (?, ?, ?)
`);
const insertPlaylistSnapshotStmt = db.prepare(`
INSERT INTO playlist_snapshots (playlist_id, slug, name, track_ids_json, snapshot_week, created_at)
VALUES (?, ?, ?, ?, ?, ?)
`);
const readPlaylistTrackIdsStmt = db.prepare(`
SELECT track_id FROM playlist_tracks WHERE playlist_id = ? ORDER BY position
`);
const readAllSettingsStmt = db.prepare("SELECT key, value FROM app_settings");
const upsertSettingStmt = db.prepare(`
INSERT INTO app_settings (key, value)
VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`);

function loadPersistedSettings() {
  const rows = readAllSettingsStmt.all() as Array<{ key: string; value: string }>;
  const persisted = new Map(rows.map((row) => [row.key, row.value]));
  const read = (key: string) => persisted.get(key);
  let secretsNeedMigration = false;
  settings.navidromeUrl = read("navidromeUrl") ?? settings.navidromeUrl;
  settings.navidromeUsername = read("navidromeUsername") ?? settings.navidromeUsername;
  const persistedNavidromePassword = read("navidromePassword");
  if (persistedNavidromePassword !== undefined) {
    const decrypted = decryptSettingValue(persistedNavidromePassword);
    settings.navidromePassword = decrypted.value;
    secretsNeedMigration = secretsNeedMigration || decrypted.needsMigration;
  }
  const persistedLastFmApiKey = read("lastFmApiKey");
  if (persistedLastFmApiKey !== undefined) {
    const decrypted = decryptSettingValue(persistedLastFmApiKey);
    settings.lastFmApiKey = decrypted.value;
    secretsNeedMigration = secretsNeedMigration || decrypted.needsMigration;
  }
  const weekly = Number.parseInt(read("weeklyPlaylistCount") ?? "", 10);
  if (!Number.isNaN(weekly)) {
    settings.weeklyPlaylistCount = clamp(weekly, 1, 8);
  }
  const maxTracks = Number.parseInt(read("maxTracksPerPlaylist") ?? "", 10);
  if (!Number.isNaN(maxTracks)) {
    settings.maxTracksPerPlaylist = clamp(maxTracks, 5, 100);
  }
  const persistedCycleFrequency = read("cycleFrequency");
  if (persistedCycleFrequency !== undefined && CYCLE_FREQUENCIES[persistedCycleFrequency]) {
    settings.cycleFrequency = persistedCycleFrequency;
  }
  const workerRunning = read("workerRunning");
  if (workerRunning !== undefined) {
    workerState.running = workerRunning === "true";
  }
  workerState.lastRunAt = read("workerLastRunAt") || null;
  workerState.lastRunWeek = read("workerLastRunWeek") || null;
  workerState.error = read("workerError") || null;
  workerState.weeklyPlaylistCount = settings.weeklyPlaylistCount;
  if (secretsNeedMigration) {
    persistSettings();
  }
}

function persistSettings() {
  const transaction = db.transaction(() => {
    upsertSettingStmt.run("navidromeUrl", settings.navidromeUrl);
    upsertSettingStmt.run("navidromeUsername", settings.navidromeUsername);
    upsertSettingStmt.run("navidromePassword", encryptSettingValue(settings.navidromePassword));
    upsertSettingStmt.run("lastFmApiKey", encryptSettingValue(settings.lastFmApiKey));
    upsertSettingStmt.run("weeklyPlaylistCount", String(settings.weeklyPlaylistCount));
    upsertSettingStmt.run("maxTracksPerPlaylist", String(settings.maxTracksPerPlaylist));
    upsertSettingStmt.run("cycleFrequency", settings.cycleFrequency);
    upsertSettingStmt.run("workerRunning", String(workerState.running));
    upsertSettingStmt.run("workerLastRunAt", workerState.lastRunAt ?? "");
    upsertSettingStmt.run("workerLastRunWeek", workerState.lastRunWeek ?? "");
    upsertSettingStmt.run("workerError", workerState.error ?? "");
  });
  transaction();
}

loadPersistedSettings();
log.info("settings loaded", { navidromeConfigured: Boolean(settings.navidromeUrl && settings.navidromeUsername && settings.navidromePassword), workerRunning: workerState.running });

function syncWorkerConnectionFromSettings() {
  if (settings.navidromeUrl && settings.navidromeUsername && settings.navidromePassword) {
    workerConnection = {
      baseUrl: settings.navidromeUrl,
      username: settings.navidromeUsername,
      password: settings.navidromePassword
    };
    return;
  }
  workerConnection = null;
}

syncWorkerConnectionFromSettings();

const lastFmArtistTagsCache = new Map<string, string[]>();
const lastFmAlbumTagsCache = new Map<string, string[]>();
const lastFmArtistImageCache = new Map<string, { imageUrl: string; artistUrl: string } | null>();
const musicBrainzArtistTagsCache = new Map<string, string[]>();

function isoWeekKey(now = new Date()): string {
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const weekNumber = Math.ceil(((date.getTime() - yearStart.getTime()) / 86400000 + 1) / 7);
  return `${date.getUTCFullYear()}-W${String(weekNumber).padStart(2, "0")}`;
}

function weeklyRotatedTrackIds(
  trackRows: Array<{ trackId: number; artist: string | null }>,
  seedKey: string,
  targetSize: number
): number[] {
  const random = seededRandom(hashSeed(seedKey));
  const ranked = [...trackRows]
    .map((row) => ({
      ...row,
      score: random()
    }))
    .sort((a, b) => a.score - b.score);
  const artistByTrackId = new Map<number, string>();
  for (const row of ranked) {
    artistByTrackId.set(row.trackId, row.artist ? normalizeTag(row.artist) : `track-${row.trackId}`);
  }
  return limitTracksByArtist(
    ranked.map((row) => row.trackId),
    artistByTrackId,
    maxArtistPerPlaylist,
    targetSize,
    maxArtistFallbackPerPlaylist
  );
}

function extractSubsonicSongId(pathValue: string): string | null {
  const value = (pathValue ?? "").trim();
  if (!value) {
    return null;
  }
  if (value.startsWith("subsonic:")) {
    const id = value.slice("subsonic:".length).trim();
    return id || null;
  }
  if (/^[a-zA-Z0-9._-]+$/.test(value)) {
    return value;
  }
  return null;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function hashText(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}


function deleteGeneratedArtworkFile(artworkPath: string | null | undefined) {
  if (!artworkPath) {
    return;
  }
  const filePath = path.join(generatedArtworkDir, path.basename(artworkPath));
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
}

function pruneGeneratedArtworkFiles() {
  const rows = db
    .prepare("SELECT artwork_path FROM playlists WHERE artwork_path != ''")
    .all() as Array<{ artwork_path: string }>;
  const activeFiles = new Set(rows.map((row) => path.basename(row.artwork_path)).filter(Boolean));
  for (const entry of fs.readdirSync(generatedArtworkDir)) {
    if (activeFiles.has(entry)) {
      continue;
    }
    const filePath = path.join(generatedArtworkDir, entry);
    if (fs.statSync(filePath).isFile()) {
      fs.unlinkSync(filePath);
    }
  }
}



function artistNameForArtistSignalPlaylist(playlist: PlaylistRecord, description: string, title: string): string | null {
  if (!playlist.slug.startsWith("artist-")) {
    return null;
  }
  const descriptionMatch = description.match(/sharing signature tags with\s+(.+)$/i);
  if (descriptionMatch?.[1]?.trim()) {
    return descriptionMatch[1].trim();
  }
  const titleMatch = title.match(/^(.*)\s+(Constellation|Orbit|Signal|Axis|Halo)$/i);
  if (titleMatch?.[1]?.trim()) {
    return titleMatch[1].trim();
  }
  return null;
}

function audioFeaturesFromTrack(track: Pick<TrackRecord, "audio_features_json">): AudioAnalysis | null {
  try {
    const parsed = JSON.parse(track.audio_features_json) as AudioAnalysis;
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function hasFreshAudioAnalysis(track: Pick<TrackRecord, "analysis_version" | "audio_features_json" | "audio_vector_json"> | undefined) {
  if (!track || track.analysis_version !== audioAnalysisVersion) {
    return false;
  }
  return Boolean(audioFeaturesFromTrack(track)?.moodTags?.length) && audioVectorFromTrack(track).length >= 8;
}

async function analyzeTrackAudio(connection: SubsonicConnection, songId: string): Promise<{ features: AudioAnalysis; vector: number[] } | null> {
  if (!audioAnalysisEnabled || !fs.existsSync(audioAnalysisScriptFile)) {
    return null;
  }
  const streamUrl = buildSubsonicStreamUrl(connection, songId);
  return await new Promise((resolve, reject) => {
    const child = spawn("python3", [audioAnalysisScriptFile, "--url", streamUrl, "--sample-seconds", String(audioAnalysisSampleSeconds)], {
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    const timeout = setTimeout(() => {
      child.kill("SIGTERM");
    }, audioAnalysisTimeoutMs);
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.on("close", (code, signal) => {
      clearTimeout(timeout);
      if (signal) {
        reject(new Error("Audio analysis timed out"));
        return;
      }
      if (code !== 0) {
        reject(new Error(stderr.trim() || `Audio analysis exited with code ${code}`));
        return;
      }
      try {
        const parsed = JSON.parse(stdout.trim()) as { features?: AudioAnalysis; vector?: number[] };
        const features = parsed.features;
        const vector = Array.isArray(parsed.vector) ? parsed.vector.map((value) => clamp(Number(value), 0, 1)) : [];
        if (!features || !Array.isArray(features.moodTags) || vector.length < 8) {
          reject(new Error("Audio analysis returned invalid payload"));
          return;
        }
        resolve({
          features: {
            ...features,
            version: audioAnalysisVersion,
            bpm: typeof features.bpm === "number" && Number.isFinite(features.bpm) ? features.bpm : null,
            energy: clamp(Number(features.energy), 0, 1),
            danceability: clamp(Number(features.danceability), 0, 1),
            valence: clamp(Number(features.valence), 0, 1),
            acousticness: clamp(Number(features.acousticness), 0, 1),
            instrumentalness: clamp(Number(features.instrumentalness), 0, 1),
            brightness: clamp(Number(features.brightness), 0, 1),
            rhythmicDensity: clamp(Number(features.rhythmicDensity), 0, 1),
            loudness: clamp(Number(features.loudness), 0, 1),
            durationSampled: Math.max(1, Number(features.durationSampled) || audioAnalysisSampleSeconds),
            moodTags: uniqueTags(features.moodTags.map((tag) => String(tag ?? "")))
          },
          vector
        });
      } catch (error) {
        reject(error instanceof Error ? error : new Error("Failed parsing audio analysis output"));
      }
    });
  });
}

function resolveConnection(input: unknown): SubsonicConnection {
  return getSubsonicConnection(input, {
    baseUrl: settings.navidromeUrl,
    username: settings.navidromeUsername,
    password: settings.navidromePassword
  });
}

async function scanLibrary(
  connection: SubsonicConnection,
  onProgress?: (progress: Partial<Pick<ScanProgress, "scannedFiles" | "processedFiles" | "updatedTracks" | "errors">>) => void,
  options?: { forceRefreshAnalysis?: boolean }
) {
  const forceRefreshAnalysis = Boolean(options?.forceRefreshAnalysis);
  const discovered = await fetchSubsonicSongs(connection);
  const discoveredPaths = new Set(discovered.map((song) => `subsonic:${song.id}`));
  const existingRows = readTrackScanStateStmt.all() as Array<{
    path: string;
    title: string | null;
    artist: string | null;
    album: string | null;
    year: number | null;
    duration: number | null;
    tags_json: string;
    audio_features_json: string;
    audio_vector_json: string;
    analysis_version: string | null;
    analysis_updated_at: string | null;
  }>;
  const existingByPath = new Map<string, (typeof existingRows)[number]>();
  for (const row of existingRows) {
    existingByPath.set(row.path, row);
  }
  const stalePaths = existingRows.map((row) => row.path).filter((trackPath) => !discoveredPaths.has(trackPath));
  const now = new Date().toISOString();
  let updated = 0;
  let processed = 0;
  let skipped = 0;
  let errors = 0;
  let pruned = 0;
  appendLog({
    level: "info",
    scope: "scan",
    message: "Scan started",
    meta: { source: connection.baseUrl, discoveredFiles: discovered.length }
  });
  onProgress?.({ scannedFiles: discovered.length, processedFiles: 0, updatedTracks: 0, errors: 0 });
  for (const song of discovered) {
    const title = song.title ?? "Unknown Title";
    const artist = song.artist ?? "Unknown Artist";
    const album = song.album ?? "Unknown Album";
    const albumArtist = song.albumArtist ?? artist;
    const year = song.year ?? null;
    const duration = song.duration ?? null;
    const trackPath = `subsonic:${song.id}`;
    const existing = existingByPath.get(trackPath);
    const existingTags = existing ? tagsFromTrack(existing) : [];
    const unchanged =
      existing &&
      (existing.title ?? "Unknown Title") === title &&
      (existing.artist ?? "Unknown Artist") === artist &&
      (existing.album ?? "Unknown Album") === album &&
      (existing.duration ?? null) === duration;
    const shouldHydrate = forceRefreshAnalysis || !unchanged || existingTags.length < 6 || !hasFreshAudioAnalysis(existing);
    if (!shouldHydrate) {
      processed += 1;
      skipped += 1;
      if (processed % 100 === 0 || processed === discovered.length) {
        onProgress?.({ scannedFiles: discovered.length, processedFiles: processed, updatedTracks: updated, errors });
      }
      continue;
    }
    try {
      const musicBrainz = await fetchMusicBrainzTags(artist, title, musicBrainzArtistTagsCache);
      const mergedYear = year ?? musicBrainz.year ?? null;
      const analyzedAudio = await analyzeTrackAudio(connection, song.id).catch((error) => {
        appendLog({
          level: "warn",
          scope: "scan",
          message: "Audio analysis skipped for track",
          meta: { songId: song.id, title: song.title, error: error instanceof Error ? error.message : "Unknown error" }
        });
        return null;
      });
      const preservedAudio = existing
        ? {
            features: audioFeaturesFromTrack(existing),
            vector: audioVectorFromTrack(existing)
          }
        : null;
      const audioFeatures = analyzedAudio?.features ?? preservedAudio?.features ?? null;
      const audioVector = analyzedAudio?.vector ?? preservedAudio?.vector ?? [];
      const rawGenres = song.genre ? song.genre.split(/[,;/|]+/).map((g) => g.trim()).filter(Boolean) : [];
      const baseTags = deriveBaseTags(title, artist, mergedYear, rawGenres, audioFeatures?.bpm ?? null);
      const lastFmTags = await fetchLastFmTags(settings.lastFmApiKey, artist, title, album, { artistTags: lastFmArtistTagsCache, albumTags: lastFmAlbumTagsCache });
      const audioTags = audioFeatures?.moodTags ?? [];
      const tags = mergeRankedTagSources([
        { source: "base", weight: 0.42, tags: baseTags },
        { source: "musicbrainz-recording", weight: 1, tags: musicBrainz.recordingTags },
        { source: "musicbrainz-artist", weight: 0.78, tags: musicBrainz.artistTags },
        { source: "lastfm-track", weight: 0.92, tags: lastFmTags.trackTags },
        { source: "lastfm-album", weight: 0.76, tags: lastFmTags.albumTags },
        { source: "lastfm-artist", weight: 0.68, tags: lastFmTags.artistTags },
        { source: "audio", weight: 0.88, tags: audioTags }
      ]);
      upsertTrackStmt.run({
        path: trackPath,
        title,
        artist,
        album,
        album_artist: albumArtist,
        year: mergedYear,
        duration,
        tags_json: JSON.stringify(tags),
        audio_features_json: JSON.stringify(audioFeatures ?? {}),
        audio_vector_json: JSON.stringify(audioVector),
        analysis_version: audioFeatures ? audioAnalysisVersion : existing?.analysis_version ?? "",
        analysis_updated_at: audioFeatures ? now : existing?.analysis_updated_at ?? null,
        last_scanned_at: now
      });
      updated += 1;
    } catch (error) {
      errors += 1;
      if (errors <= 30) {
        appendLog({
          level: "warn",
          scope: "scan",
          message: "Track scan failed",
          meta: { songId: song.id, title: song.title, error: error instanceof Error ? error.message : "Unknown error" }
        });
      }
    }
    processed += 1;
    if (processed % 25 === 0 || processed === discovered.length) {
      onProgress?.({ scannedFiles: discovered.length, processedFiles: processed, updatedTracks: updated, errors });
      appendLog({
        level: "info",
        scope: "scan",
        message: "Scan progress",
        meta: { processedFiles: processed, scannedFiles: discovered.length, updatedTracks: updated, skippedTracks: skipped, errors }
      });
    }
  }
  if (stalePaths.length) {
    const pruneTransaction = db.transaction((paths: string[]) => {
      for (const stalePath of paths) {
        deleteTrackByPathStmt.run(stalePath);
      }
    });
    pruneTransaction(stalePaths);
    pruned = stalePaths.length;
    appendLog({
      level: "info",
      scope: "scan",
      message: "Stale tracks pruned after scan",
      meta: { prunedTracks: pruned }
    });
  }
  appendLog({
    level: "info",
    scope: "scan",
    message: "Scan completed",
    meta: { source: connection.baseUrl, scannedFiles: discovered.length, processedFiles: processed, updatedTracks: updated, skippedTracks: skipped, errors, prunedTracks: pruned }
  });
  return { scannedFiles: discovered.length, updatedTracks: updated, processedFiles: processed, errors, prunedTracks: pruned };
}

function getManagedPlaylistNamesBySlug() {
  const rows = db
    .prepare("SELECT slug, name FROM playlists WHERE is_recommended = 1 OR mode IN ('pinned', 'locked')")
    .all() as Array<{ slug: string; name: string }>;
  return new Map(rows.map((row) => [row.slug, row.name]));
}

function replacePlaylists(candidates: PlaylistCandidate[]) {
  const now = new Date().toISOString();
  previousRecommendedPlaylistNamesBySlug = getManagedPlaylistNamesBySlug();
  const transaction = db.transaction((list: PlaylistCandidate[]) => {
    markPlaylistsNotRecommendedStmt.run();
    for (const playlist of list) {
      const existing = findPlaylistBySlugStmt.get(playlist.slug) as PlaylistRecord | undefined;
      let playlistId = 0;
      if (existing) {
        if (existing.mode === "locked") {
          continue;
        }
        playlistId = existing.id;
        if (existing.mode === "pinned") {
          refreshProtectedPlaylistStmt.run({
            id: existing.id,
            updated_at: now
          });
        } else {
          updatePlaylistMetadataStmt.run({
            id: existing.id,
            name: playlist.name,
            description: playlist.description,
            updated_at: now,
            is_recommended: 1
          });
        }
      } else {
        const result = insertPlaylistStmt.run({
          slug: playlist.slug,
          name: playlist.name,
          description: playlist.description,
          created_at: now,
          updated_at: now,
          mode: "dynamic",
          selected: 0,
          sync_target: "subsonic",
          is_recommended: 1
        });
        playlistId = Number(result.lastInsertRowid);
      }
      if (existing && existing.mode === "dynamic") {
        const existingTrackIds = (readPlaylistTrackIdsStmt.all(playlistId) as Array<{ track_id: number }>).map((row) => row.track_id);
        if (existingTrackIds.length > 0) {
          insertPlaylistSnapshotStmt.run(playlistId, existing.slug, existing.name, JSON.stringify(existingTrackIds), isoWeekKey(), now);
        }
      }
      deletePlaylistTracksByPlaylistIdStmt.run(playlistId);
      playlist.trackIds.forEach((trackId, position) => {
        insertPlaylistTrackStmt.run(playlistId, trackId, position + 1);
      });
    }
    const stalePlaylists = db
      .prepare("SELECT id, artwork_path FROM playlists WHERE is_recommended = 0 AND mode = 'dynamic' AND selected = 0")
      .all() as Array<{ id: number; artwork_path: string }>;
    for (const stale of stalePlaylists) {
      deleteGeneratedArtworkFile(stale.artwork_path);
      deletePlaylistByIdStmt.run(stale.id);
    }
  });
  transaction(candidates);
  pruneGeneratedArtworkFiles();
}

async function syncPlaylistArtwork(
  connection: SubsonicConnection,
  playlist: PlaylistRecord,
  remoteId: string,
  title: string,
  description: string,
  weeklyTrackIds: number[],
  trackRows: Array<{ trackId: number; path: string; artist: string | null }>,
  getNavidromeToken: () => Promise<string>
) {
  if (!remoteId || !weeklyTrackIds.length) {
    return;
  }
  const signature = hashText(`${playlistArtworkRenderVersion}|${playlist.slug}|${title}|${playlist.mode}|${weeklyTrackIds.join(",")}`);
  if (playlist.artwork_signature === signature && playlist.artwork_path) {
    const currentFile = path.join(generatedArtworkDir, path.basename(playlist.artwork_path));
    if (fs.existsSync(currentFile)) {
      return;
    }
  }
  const artists = [...new Set(trackRows.map((row) => row.artist?.trim()).filter((value): value is string => Boolean(value)))];
  const sourceArtist = artistNameForArtistSignalPlaylist(playlist, description, title);
  const lastFmArtistImage = sourceArtist ? await fetchLastFmArtistImage(settings.lastFmApiKey, sourceArtist, lastFmArtistImageCache, isUsableArtworkImage) : null;
  let imageUrl = "";
  let artworkAttribution = "";
  let artworkAttributionUrl = "";
  if (lastFmArtistImage?.imageUrl && (await isUsableArtworkImage(lastFmArtistImage.imageUrl))) {
    imageUrl = lastFmArtistImage.imageUrl;
    artworkAttribution = `Artist image for ${sourceArtist} via Last.fm`;
    artworkAttributionUrl = lastFmArtistImage.artistUrl || "https://www.last.fm/api/show/artist.getInfo";
  } else {
    imageUrl = "https://picsum.photos/1200";
  }
  const rendered = await renderPlaylistArtwork(title, imageUrl);
  const fileName = `${sanitizeFilename(playlist.slug)}-${signature.slice(0, 12)}.jpg`;
  const localPath = path.join(generatedArtworkDir, fileName);
  fs.writeFileSync(localPath, rendered);
  await uploadNavidromePlaylistArtwork(connection, remoteId, await getNavidromeToken(), rendered, fileName);
  updatePlaylistArtworkStmt.run({
    id: playlist.id,
    artwork_path: `/generated-artwork/${fileName}`,
    artwork_attribution: artworkAttribution,
    artwork_attribution_url: artworkAttributionUrl,
    artwork_signature: signature,
    artwork_updated_at: new Date().toISOString()
  });
  if (playlist.artwork_path && playlist.artwork_path !== `/generated-artwork/${fileName}`) {
    const previousFile = path.join(generatedArtworkDir, path.basename(playlist.artwork_path));
    if (fs.existsSync(previousFile)) {
      fs.unlinkSync(previousFile);
    }
  }
}

async function syncSubsonicPlaylists(connection: SubsonicConnection) {
  const managedPlaylists = db
    .prepare(
      `SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at,
              artwork_path, artwork_attribution, artwork_attribution_url, artwork_signature, artwork_updated_at
       FROM playlists
       WHERE is_recommended = 1 OR mode IN ('pinned', 'locked')
       ORDER BY CASE mode WHEN 'locked' THEN 0 WHEN 'pinned' THEN 1 ELSE 2 END, name`
    )
    .all() as PlaylistRecord[];
  const remoteList = await subsonicRequest<{ playlists?: { playlist?: Array<{ id: string; name: string }> } }>(
    connection,
    "getPlaylists.view"
  );
  const remoteByName = new Map<string, string>();
  for (const playlist of remoteList.playlists?.playlist ?? []) {
    remoteByName.set(playlist.name, playlist.id);
  }
  let applied = 0;
  let removed = 0;
  const weekKey = isoWeekKey();
  let navidromeTokenPromise: Promise<string> | null = null;
  const getNavidromeToken = () => {
    navidromeTokenPromise ??= navidromeLogin(connection);
    return navidromeTokenPromise;
  };
  const previousManagedNames = new Set(previousRecommendedPlaylistNamesBySlug.values());
  for (const playlist of managedPlaylists) {
    const trackRows = db
      .prepare(
        `SELECT pt.track_id as trackId, t.path, t.artist FROM playlist_tracks pt
         JOIN tracks t ON t.id = pt.track_id
         WHERE pt.playlist_id = ?
         ORDER BY pt.position`
      )
      .all(playlist.id) as Array<{ trackId: number; path: string; artist: string | null }>;
    const weeklyTrackIds =
      playlist.mode === "locked"
        ? trackRows.map((row) => row.trackId)
        : weeklyRotatedTrackIds(trackRows, `${playlist.slug}:${weekKey}`, clamp(settings.maxTracksPerPlaylist, 5, 100));
    const pathByTrackId = new Map<number, string>();
    for (const row of trackRows) {
      pathByTrackId.set(row.trackId, row.path);
    }
    const songIds = weeklyTrackIds
      .map((trackId) => pathByTrackId.get(trackId) ?? "")
      .map((pathValue) => extractSubsonicSongId(pathValue))
      .filter((value): value is string => Boolean(value));
    const targetName = playlist.name;
    const previousName = previousRecommendedPlaylistNamesBySlug.get(playlist.slug) ?? "";
    let remoteId = remoteByName.get(targetName) ?? "";
    if (!remoteId && previousName) {
      remoteId = remoteByName.get(previousName) ?? "";
      if (remoteId && previousName !== targetName) {
        await subsonicRequest(connection, "updatePlaylist.view", { playlistId: remoteId, name: targetName });
        remoteByName.delete(previousName);
        remoteByName.set(targetName, remoteId);
      }
    }
    const remoteAlreadyExisted = Boolean(remoteId);
    if (!remoteId) {
      await subsonicRequest(connection, "createPlaylist.view", {
        name: targetName
      });
      const refreshedRemoteList = await subsonicRequest<{ playlists?: { playlist?: Array<{ id: string; name: string }> } }>(
        connection,
        "getPlaylists.view"
      );
      for (const remotePlaylist of refreshedRemoteList.playlists?.playlist ?? []) {
        if (remotePlaylist.name === targetName) {
          remoteId = remotePlaylist.id;
          remoteByName.set(remotePlaylist.name, remotePlaylist.id);
          break;
        }
      }
    }
    if (!remoteId) {
      appendLog({
        level: "warn",
        scope: "playlist",
        message: "Playlist created but remote id not found",
        meta: { name: targetName }
      });
      continue;
    }
    if (playlist.mode === "locked" && remoteAlreadyExisted) {
      applied += 1;
      continue;
    }
    const detail = await subsonicRequest<{ playlist?: { entry?: Array<{ id: string }> } }>(connection, "getPlaylist.view", {
      id: remoteId
    });
    const entries = detail.playlist?.entry ?? [];
    if (entries.length) {
      await subsonicRequest(connection, "updatePlaylist.view", {
        playlistId: remoteId,
        songIndexToRemove: entries.map((_, index) => String(entries.length - 1 - index))
      });
    }
    if (!songIds.length) {
      appendLog({
        level: "warn",
        scope: "playlist",
        message: "No syncable song ids for playlist",
        meta: { name: targetName, sourceTracks: trackRows.length }
      });
    }
    for (const songId of songIds) {
      await subsonicRequest(connection, "updatePlaylist.view", { playlistId: remoteId, songIdToAdd: songId });
    }
    if (playlist.mode !== "locked") {
      try {
        await syncPlaylistArtwork(
          connection,
          playlist,
          remoteId,
          targetName,
          playlist.description ?? "",
          weeklyTrackIds,
          trackRows,
          getNavidromeToken
        );
      } catch (error) {
        appendLog({
          level: "warn",
          scope: "playlist",
          message: "Playlist artwork refresh failed",
          meta: {
            name: targetName,
            error: error instanceof Error ? error.message : "Unknown artwork error"
          }
        });
      }
    }
    applied += 1;
  }
  const managedNames = new Set(managedPlaylists.map((playlist) => playlist.name));
  const knownManagedNames = new Set([...managedNames, ...previousManagedNames]);
  for (const remote of remoteList.playlists?.playlist ?? []) {
    const isLegacyManagedPlaylist = remote.name.startsWith("Sortify - ");
    const isKnownManagedPlaylist = knownManagedNames.has(remote.name);
    if (!isLegacyManagedPlaylist && !isKnownManagedPlaylist) {
      continue;
    }
    if (managedNames.has(remote.name)) {
      continue;
    }
    await subsonicRequest(connection, "deletePlaylist.view", { id: remote.id });
    removed += 1;
  }
  previousRecommendedPlaylistNamesBySlug = new Map(managedPlaylists.map((playlist) => [playlist.slug, playlist.name]));
  return { applied, removed, total: managedPlaylists.length };
}

async function refreshRecommendedPlaylists(
  weeklyPlaylistCount = workerState.weeklyPlaylistCount,
  generationKey = isoWeekKey()
) {
  const pinnedSlugs = new Set(
    (
      db.prepare("SELECT slug FROM playlists WHERE mode = 'pinned'").all() as Array<{
        slug: string;
      }>
    ).map((row) => row.slug)
  );
  const lockedSlugs = new Set(
    (
      db.prepare("SELECT slug FROM playlists WHERE mode = 'locked'").all() as Array<{
        slug: string;
      }>
    ).map((row) => row.slug)
  );
  const tracks = db
    .prepare(
      "SELECT id, path, title, artist, album, year, duration, tags_json, audio_features_json, audio_vector_json, analysis_version, analysis_updated_at, play_count, favorite FROM tracks"
    )
    .all() as TrackRecord[];
  const syncableTracks = tracks.filter((track) => Boolean(extractSubsonicSongId(track.path)));
  const enrichedTracks = syncableTracks.map((track) => ({
    id: track.id,
    title: track.title,
    artist: track.artist,
    tags_json: track.tags_json,
    audio_features_json: track.audio_features_json,
    audio_vector_json: track.audio_vector_json,
    play_count: track.play_count,
    favorite: track.favorite,
    tags: tagsFromTrack(track)
  }));
  const leastPlayedRows = db.prepare("SELECT id, tags_json, audio_features_json, audio_vector_json, play_count FROM tracks ORDER BY play_count ASC, RANDOM() LIMIT 160").all() as Array<{ id: number; tags_json: string; audio_features_json: string; audio_vector_json: string; play_count: number }>;
  const favoriteRows = db.prepare("SELECT id, tags_json FROM tracks WHERE favorite = 1 ORDER BY play_count DESC LIMIT 30").all() as Array<{ id: number; tags_json: string }>;
  const maxTracksPerPlaylist = settings.maxTracksPerPlaylist;
  const candidates = await generateCandidates({
    tracks: enrichedTracks,
    desiredWeeklyPlaylists: weeklyPlaylistCount,
    maxTracksPerPlaylist,
    generationKey,
    pinnedSlugs,
    lockedSlugs,
    leastPlayedRows,
    favoriteRows
  });
  replacePlaylists(candidates);
  appendLog({
    level: "info",
    scope: "playlist",
    message: "Recommended playlists refreshed",
    meta: { generated: candidates.length, sourceTracks: syncableTracks.length, weeklyPlaylistCount, maxTracksPerPlaylist }
  });
  return { candidates, sourceTracks: syncableTracks.length };
}

function nextRunFromFrequency(now = new Date()): Date {
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  date.setUTCHours(0, 0, 0, 0);
  const freq = settings.cycleFrequency;
  if (freq === "daily") {
    date.setUTCDate(date.getUTCDate() + 1);
  } else if (freq === "every-2-days") {
    date.setUTCDate(date.getUTCDate() + 2);
  } else if (freq === "twice-weekly") {
    const day = date.getUTCDay();
    if (day < 3) date.setUTCDate(date.getUTCDate() + (3 - day));
    else if (day < 7) date.setUTCDate(date.getUTCDate() + (7 - day));
    else date.setUTCDate(date.getUTCDate() + 3);
  } else if (freq === "monthly") {
    date.setUTCMonth(date.getUTCMonth() + 1);
    date.setUTCDate(1);
  } else {
    const day = date.getUTCDay() || 7;
    date.setUTCDate(date.getUTCDate() + (8 - day));
  }
  return date;
}

function nextWeekStartIso(now = new Date()): string {
  return nextRunFromFrequency(now).toISOString();
}

function setWorkerScheduleHints() {
  workerState.nextRunAt = workerState.running ? nextWeekStartIso() : null;
}

async function runWorkerCycle(trigger: "start" | "weekly"): Promise<void> {
  if (!workerConnection) {
    throw new Error("Worker is missing Navidrome connection settings");
  }
  workerState.error = null;
  try {
    appendLog({
      level: "info",
      scope: "system",
      message: "Worker cycle started",
      meta: { trigger, weeklyPlaylistCount: workerState.weeklyPlaylistCount }
    });
    await scanLibrary(workerConnection);
    await refreshRecommendedPlaylists(workerState.weeklyPlaylistCount);
    await syncSubsonicPlaylists(workerConnection);
    workerState.lastRunAt = new Date().toISOString();
    workerState.lastRunWeek = isoWeekKey();
    setWorkerScheduleHints();
    persistSettings();
    appendLog({
      level: "info",
      scope: "system",
      message: "Worker cycle completed",
      meta: { trigger, lastRunAt: workerState.lastRunAt, week: workerState.lastRunWeek }
    });
  } catch (error) {
    workerState.error = error instanceof Error ? error.message : "Worker cycle failed";
    persistSettings();
    appendLog({
      level: "error",
      scope: "system",
      message: "Worker cycle failed",
      meta: { trigger, error: workerState.error }
    });
    throw error;
  }
}

async function runScheduleLoop() {
  while (workerState.running) {
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

function startWorkerScheduler() {
  workerState.running = true;
  setWorkerScheduleHints();
  persistSettings();
  void runScheduleLoop();
}

function stopWorkerScheduler() {
  workerState.running = false;
  workerState.nextRunAt = null;
}

function resumeWorkerSchedulerFromPersistence() {
  if (!workerState.running) {
    setWorkerScheduleHints();
    return;
  }
  if (!workerConnection) {
    workerState.running = false;
    workerState.error = "Worker could not resume because Navidrome settings are incomplete";
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
  workerState.error = null;
  setWorkerScheduleHints();
  startWorkerScheduler();
  appendLog({
    level: "info",
    scope: "system",
    message: "Worker resumed from persisted state",
    meta: { weeklyPlaylistCount: workerState.weeklyPlaylistCount, source: workerConnection.baseUrl }
  });
}

function workerSnapshot() {
  return {
    running: workerState.running,
    weeklyPlaylistCount: workerState.weeklyPlaylistCount,
    lastRunAt: workerState.lastRunAt,
    lastRunWeek: workerState.lastRunWeek,
    nextRunAt: workerState.nextRunAt,
    error: workerState.error,
    connectionConfigured: Boolean(workerConnection)
  };
}

function settingsSnapshot() {
  return {
    navidromeUrl: settings.navidromeUrl,
    navidromeUsername: settings.navidromeUsername,
    navidromePassword: "",
    hasNavidromePassword: Boolean(settings.navidromePassword),
    lastFmApiKey: "",
    hasLastFmApiKey: Boolean(settings.lastFmApiKey),
    weeklyPlaylistCount: settings.weeklyPlaylistCount,
    maxTracksPerPlaylist: settings.maxTracksPerPlaylist,
    cycleFrequency: settings.cycleFrequency
  };
}

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, dbFile, navidromeUrl: settings.navidromeUrl });
});

app.get("/api/settings", (_req, res) => {
  res.json(settingsSnapshot());
});

app.patch("/api/settings", (req, res) => {
  const body = (req.body ?? {}) as Record<string, unknown>;
  if (body.navidromeUrl !== undefined) {
    settings.navidromeUrl = String(body.navidromeUrl ?? "").trim().replace(/\/+$/, "");
  }
  if (body.navidromeUsername !== undefined) {
    settings.navidromeUsername = String(body.navidromeUsername ?? "").trim();
  }
  if (body.navidromePassword !== undefined) {
    settings.navidromePassword = String(body.navidromePassword ?? "").trim();
  }
  if (body.lastFmApiKey !== undefined) {
    settings.lastFmApiKey = String(body.lastFmApiKey ?? "").trim();
  }
  if (body.weeklyPlaylistCount !== undefined) {
    settings.weeklyPlaylistCount = clamp(Number(body.weeklyPlaylistCount), 1, 8);
    workerState.weeklyPlaylistCount = settings.weeklyPlaylistCount;
  }
  if (body.maxTracksPerPlaylist !== undefined) {
    settings.maxTracksPerPlaylist = clamp(Number(body.maxTracksPerPlaylist), 5, 100);
  }
  if (body.cycleFrequency !== undefined) {
    const freq = String(body.cycleFrequency ?? "").trim();
    if (CYCLE_FREQUENCIES[freq]) {
      settings.cycleFrequency = freq;
      configureSchedule(settings.cycleFrequency);
    }
  }
  syncWorkerConnectionFromSettings();
  if (workerState.running && !workerConnection) {
    stopWorkerScheduler();
    workerState.error = "Worker stopped because Navidrome settings are incomplete";
  } else if (workerState.running) {
    setWorkerScheduleHints();
    workerState.error = null;
  }
  persistSettings();
  appendLog({
    level: "info",
    scope: "system",
    message: "Settings updated",
    meta: { weeklyPlaylistCount: settings.weeklyPlaylistCount }
  });
  res.json(settingsSnapshot());
});

app.get("/api/stats", (_req, res) => {
  const trackCount = db.prepare("SELECT COUNT(*) as count FROM tracks").get() as { count: number };
  const playlistCount = db.prepare("SELECT COUNT(*) as count FROM playlists").get() as { count: number };
  const analyzedTrackCount = db
    .prepare("SELECT COUNT(*) as count FROM tracks WHERE analysis_version = ? AND audio_vector_json != '[]'")
    .get(audioAnalysisVersion) as { count: number };
  res.json({ tracks: trackCount.count, playlists: playlistCount.count, analyzedTracks: analyzedTrackCount.count });
});

app.get("/api/ops", (req, res) => {
  const limit = Number(req.query.limit ?? 200);
  const bounded = Math.max(1, Math.min(limit, 500));
  res.json(operationLogs.slice(-bounded).reverse());
});

app.get("/api/tracks", (req, res) => {
  const limit = Number(req.query.limit ?? 100);
  const rows = readTracksStmt.all(Math.max(1, Math.min(limit, 500))) as TrackRecord[];
  res.json(
    rows.map((row) => ({
      ...row,
      tags: tagsFromTrack(row),
      audioFeatures: audioFeaturesFromTrack(row),
      audioVector: audioVectorFromTrack(row)
    }))
  );
});

app.get("/api/worker", (_req, res) => {
  res.json(workerSnapshot());
});

app.post("/api/worker/start", (req, res) => {
  let connection: SubsonicConnection;
  try {
    connection = resolveConnection(req.body);
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : "Invalid Subsonic connection" });
    return;
  }
  const requestedCount = Number(req.body?.weeklyPlaylistCount ?? workerState.weeklyPlaylistCount);
  workerConnection = connection;
  workerState.weeklyPlaylistCount = clamp(requestedCount, 1, 8);
  settings.weeklyPlaylistCount = workerState.weeklyPlaylistCount;
  settings.navidromeUrl = connection.baseUrl;
  settings.navidromeUsername = connection.username;
  settings.navidromePassword = connection.password;
  workerState.running = true;
  workerState.error = null;
  setWorkerScheduleHints();
  persistSettings();
  startWorkerScheduler();
  void runWorkerCycle("start").catch(() => null);
  appendLog({
    level: "info",
    scope: "system",
    message: "Worker started",
    meta: { weeklyPlaylistCount: workerState.weeklyPlaylistCount, source: connection.baseUrl }
  });
  res.json(workerSnapshot());
});

app.post("/api/worker/stop", (_req, res) => {
  stopWorkerScheduler();
  persistSettings();
  appendLog({
    level: "info",
    scope: "system",
    message: "Worker stopped"
  });
  res.json(workerSnapshot());
});

app.post("/api/scan", async (req, res) => {
  try {
    const connection = resolveConnection(req.body);
    const result = await scanLibrary(connection, undefined, { forceRefreshAnalysis: Boolean(req.body?.forceRefreshAnalysis) });
    await refreshRecommendedPlaylists();
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error instanceof Error ? error.message : "Scan failed" });
  }
});

app.post("/api/scan/start", (req, res) => {
  let connection: SubsonicConnection;
  try {
    connection = resolveConnection(req.body);
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : "Invalid Subsonic connection" });
    return;
  }
  const id = randomUUID();
  const startedAt = new Date().toISOString();
  const job: ScanProgress = {
    id,
    status: "queued",
    libraryPath: connection.baseUrl,
    startedAt,
    finishedAt: null,
    scannedFiles: 0,
    processedFiles: 0,
    updatedTracks: 0,
    errors: 0,
    message: null
  };
  scanJobs.set(id, job);
  activeScanJobId = id;
  const honkerJobId = enqueueScan(connection);
  appendLog({
    level: "info",
    scope: "scan",
    message: "Scan job created",
    meta: { id, honkerJobId, source: connection.baseUrl }
  });
  void (async () => {
    const current = scanJobs.get(id);
    if (!current) {
      return;
    }
    current.status = "running";
    scanJobs.set(id, current);
    try {
      const result = await scanLibrary(
        connection,
        (progress) => {
        const active = scanJobs.get(id);
        if (!active) {
          return;
        }
        Object.assign(active, progress);
        scanJobs.set(id, active);
        },
        { forceRefreshAnalysis: Boolean(req.body?.forceRefreshAnalysis) }
      );
      const completed = scanJobs.get(id);
      if (!completed) {
        return;
      }
      completed.status = "completed";
      completed.finishedAt = new Date().toISOString();
      completed.scannedFiles = result.scannedFiles;
      completed.processedFiles = result.processedFiles;
      completed.updatedTracks = result.updatedTracks;
      completed.errors = result.errors;
      scanJobs.set(id, completed);
      if (activeScanJobId === id) {
        activeScanJobId = null;
      }
      appendLog({
        level: "info",
        scope: "scan",
        message: "Scan job completed",
        meta: { id, scannedFiles: result.scannedFiles, processedFiles: result.processedFiles, updatedTracks: result.updatedTracks, errors: result.errors }
      });
      await refreshRecommendedPlaylists();
      await syncSubsonicPlaylists(connection);
      appendLog({
        level: "info",
        scope: "playlist",
        message: "Managed playlists synced after scan",
        meta: { subsonic: connection.baseUrl }
      });
    } catch (error) {
      const failed = scanJobs.get(id);
      if (!failed) {
        return;
      }
      failed.status = "failed";
      failed.finishedAt = new Date().toISOString();
      failed.message = error instanceof Error ? error.message : "Scan failed";
      scanJobs.set(id, failed);
      if (activeScanJobId === id) {
        activeScanJobId = null;
      }
      appendLog({
        level: "error",
        scope: "scan",
        message: "Scan job failed",
        meta: { id, error: failed.message }
      });
    }
  })();
  res.status(202).json({ id, status: job.status });
});

app.get("/api/scan/active", (_req, res) => {
  if (activeScanJobId) {
    const activeJob = scanJobs.get(activeScanJobId);
    if (activeJob && (activeJob.status === "queued" || activeJob.status === "running")) {
      res.json(activeJob);
      return;
    }
    activeScanJobId = null;
  }
  const fallback = [...scanJobs.values()]
    .filter((job) => job.status === "queued" || job.status === "running")
    .sort((a, b) => b.startedAt.localeCompare(a.startedAt))[0];
  if (!fallback) {
    res.json({ active: null });
    return;
  }
  activeScanJobId = fallback.id;
  res.json(fallback);
});

app.get("/api/scan/:id", (req, res) => {
  const id = String(req.params.id);
  const job = scanJobs.get(id);
  if (!job) {
    res.status(404).json({ error: "Scan job not found" });
    return;
  }
  res.json(job);
});

app.post("/api/playlists/generate", async (req, res) => {
  const requestedCount = Number(req.body?.weeklyPlaylistCount ?? workerState.weeklyPlaylistCount);
  const count = clamp(requestedCount, 1, 8);
  const { candidates } = await refreshRecommendedPlaylists(count, randomUUID());
  res.json({ generated: candidates.length, playlists: candidates.map((item) => ({ name: item.name, tracks: item.trackIds.length })) });
});

app.get("/api/playlists", (_req, res) => {
  const playlists = db
    .prepare(
      `SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at,
              artwork_path, artwork_attribution, artwork_attribution_url, artwork_signature, artwork_updated_at
       FROM playlists
       WHERE is_recommended = 1 OR mode IN ('pinned', 'locked')
       ORDER BY CASE mode WHEN 'locked' THEN 0 WHEN 'pinned' THEN 1 ELSE 2 END, name ASC`
    )
    .all() as PlaylistRecord[];
  const response = playlists.map((playlist) => {
    const tracks = db
      .prepare(
        `SELECT t.id, t.title, t.artist, t.album, t.path
         FROM playlist_tracks pt
         JOIN tracks t ON t.id = pt.track_id
         WHERE pt.playlist_id = ?
         ORDER BY pt.position`
      )
      .all(playlist.id);
    return {
      ...playlist,
      artworkUrl: playlist.artwork_path || null,
      artworkAttribution: playlist.artwork_attribution || null,
      artworkAttributionUrl: playlist.artwork_attribution_url || null,
      tracks
    };
  });
  res.json(response);
});

app.get("/api/playlists/snapshots", (_req, res) => {
  const snapshots = db
    .prepare(
      `SELECT ps.id, ps.playlist_id, ps.slug, ps.name, ps.track_ids_json, ps.snapshot_week, ps.created_at,
              COUNT(pt.track_id) as track_count
       FROM playlist_snapshots ps
       LEFT JOIN playlist_tracks pt ON pt.playlist_id = ps.playlist_id
       WHERE ps.playlist_id IN (SELECT id FROM playlists WHERE is_recommended = 1 OR mode IN ('pinned', 'locked'))
       GROUP BY ps.id
       ORDER BY ps.created_at DESC
       LIMIT 40`
    )
    .all() as Array<{ id: number; playlist_id: number; slug: string; name: string; track_ids_json: string; snapshot_week: string; created_at: string; track_count: number }>;
  res.json(
    snapshots.map((s) => {
      let trackIds: number[] = [];
      try {
        trackIds = JSON.parse(s.track_ids_json) as number[];
      } catch { /* empty */ }
      return {
        id: s.id,
        playlistId: s.playlist_id,
        slug: s.slug,
        name: s.name,
        trackIds,
        trackCount: Array.isArray(trackIds) ? trackIds.length : s.track_count,
        snapshotWeek: s.snapshot_week,
        createdAt: s.created_at
      };
    })
  );
});

app.patch("/api/playlists/:id", async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Invalid playlist id" });
    return;
  }
  const existing = db
    .prepare("SELECT id, slug, mode, selected, sync_target, is_recommended FROM playlists WHERE id = ?")
    .get(id) as
    | {
        id: number;
        slug: string;
        mode: "dynamic" | "pinned" | "locked";
        selected: number;
        sync_target: "subsonic";
        is_recommended: number;
      }
    | undefined;
  if (!existing) {
    res.status(404).json({ error: "Playlist not found" });
    return;
  }
  const modeInput = req.body?.mode;
  const selectedInput = req.body?.selected;
  const syncTargetInput = req.body?.syncTarget;
  const selected = typeof selectedInput === "boolean" ? (selectedInput ? 1 : 0) : existing.selected;
  const mode: "dynamic" | "pinned" | "locked" =
    modeInput === "locked" || modeInput === "pinned" || modeInput === "dynamic" ? modeInput : existing.mode;
  const syncTarget: "subsonic" = syncTargetInput === "subsonic" || !syncTargetInput ? "subsonic" : existing.sync_target;
  db.prepare("UPDATE playlists SET mode = ?, selected = ?, sync_target = ?, is_recommended = ?, updated_at = ? WHERE id = ?").run(
    mode,
    selected,
    syncTarget,
    mode === "dynamic" ? existing.is_recommended : 1,
    new Date().toISOString(),
    id
  );
  const modeLabel = mode === "dynamic" ? "weekly" : mode;
  appendLog({
    level: "info",
    scope: "playlist",
    message: `Playlist set to ${modeLabel}`,
    meta: { id, mode, selected, syncTarget }
  });
  res.json({ ok: true });
});

app.post("/api/playlists/apply", async (req, res) => {
  try {
    const connection = resolveConnection(req.body);
    const result = await syncSubsonicPlaylists(connection);
    appendLog({
      level: "info",
      scope: "playlist",
      message: "Managed playlists applied to Navidrome",
      meta: { subsonic: connection.baseUrl, ...result }
    });
    res.json(result);
  } catch (error) {
    appendLog({
      level: "error",
      scope: "playlist",
      message: "Playlist apply failed",
      meta: { error: error instanceof Error ? error.message : "Unknown error" }
    });
    res.status(500).json({ error: error instanceof Error ? error.message : "Failed applying playlist changes" });
  }
});


if (frontendDistDir) {
  app.use(express.static(frontendDistDir));
  app.use((req, res, next) => {
    if (req.method !== "GET" || req.path.startsWith("/api/")) {
      next();
      return;
    }
    res.sendFile(path.join(frontendDistDir, "index.html"));
  });
}

bootstrapScheduler(dbFile, settings.cycleFrequency);
log.info("scheduler bootstrapped", { cycleFrequency: settings.cycleFrequency });
resumeWorkerSchedulerFromPersistence();

app.listen(port, () => {
  log.info("server started", { port, dbFile, frontendDistDir: frontendDistDir ?? "none" });
});
