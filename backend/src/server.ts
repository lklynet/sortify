import cors from "cors";
import Database from "better-sqlite3";
import { config } from "dotenv";
import express from "express";
import { spawn } from "node:child_process";
import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID, scryptSync } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

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
};

type PlaylistCandidate = {
  name: string;
  slug: string;
  description: string;
  trackIds: number[];
  source: "cluster" | "mood" | "artist" | "discovery";
  signatureTags: string[];
  audioVector: number[];
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
  cycleRunning: boolean;
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
};

type OperationLogEntry = {
  id: string;
  timestamp: string;
  level: "info" | "warn" | "error";
  scope: "scan" | "playlist" | "system";
  message: string;
  meta?: Record<string, unknown>;
};

type SubsonicConnection = {
  baseUrl: string;
  username: string;
  password: string;
};

type SubsonicSong = {
  id: string;
  title?: string;
  artist?: string;
  album?: string;
  year?: number;
  duration?: number;
  genre?: string;
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
  moodTags: string[];
};

type RankedTagSource = {
  source: "base" | "musicbrainz-recording" | "musicbrainz-artist" | "lastfm-track" | "lastfm-artist" | "lastfm-album" | "audio";
  weight: number;
  tags: string[];
};

type LastFmTagResult = {
  trackTags: string[];
  artistTags: string[];
  albumTags: string[];
};

type MusicBrainzTagResult = {
  recordingTags: string[];
  artistTags: string[];
  year: number | null;
};

const envCandidates = [path.resolve(process.cwd(), ".env"), path.resolve(process.cwd(), "../.env")];
for (const envFile of envCandidates) {
  if (fs.existsSync(envFile)) {
    config({ path: envFile });
    break;
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
const audioAnalysisScriptFile = path.resolve(serverDir, "../scripts/analyze_track.py");
const audioAnalysisVersion = "audio-v1";
const audioAnalysisEnabled = process.env.AUDIO_ANALYSIS_ENABLED !== "false";
const audioAnalysisSampleSeconds = Math.max(30, Math.min(180, Number(process.env.AUDIO_ANALYSIS_SAMPLE_SECONDS ?? 90)));
const audioAnalysisTimeoutMs = Math.max(10_000, Math.min(90_000, Number(process.env.AUDIO_ANALYSIS_TIMEOUT_MS ?? 25_000)));
const encryptedSettingPrefix = "enc-v1";
const scanJobs = new Map<string, ScanProgress>();
const operationLogs: OperationLogEntry[] = [];
let activeScanJobId: string | null = null;
let workerConnection: SubsonicConnection | null = null;
let workerTimer: NodeJS.Timeout | null = null;
let previousRecommendedPlaylistNamesBySlug = new Map<string, string>();
const workerTickMs = 60_000;
const workerState: WorkerState = {
  running: false,
  cycleRunning: false,
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
  maxTracksPerPlaylist: defaultMaxTracksPerPlaylist
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
  is_recommended INTEGER NOT NULL DEFAULT 1
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
if (!hasColumn("selected")) {
  db.exec("ALTER TABLE playlists ADD COLUMN selected INTEGER NOT NULL DEFAULT 0");
}
if (!hasColumn("sync_target")) {
  db.exec("ALTER TABLE playlists ADD COLUMN sync_target TEXT NOT NULL DEFAULT 'subsonic'");
}
if (!hasColumn("is_recommended")) {
  db.exec("ALTER TABLE playlists ADD COLUMN is_recommended INTEGER NOT NULL DEFAULT 1");
}
db.exec("UPDATE playlists SET mode = 'locked' WHERE mode = 'frozen'");
db.exec("UPDATE playlists SET mode = 'dynamic' WHERE mode NOT IN ('dynamic', 'pinned', 'locked')");
db.exec("UPDATE playlists SET sync_target = 'subsonic' WHERE sync_target = 'm3u'");
db.exec("UPDATE playlists SET updated_at = CASE WHEN updated_at = '' THEN created_at ELSE updated_at END");

app.use(cors());
app.use(express.json({ limit: "2mb" }));

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
  "SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at FROM playlists WHERE slug = ?"
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
const deletePlaylistByIdStmt = db.prepare("DELETE FROM playlists WHERE id = ?");
const deletePlaylistTracksByPlaylistIdStmt = db.prepare("DELETE FROM playlist_tracks WHERE playlist_id = ?");
const insertPlaylistTrackStmt = db.prepare(`
INSERT INTO playlist_tracks (playlist_id, track_id, position)
VALUES (?, ?, ?)
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
    settings.weeklyPlaylistCount = clamp(weekly, 1, 5);
  }
  const maxTracks = Number.parseInt(read("maxTracksPerPlaylist") ?? "", 10);
  if (!Number.isNaN(maxTracks)) {
    settings.maxTracksPerPlaylist = clamp(maxTracks, 5, 100);
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
    upsertSettingStmt.run("workerRunning", String(workerState.running));
    upsertSettingStmt.run("workerLastRunAt", workerState.lastRunAt ?? "");
    upsertSettingStmt.run("workerLastRunWeek", workerState.lastRunWeek ?? "");
    upsertSettingStmt.run("workerError", workerState.error ?? "");
  });
  transaction();
}

loadPersistedSettings();

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

const moodRules: Record<string, string[]> = {
  Chill: ["chill", "ambient", "downtempo", "dream", "lofi", "trip-hop", "calm"],
  "High Energy": ["edm", "dance", "drum and bass", "house", "electro", "metal", "punk", "hardcore"],
  Melancholic: ["sad", "melancholic", "dark", "slowcore", "shoegaze", "blues", "ballad"]
};

const stopTags = new Set([
  "seen live",
  "seen_live",
  "favorites",
  "favourite",
  "favorite",
  "my",
  "songs",
  "tracks",
  "artist",
  "love",
  "loved",
  "beautiful",
  "awesome",
  "amazing",
  "masterpiece",
  "great",
  "good",
  "best",
  "spotify",
  "last.fm",
  "lastfm",
  "playlist",
  "heard on pandora",
  "heard",
  "to listen",
  "listen",
  "100%",
  "5 stars",
  "5 star",
  "music",
  "track",
  "song",
  "album",
  "cool",
  "nice",
  "perfect",
  "epic",
  "brilliant",
  "fav",
  "favs",
  "faves",
  "love at first listen"
]);

const tagAliases: Record<string, string> = {
  "hip hop": "hip-hop",
  "hiphop": "hip-hop",
  "r n b": "r&b",
  "rnb": "r&b",
  "r and b": "r&b",
  "rock and roll": "rock & roll",
  "rock n roll": "rock & roll",
  "drum and bass": "drum & bass",
  "dnb": "drum & bass",
  "d&b": "drum & bass",
  "electronica": "electronic",
  "synthpop": "synth-pop",
  "indie pop": "indie-pop",
  "indie rock": "indie-rock",
  "alt rock": "alternative rock",
  "alt-rock": "alternative rock",
  "post punk": "post-punk"
};

const lastFmArtistTagsCache = new Map<string, string[]>();
const lastFmAlbumTagsCache = new Map<string, string[]>();
const musicBrainzArtistTagsCache = new Map<string, string[]>();

const lowSignalTags = new Set(["decade", "low tempo", "mid tempo", "year"]);
const playlistPoolTargetSize = 72;
const maxArtistPerPlaylist = 1;
const maxArtistFallbackPerPlaylist = 2;

function isDecadeTag(tag: string): boolean {
  return /^(?:\d{2}|\d{3}|\d{4})s$/.test(tag);
}

function isPrimaryClusterTag(tag: string): boolean {
  if (!tag) {
    return false;
  }
  if (isDecadeTag(tag)) {
    return false;
  }
  if (lowSignalTags.has(tag)) {
    return false;
  }
  return true;
}

function tagWeight(tag: string): number {
  if (isDecadeTag(tag) || tag === "decade") {
    return 0.12;
  }
  if (lowSignalTags.has(tag)) {
    return 0.45;
  }
  return 1;
}

function hashSeed(input: string): number {
  let hash = 2166136261;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function seededRandom(seed: number): () => number {
  let value = seed || 1;
  return () => {
    value += 0x6d2b79f5;
    let result = Math.imul(value ^ (value >>> 15), value | 1);
    result ^= result + Math.imul(result ^ (result >>> 7), result | 61);
    return ((result ^ (result >>> 14)) >>> 0) / 4294967296;
  };
}

function isoWeekKey(now = new Date()): string {
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const weekNumber = Math.ceil(((date.getTime() - yearStart.getTime()) / 86400000 + 1) / 7);
  return `${date.getUTCFullYear()}-W${String(weekNumber).padStart(2, "0")}`;
}

function limitTracksByArtist(
  trackIds: number[],
  artistByTrackId: Map<number, string>,
  maxPerArtist: number,
  targetSize: number,
  fallbackMaxPerArtist = maxPerArtist
): number[] {
  const selected: number[] = [];
  const selectedSet = new Set<number>();
  const artistCount = new Map<string, number>();
  for (const trackId of trackIds) {
    const artist = artistByTrackId.get(trackId) ?? `track-${trackId}`;
    const count = artistCount.get(artist) ?? 0;
    if (count >= maxPerArtist) {
      continue;
    }
    selected.push(trackId);
    selectedSet.add(trackId);
    artistCount.set(artist, count + 1);
    if (selected.length >= targetSize) {
      break;
    }
  }
  if (selected.length >= Math.min(targetSize, trackIds.length)) {
    return selected;
  }
  for (const trackId of trackIds) {
    if (selectedSet.has(trackId)) {
      continue;
    }
    const artist = artistByTrackId.get(trackId) ?? `track-${trackId}`;
    const count = artistCount.get(artist) ?? 0;
    if (count >= fallbackMaxPerArtist) {
      continue;
    }
    selected.push(trackId);
    selectedSet.add(trackId);
    artistCount.set(artist, count + 1);
    if (selected.length >= targetSize) {
      break;
    }
  }
  return selected;
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

const namingFamilies = [
  {
    id: "nocturnal",
    match: ["ambient", "dark", "night", "nocturne", "dream", "slowcore", "shoegaze", "trip-hop"],
    adjectives: ["Nocturnal", "Moonlit", "Dusky", "Shadow", "Midnight", "Lowlight", "Afterhours", "Hushed"],
    textures: ["Velvet", "Smoke", "Ash", "Static", "Obsidian", "Glass", "Mist", "Echo"],
    nouns: ["Drift", "Signal", "Tide", "Afterglow", "Horizon", "Current", "Ritual", "Orbit"]
  },
  {
    id: "kinetic",
    match: ["dance", "house", "edm", "electronic", "techno", "drum & bass", "jungle", "club"],
    adjectives: ["Kinetic", "Electric", "Neon", "Rapid", "Bright", "Restless", "Fever", "Infrared"],
    textures: ["Chrome", "Circuit", "Laser", "Static", "Voltage", "Digital", "Strobe", "Mirror"],
    nouns: ["Pulse", "Rush", "Ignition", "Motion", "Surge", "Orbit", "Drive", "Frequency"]
  },
  {
    id: "organic",
    match: ["acoustic", "folk", "americana", "country", "organic", "singer-songwriter", "bluegrass"],
    adjectives: ["Golden", "Earthbound", "Open", "Dusty", "Warm", "Sunworn", "Woodland", "Plainspoken"],
    textures: ["Amber", "Cedar", "Canvas", "Meadow", "Lantern", "Soil", "Prairie", "Oak"],
    nouns: ["Trails", "Fields", "Breeze", "Valley", "Harbor", "Campfire", "Meadow", "Path"]
  },
  {
    id: "heavy",
    match: ["metal", "doom", "sludge", "hardcore", "punk", "industrial", "noise", "grindcore"],
    adjectives: ["Iron", "Feral", "Crimson", "Blackened", "Shattered", "Savage", "Burning", "Rusted"],
    textures: ["Steel", "Ash", "Soot", "Concrete", "Furnace", "Ember", "Granite", "Smoke"],
    nouns: ["Collapse", "Ritual", "Pressure", "Strike", "Surge", "Faultline", "March", "Voltage"]
  },
  {
    id: "lush",
    match: ["soul", "r&b", "funk", "disco", "jazz", "groove", "boogie", "swing"],
    adjectives: ["Velvet", "Satin", "Late", "Honeyed", "Luminous", "Slowburn", "Silken", "Golden"],
    textures: ["Mirage", "Rouge", "Smoke", "Velour", "Brass", "Lace", "Neon", "Ivory"],
    nouns: ["Groove", "Parade", "Current", "Room", "Afterglow", "Boulevard", "Shimmer", "Pulse"]
  },
  {
    id: "cinematic",
    match: ["classical", "orchestral", "cinematic", "post-rock", "soundtrack", "instrumental", "ambient"],
    adjectives: ["Luminous", "Vast", "Radiant", "Silver", "Weightless", "Quiet", "Endless", "Celestial"],
    textures: ["Halo", "Glass", "Ivory", "Marble", "Mist", "Skyline", "Aurora", "Prism"],
    nouns: ["Nocturne", "Passage", "Horizon", "Arc", "Sky", "Afterglow", "Bloom", "Overture"]
  }
] as const;

const namingFallback = {
  adjectives: ["Velvet", "Silver", "Tender", "Restless", "Radiant", "Faded", "Wild", "Quiet", "Golden", "Midnight"],
  textures: ["Smoke", "Chrome", "Amber", "Glass", "Ash", "Static", "Cedar", "Halo", "Velour", "Mirror"],
  nouns: ["Drift", "Pulse", "Tide", "Orbit", "Bloom", "Signal", "Current", "Afterglow", "Ritual", "Horizon"]
};

const lowValueNameTags = new Set([
  "high energy",
  "low tempo",
  "mid tempo",
  "decade",
  "favorites",
  "favorite",
  "favorite tracks",
  "songs",
  "tracks",
  "music",
  "artist"
]);

function titleCaseWords(value: string): string {
  return value
    .split(/[\s-]+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function uniqueWordPool(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))];
}

function pickWord(random: () => number, values: string[], fallback: string) {
  return values[Math.floor(random() * values.length)] ?? fallback;
}

function inferNamingPools(signatureTags: string[], audioVector: number[]) {
  const matchingFamilies = namingFamilies
    .map((family) => ({
      family,
      score: family.match.reduce((total, tag) => total + (signatureTags.includes(tag) ? 1 : 0), 0)
    }))
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map((entry) => entry.family);

  const adjectives = [
    ...matchingFamilies.flatMap((family) => family.adjectives),
    ...(audioVector[1] ?? 0) > 0.7 ? ["Fever", "Rapid", "Restless"] : [],
    ...(audioVector[1] ?? 0) < 0.35 ? ["Quiet", "Soft", "Still"] : [],
    ...(audioVector[3] ?? 0) < 0.35 ? ["Faded", "Tender", "Haunted"] : [],
    ...(audioVector[3] ?? 0) > 0.68 ? ["Bright", "Golden", "Lively"] : [],
    ...(audioVector[4] ?? 0) > 0.62 ? ["Earthbound", "Plainspoken", "Warm"] : [],
    ...(audioVector[6] ?? 0) > 0.66 ? ["Luminous", "Radiant", "Neon"] : [],
    ...(audioVector[6] ?? 0) < 0.32 ? ["Dusky", "Lowlight", "Shadow"] : [],
    ...namingFallback.adjectives
  ];

  const textures = [
    ...matchingFamilies.flatMap((family) => family.textures),
    ...(audioVector[5] ?? 0) > 0.62 ? ["Mist", "Halo", "Echo"] : [],
    ...(audioVector[1] ?? 0) > 0.72 ? ["Voltage", "Chrome", "Strobe"] : [],
    ...(audioVector[4] ?? 0) > 0.64 ? ["Cedar", "Amber", "Canvas"] : [],
    ...(audioVector[6] ?? 0) < 0.32 ? ["Ash", "Smoke", "Obsidian"] : [],
    ...namingFallback.textures
  ];

  const nouns = [
    ...matchingFamilies.flatMap((family) => family.nouns),
    ...(audioVector[2] ?? 0) > 0.7 ? ["Pulse", "Motion", "Drive"] : [],
    ...(audioVector[0] ?? 0) < 0.34 ? ["Drift", "Tide", "Harbor"] : [],
    ...(audioVector[0] ?? 0) > 0.68 ? ["Ignition", "Surge", "Rush"] : [],
    ...(audioVector[5] ?? 0) > 0.64 ? ["Passage", "Sky", "Arc"] : [],
    ...namingFallback.nouns
  ];

  const accentTags = signatureTags.filter(
    (tag) => /^[a-z0-9& -]{3,18}$/i.test(tag) && !lowValueNameTags.has(tag) && !isDecadeTag(tag)
  );

  const dominantFamily = matchingFamilies[0]?.id ?? ((audioVector[1] ?? 0) > 0.66 ? "kinetic" : (audioVector[3] ?? 0) < 0.38 ? "nocturnal" : "hybrid");

  return {
    dominantFamily,
    adjectives: uniqueWordPool(adjectives),
    textures: uniqueWordPool(textures),
    nouns: uniqueWordPool(nouns),
    accentTags
  };
}

function buildGeneratedPlaylistName(signatureTags: string[], audioVector: number[], seedKey: string): string {
  const pools = inferNamingPools(signatureTags, audioVector);
  const seed = `${seedKey}:${signatureTags.join("|")}:${audioVector.join("|")}:${pools.dominantFamily}`;
  const random = seededRandom(hashSeed(seed));
  const adjective = pickWord(random, pools.adjectives, "Velvet");
  const texture = pickWord(random, pools.textures, "Smoke");
  const noun = pickWord(random, pools.nouns, "Drift");
  const accentTag = pools.accentTags[Math.floor(random() * pools.accentTags.length)] ?? "";
  const accent = accentTag ? titleCaseWords(accentTag) : "";
  const templates = accent
    ? [
        `${adjective} ${noun}`,
        `${texture} ${noun}`,
        `${adjective} ${accent} ${noun}`,
        `${texture} ${accent} ${noun}`,
        `${accent} ${noun}`
      ]
    : [`${adjective} ${noun}`, `${texture} ${noun}`, `${adjective} ${texture}`, `${texture} ${adjective} ${noun}`];
  const name = templates[Math.floor(random() * templates.length)] ?? `${adjective} ${noun}`;
  const cleaned = name.replace(/\s+/g, " ").trim();
  if (cleaned.split(" ").length > 3) {
    const compact = `${adjective} ${noun}`;
    return compact.replace(/\s+/g, " ").trim();
  }
  return cleaned;
}

function buildMoodPlaylistName(moodName: string, signatureTags: string[], audioVector: number[], generationKey: string): string {
  const seed = `${generationKey}:mood:${moodName}:${signatureTags.join("|")}:${audioVector.join("|")}`;
  const random = seededRandom(hashSeed(seed));
  const suffixes = ["Flow", "Current", "Pulse", "Arc", "Drift"];
  const templates = [
    `${moodName} ${pickWord(random, suffixes, "Flow")}`,
    buildGeneratedPlaylistName(signatureTags, audioVector, `${generationKey}:mood-name:${moodName}`),
    `${moodName} ${pickWord(random, ["Drive", "Tide", "Signal"], "Signal")}`
  ];
  return templates[Math.floor(random() * templates.length)] ?? `${moodName} Flow`;
}

function buildArtistPlaylistName(seedArtist: string, signatureTags: string[], audioVector: number[], generationKey: string): string {
  const seed = `${generationKey}:artist:${seedArtist}:${signatureTags.join("|")}:${audioVector.join("|")}`;
  const random = seededRandom(hashSeed(seed));
  const suffix = pickWord(random, ["Constellation", "Orbit", "Signal", "Axis", "Halo"], "Constellation");
  return `${seedArtist} ${suffix}`;
}

function buildDiscoveryPlaylistName(signatureTags: string[], audioVector: number[], generationKey: string): string {
  const seed = `${generationKey}:discovery:${signatureTags.join("|")}:${audioVector.join("|")}`;
  const random = seededRandom(hashSeed(seed));
  const names = [
    "Discovery Mix",
    "Hidden Current",
    "Offpath Pulse",
    "Fresh Finds",
    "Deep Discovery"
  ];
  return names[Math.floor(random() * names.length)] ?? "Discovery Mix";
}

function buildGeneratedPlaylistDescription(signatureTags: string[], audioVector: number[], source: PlaylistCandidate["source"]): string {
  const pools = inferNamingPools(signatureTags, audioVector);
  const vibe =
    pools.dominantFamily === "kinetic"
      ? "higher-energy movement"
      : pools.dominantFamily === "organic"
        ? "warm, organic textures"
        : pools.dominantFamily === "heavy"
          ? "heavier, high-impact edges"
          : pools.dominantFamily === "lush"
            ? "groove-led, fuller color"
            : pools.dominantFamily === "cinematic"
              ? "wide, cinematic atmosphere"
              : "low-light, reflective flow";
  const focus = signatureTags.slice(0, 4).join(", ");
  if (source === "discovery") {
    return `A discovery lane built from overlooked tracks that still align with ${focus || vibe}.`;
  }
  if (source === "artist") {
    return `A signature set clustered around ${focus || vibe} with tighter artist relationships.`;
  }
  return `A ${vibe} playlist shaped by ${focus || "closely related tags and audio features"}.`;
}

function normalizeTag(value: string): string {
  let normalized = value.trim().toLowerCase().replace(/\s+/g, " ");
  if (tagAliases[normalized]) {
    normalized = tagAliases[normalized];
  }
  return normalized;
}

function uniqueTags(tags: string[]): string[] {
  return [...new Set(
    tags
      .map(normalizeTag)
      .filter((tag) => {
        if (tag.length <= 1) return false;
        if (stopTags.has(tag)) return false;
        // Filter out pure numbers (like exact years "2013" or counts "100") 
        // Note: Decades like "90s" or "1990s" will pass because of the 's'
        if (/^\d+$/.test(tag)) return false;
        return true;
      })
  )].slice(0, 40);
}

function mergeRankedTagSources(sources: RankedTagSource[], limit = 40): string[] {
  const scoredTags = new Map<
    string,
    {
      score: number;
      sourceCount: number;
      bestRank: number;
      hasAudio: boolean;
      hasExternalMetadata: boolean;
      hasHeuristic: boolean;
    }
  >();
  for (const source of sources) {
    const tags = uniqueTags(source.tags);
    tags.forEach((tag, index) => {
      const entry = scoredTags.get(tag) ?? {
        score: 0,
        sourceCount: 0,
        bestRank: Number.POSITIVE_INFINITY,
        hasAudio: false,
        hasExternalMetadata: false,
        hasHeuristic: false
      };
      const rankWeight = Math.max(0.24, 1 - index * 0.08);
      entry.score += source.weight * rankWeight * tagWeight(tag);
      entry.sourceCount += 1;
      entry.bestRank = Math.min(entry.bestRank, index);
      if (source.source === "audio") {
        entry.hasAudio = true;
      } else if (source.source === "base") {
        entry.hasHeuristic = true;
      } else {
        entry.hasExternalMetadata = true;
      }
      scoredTags.set(tag, entry);
    });
  }
  return [...scoredTags.entries()]
    .map(([tag, entry]) => ({
      tag,
      score:
        entry.score +
        Math.max(0, entry.sourceCount - 1) * 0.38 +
        (entry.hasAudio && entry.hasExternalMetadata ? 0.18 : 0) +
        (entry.hasHeuristic && entry.hasExternalMetadata ? 0.08 : 0),
      bestRank: entry.bestRank,
      sourceCount: entry.sourceCount
    }))
    .sort((a, b) => b.score - a.score || b.sourceCount - a.sourceCount || a.bestRank - b.bestRank || a.tag.localeCompare(b.tag))
    .slice(0, limit)
    .map((entry) => entry.tag);
}

function deriveBaseTags(
  title: string,
  artist: string,
  year: number | null,
  rawGenres: string[],
  bpm: number | null
): string[] {
  const tags = [...rawGenres];
  if (year) {
    tags.push(`${Math.floor(year / 10) * 10}s`, "decade");
  }
  if (bpm && Number.isFinite(bpm)) {
    if (bpm < 95) {
      tags.push("low tempo", "chill");
    } else if (bpm > 130) {
      tags.push("high energy", "dance");
    } else {
      tags.push("mid tempo");
    }
  }
  const text = `${title} ${artist}`.toLowerCase();
  if (/(acoustic|unplugged|folk)/.test(text)) {
    tags.push("acoustic");
  }
  if (/(live|session)/.test(text)) {
    tags.push("live");
  }
  if (/(remix|mix)/.test(text)) {
    tags.push("electronic");
  }
  return uniqueTags(tags);
}

async function fetchLastFmTopTags(params: URLSearchParams): Promise<string[]> {
  const apiKey = settings.lastFmApiKey;
  if (!apiKey) {
    return [];
  }
  params.set("api_key", apiKey);
  params.set("format", "json");
  params.set("autocorrect", "1");
  const url = `https://ws.audioscrobbler.com/2.0/?${params.toString()}`;
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(3500) });
    if (!response.ok) {
      return [];
    }
    const payload = (await response.json()) as {
      toptags?: { tag?: Array<{ name?: string; count?: number | string }> };
    };
    return uniqueTags(
      (payload.toptags?.tag ?? [])
        .map((tag) => ({
          name: tag.name ?? "",
          count: typeof tag.count === "number" ? tag.count : Number.parseInt(String(tag.count ?? "0"), 10)
        }))
        .filter((entry) => entry.name.length > 0)
        .sort((a, b) => b.count - a.count)
        .map((entry) => entry.name)
    );
  } catch {
    return [];
  }
}

async function fetchLastFmTags(artist: string, title: string, album?: string | null): Promise<LastFmTagResult> {
  if (!artist || !title) {
    return {
      trackTags: [],
      artistTags: [],
      albumTags: []
    };
  }
  const normalizedArtist = normalizeTag(artist);
  const normalizedAlbum = album ? normalizeTag(album) : "";

  const trackTagsPromise = fetchLastFmTopTags(
    new URLSearchParams({
      method: "track.getTopTags",
      artist,
      track: title
    })
  );

  const artistTagsPromise = (() => {
    if (lastFmArtistTagsCache.has(normalizedArtist)) {
      return Promise.resolve(lastFmArtistTagsCache.get(normalizedArtist) ?? []);
    }
    return fetchLastFmTopTags(
      new URLSearchParams({
        method: "artist.getTopTags",
        artist
      })
    ).then((tags) => {
      lastFmArtistTagsCache.set(normalizedArtist, tags);
      return tags;
    });
  })();

  const albumTagsPromise = (() => {
    if (!album || album === "Unknown Album") {
      return Promise.resolve([] as string[]);
    }
    const key = `${normalizedArtist}::${normalizedAlbum}`;
    if (lastFmAlbumTagsCache.has(key)) {
      return Promise.resolve(lastFmAlbumTagsCache.get(key) ?? []);
    }
    return fetchLastFmTopTags(
      new URLSearchParams({
        method: "album.getTopTags",
        artist,
        album
      })
    ).then((tags) => {
      lastFmAlbumTagsCache.set(key, tags);
      return tags;
    });
  })();

  const [trackTags, artistTags, albumTags] = await Promise.all([trackTagsPromise, artistTagsPromise, albumTagsPromise]);
  return {
    trackTags,
    artistTags,
    albumTags
  };
}

async function fetchMusicBrainzArtistTags(artistMbid: string): Promise<string[]> {
  if (!artistMbid) {
    return [];
  }
  if (musicBrainzArtistTagsCache.has(artistMbid)) {
    return musicBrainzArtistTagsCache.get(artistMbid) ?? [];
  }
  const url = `https://musicbrainz.org/ws/2/artist/${encodeURIComponent(artistMbid)}?fmt=json&inc=tags`;
  try {
    const response = await fetch(url, {
      signal: AbortSignal.timeout(5000),
      headers: {
        "User-Agent": "Sortify/0.1 ( self-hosted metadata enrich )"
      }
    });
    if (!response.ok) {
      musicBrainzArtistTagsCache.set(artistMbid, []);
      return [];
    }
    const payload = (await response.json()) as { tags?: Array<{ name?: string; count?: number | string }> };
    const tags = uniqueTags(
      (payload.tags ?? [])
        .map((tag) => ({
          name: tag.name ?? "",
          count: typeof tag.count === "number" ? tag.count : Number.parseInt(String(tag.count ?? "0"), 10)
        }))
        .filter((entry) => entry.name.length > 0)
        .sort((a, b) => b.count - a.count)
        .map((entry) => entry.name)
    );
    musicBrainzArtistTagsCache.set(artistMbid, tags);
    return tags;
  } catch {
    musicBrainzArtistTagsCache.set(artistMbid, []);
    return [];
  }
}

async function fetchMusicBrainzTags(artist: string, title: string): Promise<MusicBrainzTagResult> {
  if (!artist || !title) {
    return { recordingTags: [], artistTags: [], year: null };
  }
  const query = new URLSearchParams({
    query: `recording:"${title}" AND artist:"${artist}"`,
    fmt: "json",
    inc: "artist-credits+tags",
    limit: "5"
  });
  const url = `https://musicbrainz.org/ws/2/recording?${query.toString()}`;
  try {
    const response = await fetch(url, {
      signal: AbortSignal.timeout(5000),
      headers: {
        "User-Agent": "Sortify/0.1 ( self-hosted metadata enrich )"
      }
    });
    if (!response.ok) {
      return { recordingTags: [], artistTags: [], year: null };
    }
    const payload = (await response.json()) as {
      recordings?: Array<{
        score?: number | string;
        tags?: Array<{ name?: string; count?: number | string }>;
        "first-release-date"?: string;
        "artist-credit"?: Array<{ artist?: { id?: string } }>;
      }>;
    };
    const recordings = payload.recordings ?? [];
    if (!recordings.length) {
      return { recordingTags: [], artistTags: [], year: null };
    }
    const rankedRecordings = [...recordings].sort((a, b) => {
      const scoreA = typeof a.score === "number" ? a.score : Number.parseInt(String(a.score ?? "0"), 10);
      const scoreB = typeof b.score === "number" ? b.score : Number.parseInt(String(b.score ?? "0"), 10);
      return scoreB - scoreA;
    });
    const selected = rankedRecordings.filter((recording) => {
      const score = typeof recording.score === "number" ? recording.score : Number.parseInt(String(recording.score ?? "0"), 10);
      return score >= 70;
    });
    const sample = (selected.length ? selected : rankedRecordings).slice(0, 3);
    const bestDate = sample
      .map((recording) => recording["first-release-date"] ?? "")
      .find((value) => value.length >= 4) ?? "";
    const year = Number.parseInt(bestDate.slice(0, 4), 10);
    const recordingTags = uniqueTags(
      sample.flatMap((recording) =>
        (recording.tags ?? [])
          .map((tag) => ({
            name: tag.name ?? "",
            count: typeof tag.count === "number" ? tag.count : Number.parseInt(String(tag.count ?? "0"), 10)
          }))
          .filter((entry) => entry.name.length > 0)
          .sort((a, b) => b.count - a.count)
          .map((entry) => entry.name)
      )
    );
    const artistMbid = sample[0]?.["artist-credit"]?.[0]?.artist?.id ?? "";
    const artistTags = await fetchMusicBrainzArtistTags(artistMbid);
    return {
      recordingTags,
      artistTags,
      year: Number.isNaN(year) ? null : year
    };
  } catch {
    return { recordingTags: [], artistTags: [], year: null };
  }
}

function clampUnit(value: number): number {
  return Math.max(0, Math.min(1, value));
}

function buildSubsonicQuery(connection: SubsonicConnection, params: Record<string, string> = {}, includeFormat = true) {
  const salt = randomUUID().replace(/-/g, "").slice(0, 12);
  const token = createHash("md5").update(`${connection.password}${salt}`).digest("hex");
  return new URLSearchParams({
    u: connection.username,
    t: token,
    s: salt,
    v: "1.16.1",
    c: "sortify",
    ...(includeFormat ? { f: "json" } : {}),
    ...params
  });
}

function buildSubsonicStreamUrl(connection: SubsonicConnection, songId: string) {
  return `${connection.baseUrl}/rest/stream.view?${buildSubsonicQuery(connection, { id: songId }, false).toString()}`;
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

function audioVectorFromTrack(track: Pick<TrackRecord, "audio_vector_json">): number[] {
  try {
    const parsed = JSON.parse(track.audio_vector_json) as number[];
    return Array.isArray(parsed) ? parsed.filter((value) => Number.isFinite(value)).map((value) => Number(value)) : [];
  } catch {
    return [];
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
        const vector = Array.isArray(parsed.vector) ? parsed.vector.map((value) => clampUnit(Number(value))) : [];
        if (!features || !Array.isArray(features.moodTags) || vector.length < 8) {
          reject(new Error("Audio analysis returned invalid payload"));
          return;
        }
        resolve({
          features: {
            ...features,
            version: audioAnalysisVersion,
            bpm: typeof features.bpm === "number" && Number.isFinite(features.bpm) ? features.bpm : null,
            energy: clampUnit(Number(features.energy)),
            danceability: clampUnit(Number(features.danceability)),
            valence: clampUnit(Number(features.valence)),
            acousticness: clampUnit(Number(features.acousticness)),
            instrumentalness: clampUnit(Number(features.instrumentalness)),
            brightness: clampUnit(Number(features.brightness)),
            rhythmicDensity: clampUnit(Number(features.rhythmicDensity)),
            loudness: clampUnit(Number(features.loudness)),
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

function getSubsonicConnection(input: unknown): SubsonicConnection {
  const body = (input ?? {}) as Record<string, unknown>;
  const baseUrl = String(body.baseUrl ?? settings.navidromeUrl).trim().replace(/\/+$/, "");
  const username = String(body.username ?? settings.navidromeUsername).trim();
  const password = String(body.password ?? settings.navidromePassword).trim();
  if (!baseUrl || !username || !password) {
    throw new Error("Subsonic connection requires baseUrl, username, and password");
  }
  return { baseUrl, username, password };
}

async function subsonicRequest<T>(connection: SubsonicConnection, endpoint: string, params: Record<string, string> = {}): Promise<T> {
  const query = buildSubsonicQuery(connection, params);
  const response = await fetch(`${connection.baseUrl}/rest/${endpoint}?${query.toString()}`, {
    signal: AbortSignal.timeout(12000)
  });
  if (!response.ok) {
    throw new Error(`Subsonic request failed: ${response.status}`);
  }
  const payload = (await response.json()) as {
    "subsonic-response"?: {
      status?: string;
      error?: { message?: string };
    } & T;
  };
  const envelope = payload["subsonic-response"];
  if (!envelope || envelope.status !== "ok") {
    throw new Error(envelope?.error?.message ?? "Subsonic API error");
  }
  return envelope as T;
}

async function fetchSubsonicSongs(connection: SubsonicConnection): Promise<SubsonicSong[]> {
  const songs: SubsonicSong[] = [];
  const pageSize = 500;
  let offset = 0;
  while (true) {
    const list = await subsonicRequest<{ albumList2?: { album?: Array<{ id: string }> } }>(connection, "getAlbumList2.view", {
      type: "alphabeticalByName",
      size: String(pageSize),
      offset: String(offset)
    });
    const albums = list.albumList2?.album ?? [];
    if (!albums.length) {
      break;
    }
    for (const album of albums) {
      const detail = await subsonicRequest<{ album?: { song?: SubsonicSong[] } }>(connection, "getAlbum.view", { id: album.id });
      songs.push(...(detail.album?.song ?? []));
    }
    if (albums.length < pageSize) {
      break;
    }
    offset += pageSize;
  }
  return songs;
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
      const musicBrainz = await fetchMusicBrainzTags(artist, title);
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
      const baseTags = deriveBaseTags(title, artist, mergedYear, song.genre ? [song.genre] : [], audioFeatures?.bpm ?? null);
      const lastFmTags = await fetchLastFmTags(artist, title, album);
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
        album_artist: artist,
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

function tagsFromTrack(track: Pick<TrackRecord, "tags_json">): string[] {
  try {
    const parsed = JSON.parse(track.tags_json) as string[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function cosineScore(a: string[], b: string[]): number {
  if (!a.length || !b.length) {
    return 0;
  }
  const vecA = new Map<string, number>();
  const vecB = new Map<string, number>();
  for (const tag of a) {
    vecA.set(tag, (vecA.get(tag) ?? 0) + tagWeight(tag));
  }
  for (const tag of b) {
    vecB.set(tag, (vecB.get(tag) ?? 0) + tagWeight(tag));
  }
  let dot = 0;
  let magA = 0;
  let magB = 0;
  for (const value of vecA.values()) {
    magA += value * value;
  }
  for (const value of vecB.values()) {
    magB += value * value;
  }
  for (const [tag, value] of vecA) {
    dot += value * (vecB.get(tag) ?? 0);
  }
  if (magA === 0 || magB === 0) {
    return 0;
  }
  return dot / Math.sqrt(magA * magB);
}

function cosineNumberScore(a: number[], b: number[]): number {
  if (!a.length || !b.length || a.length !== b.length) {
    return 0;
  }
  let dot = 0;
  let magA = 0;
  let magB = 0;
  for (let index = 0; index < a.length; index += 1) {
    dot += a[index] * b[index];
    magA += a[index] * a[index];
    magB += b[index] * b[index];
  }
  if (magA === 0 || magB === 0) {
    return 0;
  }
  return dot / Math.sqrt(magA * magB);
}

function averageAudioVector(trackIds: number[], audioVectorByTrackId: Map<number, number[]>): number[] {
  const vectors = trackIds
    .map((trackId) => audioVectorByTrackId.get(trackId) ?? [])
    .filter((vector) => vector.length > 0);
  if (!vectors.length) {
    return [];
  }
  const length = vectors[0].length;
  const sums = new Array<number>(length).fill(0);
  let count = 0;
  for (const vector of vectors) {
    if (vector.length !== length) {
      continue;
    }
    count += 1;
    for (let index = 0; index < length; index += 1) {
      sums[index] += vector[index];
    }
  }
  if (!count) {
    return [];
  }
  return sums.map((value) => value / count);
}

function averageCentroidSimilarity(
  trackIds: number[],
  signatureTags: string[],
  audioVector: number[],
  primaryTagsByTrackId: Map<number, string[]>,
  audioVectorByTrackId: Map<number, number[]>
): number {
  const sample = trackIds.slice(0, 18);
  if (!sample.length) {
    return 0;
  }
  let total = 0;
  for (const trackId of sample) {
    const tagSimilarity = signatureTags.length ? cosineScore(primaryTagsByTrackId.get(trackId) ?? [], signatureTags) : 0;
    const audioSimilarity = audioVector.length ? cosineNumberScore(audioVectorByTrackId.get(trackId) ?? [], audioVector) : 0;
    total += tagSimilarity * 0.7 + audioSimilarity * 0.3;
  }
  return total / sample.length;
}

function topTagsFromTrackIds(trackIds: number[], primaryTagsByTrackId: Map<number, string[]>, limit: number): string[] {
  const counts = new Map<string, number>();
  for (const trackId of trackIds) {
    for (const tag of primaryTagsByTrackId.get(trackId) ?? []) {
      counts.set(tag, (counts.get(tag) ?? 0) + 1);
    }
  }
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([tag]) => tag);
}

function jaccardScore(valuesA: Iterable<string | number>, valuesB: Iterable<string | number>): number {
  const setA = new Set(valuesA);
  const setB = new Set(valuesB);
  if (!setA.size || !setB.size) {
    return 0;
  }
  let overlap = 0;
  for (const value of setA) {
    if (setB.has(value)) {
      overlap += 1;
    }
  }
  return overlap / (setA.size + setB.size - overlap);
}

function selectPlaylistTracksForVariety(
  trackIds: number[],
  artistByTrackId: Map<number, string>,
  globalArtistCounts: Map<string, number>,
  globalTrackIds: Set<number>,
  targetSize: number
): number[] {
  const selected: number[] = [];
  const selectedSet = new Set<number>();
  const playlistArtistCounts = new Map<string, number>();
  const passes = [
    { maxPlaylistArtistCount: maxArtistPerPlaylist, maxGlobalArtistCount: 0, allowRepeatedTracks: false },
    { maxPlaylistArtistCount: maxArtistPerPlaylist, maxGlobalArtistCount: 1, allowRepeatedTracks: false },
    { maxPlaylistArtistCount: maxArtistFallbackPerPlaylist, maxGlobalArtistCount: 2, allowRepeatedTracks: false },
    { maxPlaylistArtistCount: maxArtistFallbackPerPlaylist, maxGlobalArtistCount: Number.POSITIVE_INFINITY, allowRepeatedTracks: true }
  ];
  for (const pass of passes) {
    for (const trackId of trackIds) {
      if (selected.length >= targetSize || selectedSet.has(trackId)) {
        continue;
      }
      if (!pass.allowRepeatedTracks && globalTrackIds.has(trackId)) {
        continue;
      }
      const artist = artistByTrackId.get(trackId) ?? `track-${trackId}`;
      const playlistArtistCount = playlistArtistCounts.get(artist) ?? 0;
      const globalArtistCount = globalArtistCounts.get(artist) ?? 0;
      if (playlistArtistCount >= pass.maxPlaylistArtistCount || globalArtistCount > pass.maxGlobalArtistCount) {
        continue;
      }
      selected.push(trackId);
      selectedSet.add(trackId);
      playlistArtistCounts.set(artist, playlistArtistCount + 1);
    }
  }
  for (const trackId of selected) {
    globalTrackIds.add(trackId);
    const artist = artistByTrackId.get(trackId) ?? `track-${trackId}`;
    globalArtistCounts.set(artist, (globalArtistCounts.get(artist) ?? 0) + 1);
  }
  return selected;
}

function rerankTrackPoolForPlaylist(
  trackIds: number[],
  targetSize: number,
  centroidTags: string[],
  centroidAudioVector: number[],
  trackTagsByTrackId: Map<number, string[]>,
  audioVectorByTrackId: Map<number, number[]>,
  artistByTrackId: Map<number, string>,
  playCountByTrackId: Map<number, number>,
  favoriteByTrackId: Map<number, boolean>,
  maxPlayCount: number
): number[] {
  const uniqueTrackIds = [...new Set(trackIds)];
  if (!uniqueTrackIds.length) {
    return [];
  }
  const sourceRankByTrackId = new Map(uniqueTrackIds.map((trackId, index) => [trackId, index]));
  const selected: number[] = [];
  const selectedSet = new Set<number>();
  const artistCounts = new Map<string, number>();
  const passes = [maxArtistPerPlaylist, maxArtistFallbackPerPlaylist, Number.POSITIVE_INFINITY];
  for (const maxPerArtist of passes) {
    while (selected.length < targetSize) {
      let bestTrackId: number | null = null;
      let bestScore = Number.NEGATIVE_INFINITY;
      for (const trackId of uniqueTrackIds) {
        if (selectedSet.has(trackId)) {
          continue;
        }
        const artist = artistByTrackId.get(trackId) ?? `track-${trackId}`;
        const artistCount = artistCounts.get(artist) ?? 0;
        if (artistCount >= maxPerArtist) {
          continue;
        }
        const tags = trackTagsByTrackId.get(trackId) ?? [];
        const audioVector = audioVectorByTrackId.get(trackId) ?? [];
        const tagScore = centroidTags.length ? cosineScore(tags, centroidTags) : 0;
        const audioScore = centroidAudioVector.length ? cosineNumberScore(audioVector, centroidAudioVector) : 0;
        const playCount = playCountByTrackId.get(trackId) ?? 0;
        const noveltyScore = maxPlayCount > 0 ? 1 - Math.log1p(playCount) / Math.log1p(maxPlayCount) : 0.5;
        const favoriteBonus = favoriteByTrackId.get(trackId) ? 0.06 : 0;
        const sourceRank = sourceRankByTrackId.get(trackId) ?? uniqueTrackIds.length - 1;
        const sourceScore = uniqueTrackIds.length > 1 ? 1 - sourceRank / (uniqueTrackIds.length - 1) : 1;
        const redundancyPenalty = selected.length
          ? Math.max(
              ...selected.map((selectedTrackId) => {
                const selectedTags = trackTagsByTrackId.get(selectedTrackId) ?? [];
                const selectedAudio = audioVectorByTrackId.get(selectedTrackId) ?? [];
                return cosineScore(tags, selectedTags) * 0.72 + cosineNumberScore(audioVector, selectedAudio) * 0.28;
              })
            )
          : 0;
        const score = tagScore * 0.46 + audioScore * 0.24 + noveltyScore * 0.16 + sourceScore * 0.14 + favoriteBonus - redundancyPenalty * 0.18;
        if (score > bestScore) {
          bestScore = score;
          bestTrackId = trackId;
        }
      }
      if (bestTrackId === null) {
        break;
      }
      selected.push(bestTrackId);
      selectedSet.add(bestTrackId);
      const artist = artistByTrackId.get(bestTrackId) ?? `track-${bestTrackId}`;
      artistCounts.set(artist, (artistCounts.get(artist) ?? 0) + 1);
    }
    if (selected.length >= targetSize) {
      break;
    }
  }
  return selected;
}

function selectDiverseCandidates(
  candidates: PlaylistCandidate[],
  playlistCount: number,
  trackCount: number,
  artistByTrackId: Map<number, string>,
  primaryTagsByTrackId: Map<number, string[]>,
  audioVectorByTrackId: Map<number, number[]>,
  generationKey: string,
  prioritizedSlugs = new Set<string>()
): PlaylistCandidate[] {
  const candidateProfiles = candidates.map((candidate) => {
    const artistSet = new Set(candidate.trackIds.map((trackId) => artistByTrackId.get(trackId) ?? `track-${trackId}`));
    const signatureTags = candidate.signatureTags.length ? candidate.signatureTags : topTagsFromTrackIds(candidate.trackIds, primaryTagsByTrackId, 8);
    const audioVector = candidate.audioVector.length ? candidate.audioVector : averageAudioVector(candidate.trackIds, audioVectorByTrackId);
    const random = seededRandom(hashSeed(`${generationKey}:selection:${candidate.slug}:${candidate.name}`));
    const audioCoverage =
      candidate.trackIds.filter((trackId) => (audioVectorByTrackId.get(trackId) ?? []).length > 0).length / Math.max(candidate.trackIds.length, 1);
    const cohesion = averageCentroidSimilarity(candidate.trackIds, signatureTags, audioVector, primaryTagsByTrackId, audioVectorByTrackId);
    const intrinsicScore =
      Math.min(candidate.trackIds.length / Math.max(trackCount * 1.4, 1), 1) * 0.28 +
      Math.min(signatureTags.length / 8, 1) * 0.2 +
      Math.min(artistSet.size / Math.max(trackCount, 1), 1) * 0.16 +
      audioCoverage * 0.16 +
      cohesion * 0.2;
    return {
      candidate,
      artistSet,
      signatureTags,
      audioVector,
      intrinsicScore,
      selectionBias: (random() - 0.5) * 0.12
    };
  });
  const selected: typeof candidateProfiles = [];
  while (selected.length < playlistCount && selected.length < candidateProfiles.length) {
    const preferredPool = candidateProfiles.filter(
      (entry) => prioritizedSlugs.has(entry.candidate.slug) && !selected.includes(entry)
    );
    const usedSources = new Set(selected.map((entry) => entry.candidate.source));
    const selectionPool = preferredPool.length ? preferredPool : candidateProfiles.filter((entry) => !selected.includes(entry));
    const next = selectionPool
      .map((entry) => {
        if (!selected.length) {
          return { entry, score: entry.intrinsicScore + entry.selectionBias };
        }
        const similarities = selected.map((picked) => {
          const trackOverlap = jaccardScore(entry.candidate.trackIds, picked.candidate.trackIds);
          const artistOverlap = jaccardScore(entry.artistSet, picked.artistSet);
          const tagSimilarity = cosineScore(entry.signatureTags, picked.signatureTags);
          const audioSimilarity = cosineNumberScore(entry.audioVector, picked.audioVector);
          return trackOverlap * 0.35 + artistOverlap * 0.25 + tagSimilarity * 0.25 + audioSimilarity * 0.15;
        });
        const maxSimilarity = Math.max(...similarities);
        const averageSimilarity = similarities.reduce((total, value) => total + value, 0) / similarities.length;
        const sourceBonus = usedSources.has(entry.candidate.source) ? 0 : 0.08;
        return {
          entry,
          score: entry.intrinsicScore + entry.selectionBias + sourceBonus - maxSimilarity * 0.85 - averageSimilarity * 0.3
        };
      })
      .sort((a, b) => b.score - a.score)[0];
    if (!next) {
      break;
    }
    selected.push(next.entry);
  }
  const globalArtistCounts = new Map<string, number>();
  const globalTrackIds = new Set<number>();
  return selected.map(({ candidate }) => ({
    ...candidate,
    trackIds: selectPlaylistTracksForVariety(candidate.trackIds, artistByTrackId, globalArtistCounts, globalTrackIds, trackCount)
  }));
}

function slugify(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
}

async function generateCandidates(
  tracks: TrackRecord[],
  desiredWeeklyPlaylists: number,
  generationKey = isoWeekKey(),
  pinnedSlugs = new Set<string>(),
  lockedSlugs = new Set<string>()
): Promise<PlaylistCandidate[]> {
  const enriched = tracks.map((track) => ({
    ...track,
    tags: tagsFromTrack(track)
  }));
  const candidates: PlaylistCandidate[] = [];
  const artistByTrackId = new Map<number, string>();
  const primaryTagsByTrackId = new Map<number, string[]>();
  const trackTagsByTrackId = new Map<number, string[]>();
  const audioVectorByTrackId = new Map<number, number[]>();
  const playCountByTrackId = new Map<number, number>();
  const favoriteByTrackId = new Map<number, boolean>();
  const maxPlayCount = Math.max(...enriched.map((track) => Math.max(0, track.play_count ?? 0)), 0);

  for (const track of enriched) {
    artistByTrackId.set(track.id, track.artist ? normalizeTag(track.artist) : `track-${track.id}`);
    trackTagsByTrackId.set(track.id, track.tags);
    const primaryTags = uniqueTags(track.tags.filter((tag) => isPrimaryClusterTag(tag))).slice(0, 10);
    primaryTagsByTrackId.set(track.id, primaryTags);
    audioVectorByTrackId.set(track.id, audioVectorFromTrack(track));
    playCountByTrackId.set(track.id, Math.max(0, track.play_count ?? 0));
    favoriteByTrackId.set(track.id, Boolean(track.favorite));
  }

  const tagHistogram = new Map<string, number>();
  for (const tags of primaryTagsByTrackId.values()) {
    for (const tag of tags) {
      tagHistogram.set(tag, (tagHistogram.get(tag) ?? 0) + 1);
    }
  }
  const topClusterTags = [...tagHistogram.entries()]
    .filter(([, count]) => count >= 4)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 16)
    .map(([tag]) => tag);
  for (const primaryTag of topClusterTags) {
    const seedTracks = enriched.filter((track) => (primaryTagsByTrackId.get(track.id) ?? []).includes(primaryTag));
    if (seedTracks.length < 8) {
      continue;
    }
    const coTagCounts = new Map<string, number>();
    for (const track of seedTracks) {
      for (const tag of primaryTagsByTrackId.get(track.id) ?? []) {
        if (tag === primaryTag) {
          continue;
        }
        coTagCounts.set(tag, (coTagCounts.get(tag) ?? 0) + 1);
      }
    }
    const companionTags = [...coTagCounts.entries()]
      .filter(([, count]) => count >= 3)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([tag]) => tag);
    const centroid = uniqueTags([primaryTag, ...companionTags]).slice(0, 4);
    const seedAudioVector = averageAudioVector(
      seedTracks.map((track) => track.id),
      audioVectorByTrackId
    );
    const ranked = [...enriched]
      .map((track) => ({
        id: track.id,
        score: (() => {
          const tags = primaryTagsByTrackId.get(track.id) ?? track.tags;
          const overlapBoost = tags.filter((tag) => centroid.includes(tag)).length / centroid.length;
          const audioBoost = cosineNumberScore(audioVectorByTrackId.get(track.id) ?? [], seedAudioVector);
          return cosineScore(tags, centroid) * 0.68 + overlapBoost * 0.2 + audioBoost * 0.12;
        })()
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, playlistPoolTargetSize * 3)
      .map((item) => item.id);
    const pooled = limitTracksByArtist(
      rerankTrackPoolForPlaylist(
        ranked,
        clamp(settings.maxTracksPerPlaylist * 2, 24, playlistPoolTargetSize),
        centroid,
        seedAudioVector,
        trackTagsByTrackId,
        audioVectorByTrackId,
        artistByTrackId,
        playCountByTrackId,
        favoriteByTrackId,
        maxPlayCount
      ),
      artistByTrackId,
      maxArtistPerPlaylist,
      clamp(settings.maxTracksPerPlaylist * 2, 24, playlistPoolTargetSize),
      maxArtistFallbackPerPlaylist
    );
    if (pooled.length < 16) {
      continue;
    }
    const name = buildGeneratedPlaylistName(centroid, seedAudioVector, `${generationKey}:cluster:${primaryTag}`);
    candidates.push({
      name,
      slug: `cluster-${slugify(primaryTag)}`,
      description: buildGeneratedPlaylistDescription(centroid, seedAudioVector, "cluster"),
      trackIds: pooled,
      source: "cluster",
      signatureTags: centroid,
      audioVector: averageAudioVector(pooled, audioVectorByTrackId)
    });
  }

  for (const [moodName, moodTags] of Object.entries(moodRules)) {
    const seed = enriched.filter((track) => track.tags.some((tag) => moodTags.includes(tag)));
    if (seed.length < 8) {
      continue;
    }
    const centroid = uniqueTags(seed.flatMap((track) => track.tags));
    const moodAudioVector = averageAudioVector(
      seed.map((track) => track.id),
      audioVectorByTrackId
    );
    const ranked = [...enriched]
      .map((track) => ({
        id: track.id,
        score: cosineScore(track.tags, centroid) * 0.76 + cosineNumberScore(audioVectorByTrackId.get(track.id) ?? [], moodAudioVector) * 0.24
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, playlistPoolTargetSize * 2)
      .map((item) => item.id);
    const pooled = rerankTrackPoolForPlaylist(
      ranked,
      48,
      uniqueTags(moodTags),
      moodAudioVector,
      trackTagsByTrackId,
      audioVectorByTrackId,
      artistByTrackId,
      playCountByTrackId,
      favoriteByTrackId,
      maxPlayCount
    );
    const name = buildMoodPlaylistName(moodName, uniqueTags(moodTags), averageAudioVector(pooled, audioVectorByTrackId), generationKey);
    candidates.push({
      name,
      slug: `mood-${slugify(moodName)}`,
      description: `${moodName} tracks ranked by tag similarity`,
      trackIds: pooled,
      source: "mood",
      signatureTags: uniqueTags(moodTags),
      audioVector: averageAudioVector(pooled, audioVectorByTrackId)
    });
  }

  const artists = new Map<string, { ids: number[]; tags: string[] }>();
  for (const track of enriched) {
    const key = (track.artist ?? "").trim();
    if (!key) {
      continue;
    }
    const entry = artists.get(key) ?? { ids: [], tags: [] };
    entry.ids.push(track.id);
    entry.tags.push(...track.tags);
    artists.set(key, entry);
  }
  const artistSeeds = [...artists.entries()]
    .filter(([, entry]) => entry.ids.length >= 3)
    .sort((a, b) => b[1].ids.length - a[1].ids.length)
    .slice(0, 4);
  for (const [seedArtist, seedData] of artistSeeds) {
    const seedTags = uniqueTags(seedData.tags.filter((tag) => isPrimaryClusterTag(tag)));
    const seedAudioVector = averageAudioVector(seedData.ids, audioVectorByTrackId);
    const similarTracks = [...enriched]
      .map((track) => ({
        id: track.id,
        score:
          cosineScore(primaryTagsByTrackId.get(track.id) ?? track.tags, seedTags) * 0.78 +
          cosineNumberScore(audioVectorByTrackId.get(track.id) ?? [], seedAudioVector) * 0.22
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, playlistPoolTargetSize * 2)
      .map((item) => item.id);
    const pooled = rerankTrackPoolForPlaylist(
      similarTracks,
      54,
      seedTags,
      seedAudioVector,
      trackTagsByTrackId,
      audioVectorByTrackId,
      artistByTrackId,
      playCountByTrackId,
      favoriteByTrackId,
      maxPlayCount
    );
    const audioVector = averageAudioVector(pooled, audioVectorByTrackId);
    const name = buildArtistPlaylistName(seedArtist, seedTags, audioVector, generationKey);
    candidates.push({
      name,
      slug: `artist-${slugify(seedArtist)}`,
      description: `Artists and tracks sharing signature tags with ${seedArtist}`,
      trackIds: pooled,
      source: "artist",
      signatureTags: seedTags,
      audioVector
    });
  }

  const leastPlayed = db
    .prepare("SELECT id, tags_json, audio_vector_json, play_count FROM tracks ORDER BY play_count ASC, RANDOM() LIMIT 160")
    .all() as Array<{ id: number; tags_json: string; audio_vector_json: string; play_count: number }>;
  if (leastPlayed.length > 24) {
    const favoriteTagsRows = db
      .prepare("SELECT id, tags_json FROM tracks WHERE favorite = 1 ORDER BY play_count DESC LIMIT 30")
      .all() as Array<{ id: number; tags_json: string }>;
    const favoriteTags = uniqueTags(
      favoriteTagsRows.flatMap((row) => {
        try {
          return JSON.parse(row.tags_json) as string[];
        } catch {
          return [];
        }
      })
    );
    const favoriteAudioVector = averageAudioVector(
      favoriteTagsRows.map((row) => row.id),
      audioVectorByTrackId
    );
    const ranked = leastPlayed
      .map((row) => {
        const tags = (() => {
          try {
            return JSON.parse(row.tags_json) as string[];
          } catch {
            return [];
          }
        })();
        const audioVector = audioVectorFromTrack(row);
        const noveltyScore = maxPlayCount > 0 ? 1 - Math.log1p(Math.max(0, row.play_count)) / Math.log1p(maxPlayCount) : 0.5;
        return {
          id: row.id,
          score: cosineScore(tags, favoriteTags) * 0.68 + cosineNumberScore(audioVector, favoriteAudioVector) * 0.2 + noveltyScore * 0.12
        };
      })
      .sort((a, b) => b.score - a.score)
      .slice(0, playlistPoolTargetSize * 2)
      .map((row) => row.id);
    const pooled = rerankTrackPoolForPlaylist(
      ranked,
      48,
      favoriteTags.slice(0, 10),
      favoriteAudioVector,
      trackTagsByTrackId,
      audioVectorByTrackId,
      artistByTrackId,
      playCountByTrackId,
      favoriteByTrackId,
      maxPlayCount
    );
    const audioVector = averageAudioVector(pooled, audioVectorByTrackId);
    candidates.push({
      name: buildDiscoveryPlaylistName(favoriteTags.slice(0, 8), audioVector, generationKey),
      slug: "discovery-mix",
      description: "Least-played tracks with high overlap to favorites",
      trackIds: pooled,
      source: "discovery",
      signatureTags: favoriteTags.slice(0, 8),
      audioVector
    });
  }

  const limitedPlaylistCount = clamp(desiredWeeklyPlaylists, 1, 5) + pinnedSlugs.size;
  const limitedTrackCount = clamp(settings.maxTracksPerPlaylist, 5, 100);
  const minimumTrackCount = Math.min(limitedTrackCount, 12);
  return selectDiverseCandidates(
    candidates
      .map((candidate, index) => ({
        ...candidate,
        slug: candidate.slug || `weekly-${index + 1}`,
        trackIds: [...new Set(candidate.trackIds)].slice(0, playlistPoolTargetSize)
      }))
      .filter((candidate) => !lockedSlugs.has(candidate.slug))
      .filter((candidate) => candidate.trackIds.length >= minimumTrackCount)
      .filter((candidate, index, list) => list.findIndex((entry) => entry.slug === candidate.slug) === index),
    limitedPlaylistCount,
    limitedTrackCount,
    artistByTrackId,
    primaryTagsByTrackId,
    audioVectorByTrackId,
    generationKey,
    pinnedSlugs
  )
    .filter((candidate) => candidate.trackIds.length >= minimumTrackCount)
    .map((candidate, index) => ({
      ...candidate,
      slug: candidate.slug || `weekly-${index + 1}`
    }));
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
      deletePlaylistTracksByPlaylistIdStmt.run(playlistId);
      playlist.trackIds.forEach((trackId, position) => {
        insertPlaylistTrackStmt.run(playlistId, trackId, position + 1);
      });
    }
    const stalePlaylists = db
      .prepare("SELECT id FROM playlists WHERE is_recommended = 0 AND mode = 'dynamic' AND selected = 0")
      .all() as Array<{ id: number }>;
    for (const stale of stalePlaylists) {
      deletePlaylistByIdStmt.run(stale.id);
    }
  });
  transaction(candidates);
}

function managedSubsonicPlaylistName(playlist: PlaylistRecord): string {
  return playlist.name;
}

async function syncSubsonicPlaylists(connection: SubsonicConnection) {
  const managedPlaylists = db
    .prepare(
      `SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at
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
    const targetName = managedSubsonicPlaylistName(playlist);
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
      const removeParams: Record<string, string> = { playlistId: remoteId };
      removeParams.songIndexToRemove = entries.map((_, index) => String(entries.length - 1 - index)).join(",");
      await subsonicRequest(connection, "updatePlaylist.view", removeParams);
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
    applied += 1;
  }
  const managedNames = new Set(managedPlaylists.map((playlist) => managedSubsonicPlaylistName(playlist)));
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
  const candidates = await generateCandidates(syncableTracks, weeklyPlaylistCount, generationKey, pinnedSlugs, lockedSlugs);
  replacePlaylists(candidates);
  appendLog({
    level: "info",
    scope: "playlist",
    message: "Recommended playlists refreshed",
    meta: { generated: candidates.length, sourceTracks: syncableTracks.length, weeklyPlaylistCount, maxTracksPerPlaylist: settings.maxTracksPerPlaylist }
  });
  return { candidates, sourceTracks: syncableTracks.length };
}

function nextWeekStartIso(now = new Date()): string {
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + (8 - day));
  date.setUTCHours(0, 0, 0, 0);
  return date.toISOString();
}

function setWorkerScheduleHints() {
  workerState.nextRunAt = workerState.running ? nextWeekStartIso() : null;
}

async function runWorkerCycle(trigger: "start" | "weekly"): Promise<void> {
  if (!workerConnection) {
    throw new Error("Worker is missing Navidrome connection settings");
  }
  if (workerState.cycleRunning) {
    return;
  }
  workerState.cycleRunning = true;
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
  } finally {
    workerState.cycleRunning = false;
  }
}

async function tickWorker() {
  if (!workerState.running || workerState.cycleRunning) {
    return;
  }
  const currentWeek = isoWeekKey();
  if (workerState.lastRunWeek === currentWeek) {
    return;
  }
  await runWorkerCycle("weekly");
}

function startWorkerScheduler() {
  if (workerTimer) {
    clearInterval(workerTimer);
  }
  workerTimer = setInterval(() => {
    void tickWorker();
  }, workerTickMs);
}

function stopWorkerScheduler() {
  if (workerTimer) {
    clearInterval(workerTimer);
    workerTimer = null;
  }
  workerState.running = false;
  workerState.cycleRunning = false;
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
  void tickWorker().catch((error) => {
    workerState.error = error instanceof Error ? error.message : "Worker resume tick failed";
    persistSettings();
    appendLog({
      level: "error",
      scope: "system",
      message: "Worker resume tick failed",
      meta: { error: workerState.error }
    });
  });
}

function workerSnapshot() {
  return {
    running: workerState.running,
    cycleRunning: workerState.cycleRunning,
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
    maxTracksPerPlaylist: settings.maxTracksPerPlaylist
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
    settings.weeklyPlaylistCount = clamp(Number(body.weeklyPlaylistCount), 1, 5);
    workerState.weeklyPlaylistCount = settings.weeklyPlaylistCount;
  }
  if (body.maxTracksPerPlaylist !== undefined) {
    settings.maxTracksPerPlaylist = clamp(Number(body.maxTracksPerPlaylist), 5, 100);
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
    connection = getSubsonicConnection(req.body);
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : "Invalid Subsonic connection" });
    return;
  }
  const requestedCount = Number(req.body?.weeklyPlaylistCount ?? workerState.weeklyPlaylistCount);
  workerConnection = connection;
  workerState.weeklyPlaylistCount = clamp(requestedCount, 1, 5);
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
    const connection = getSubsonicConnection(req.body);
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
    connection = getSubsonicConnection(req.body);
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
  appendLog({
    level: "info",
    scope: "scan",
    message: "Scan job created",
    meta: { id, source: connection.baseUrl }
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
  const count = clamp(requestedCount, 1, 5);
  const { candidates } = await refreshRecommendedPlaylists(count, randomUUID());
  res.json({ generated: candidates.length, playlists: candidates.map((item) => ({ name: item.name, tracks: item.trackIds.length })) });
});

app.get("/api/playlists", (_req, res) => {
  const playlists = db
    .prepare(
      `SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at
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
      tracks
    };
  });
  res.json(response);
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
    const connection = getSubsonicConnection(req.body);
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

app.post("/api/playlists/sync", async (req, res) => {
  try {
    const connection = getSubsonicConnection(req.body);
    const result = await syncSubsonicPlaylists(connection);
    appendLog({
      level: "info",
      scope: "playlist",
      message: "Selected playlists synced (legacy endpoint)",
      meta: { subsonic: connection.baseUrl, ...result }
    });
    res.json(result);
  } catch (error) {
    appendLog({
      level: "error",
      scope: "playlist",
      message: "Legacy playlist sync failed",
      meta: { error: error instanceof Error ? error.message : "Unknown error" }
    });
    res.status(500).json({ error: error instanceof Error ? error.message : "Failed syncing playlists" });
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

resumeWorkerSchedulerFromPersistence();

app.listen(port, () => {
  console.log(`Sortify backend listening on http://localhost:${port}`);
});
