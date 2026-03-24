import cors from "cors";
import Database from "better-sqlite3";
import { config } from "dotenv";
import express from "express";
import { createHash, randomUUID } from "node:crypto";
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
};

type PlaylistRecord = {
  id: number;
  slug: string;
  name: string;
  description: string | null;
  mode: "live" | "frozen";
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
};

type AiPlaylistPlan = {
  name: string;
  description: string;
  focusTags: string[];
  avoidTags: string[];
  targetSize: number;
  maxPerArtist: number;
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
  geminiApiKey: string;
  geminiModel: string;
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
const defaultGeminiApiKey = process.env.GEMINI_API_KEY ?? "";
const defaultGeminiModel = process.env.GEMINI_MODEL ?? "gemini-flash-lite-latest";
const defaultMaxTracksPerPlaylist = Math.max(5, Math.min(100, Number(process.env.MAX_TRACKS_PER_PLAYLIST ?? 20)));
const scanJobs = new Map<string, ScanProgress>();
const operationLogs: OperationLogEntry[] = [];
let activeScanJobId: string | null = null;
let workerConnection: SubsonicConnection | null = null;
let workerTimer: NodeJS.Timeout | null = null;
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
  geminiApiKey: defaultGeminiApiKey,
  geminiModel: defaultGeminiModel,
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

function normalizeGeminiModel(value: string): string {
  const normalized = value.trim().replace(/^models\//, "");
  return normalized || defaultGeminiModel;
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
db.exec("UPDATE playlists SET sync_target = 'subsonic' WHERE sync_target = 'm3u'");
db.exec("UPDATE playlists SET updated_at = CASE WHEN updated_at = '' THEN created_at ELSE updated_at END");

app.use(cors());
app.use(express.json({ limit: "2mb" }));

const readTracksStmt = db.prepare(`
SELECT id, path, title, artist, album, year, duration, tags_json
FROM tracks
ORDER BY last_scanned_at DESC, id DESC
LIMIT ?
`);

const upsertTrackStmt = db.prepare(`
INSERT INTO tracks (path, title, artist, album, album_artist, year, duration, tags_json, last_scanned_at)
VALUES (@path, @title, @artist, @album, @album_artist, @year, @duration, @tags_json, @last_scanned_at)
ON CONFLICT(path) DO UPDATE SET
  title = excluded.title,
  artist = excluded.artist,
  album = excluded.album,
  album_artist = excluded.album_artist,
  year = excluded.year,
  duration = excluded.duration,
  tags_json = excluded.tags_json,
  last_scanned_at = excluded.last_scanned_at
`);
const readTrackScanStateStmt = db.prepare(`
SELECT path, title, artist, album, year, duration, tags_json
FROM tracks
`);

const markPlaylistsNotRecommendedStmt = db.prepare("UPDATE playlists SET is_recommended = 0");
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
  settings.navidromeUrl = read("navidromeUrl") ?? settings.navidromeUrl;
  settings.navidromeUsername = read("navidromeUsername") ?? settings.navidromeUsername;
  settings.navidromePassword = read("navidromePassword") ?? settings.navidromePassword;
  settings.lastFmApiKey = read("lastFmApiKey") ?? settings.lastFmApiKey;
  settings.geminiApiKey = read("geminiApiKey") ?? settings.geminiApiKey;
  settings.geminiModel = normalizeGeminiModel(read("geminiModel") ?? settings.geminiModel);
  const weekly = Number.parseInt(read("weeklyPlaylistCount") ?? "", 10);
  if (!Number.isNaN(weekly)) {
    settings.weeklyPlaylistCount = clamp(weekly, 1, 5);
  }
  const maxTracks = Number.parseInt(read("maxTracksPerPlaylist") ?? "", 10);
  if (!Number.isNaN(maxTracks)) {
    settings.maxTracksPerPlaylist = clamp(maxTracks, 5, 100);
  }
  workerState.weeklyPlaylistCount = settings.weeklyPlaylistCount;
}

function persistSettings() {
  const transaction = db.transaction(() => {
    upsertSettingStmt.run("navidromeUrl", settings.navidromeUrl);
    upsertSettingStmt.run("navidromeUsername", settings.navidromeUsername);
    upsertSettingStmt.run("navidromePassword", settings.navidromePassword);
    upsertSettingStmt.run("lastFmApiKey", settings.lastFmApiKey);
    upsertSettingStmt.run("geminiApiKey", settings.geminiApiKey);
    upsertSettingStmt.run("geminiModel", settings.geminiModel);
    upsertSettingStmt.run("weeklyPlaylistCount", String(settings.weeklyPlaylistCount));
    upsertSettingStmt.run("maxTracksPerPlaylist", String(settings.maxTracksPerPlaylist));
  });
  transaction();
}

loadPersistedSettings();

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

function sanitizeAiPlaylistPlan(input: unknown): AiPlaylistPlan | null {
  if (!input || typeof input !== "object") {
    return null;
  }
  const value = input as Record<string, unknown>;
  const name = String(value.name ?? "").trim();
  const description = String(value.description ?? "").trim();
  const focusTagsRaw = Array.isArray(value.focusTags) ? value.focusTags : [];
  const avoidTagsRaw = Array.isArray(value.avoidTags) ? value.avoidTags : [];
  const focusTags = uniqueTags(focusTagsRaw.map((item) => String(item ?? "")).filter(Boolean)).filter((tag) => isPrimaryClusterTag(tag)).slice(0, 8);
  const avoidTags = uniqueTags(avoidTagsRaw.map((item) => String(item ?? "")).filter(Boolean)).slice(0, 8);
  const targetSize = clamp(Number(value.targetSize ?? settings.maxTracksPerPlaylist * 2), 12, playlistPoolTargetSize);
  const maxPerArtist = clamp(Number(value.maxPerArtist ?? maxArtistPerPlaylist), 1, 4);
  if (!name || !description || focusTags.length < 2) {
    return null;
  }
  return { name, description, focusTags, avoidTags, targetSize, maxPerArtist };
}

async function fetchGeminiPlaylistPlans(tracks: Array<{ title: string; artist: string; year: number | null; tags: string[] }>): Promise<AiPlaylistPlan[]> {
  if (!settings.geminiApiKey) {
    return [];
  }
  const sampled = tracks.slice(0, 140).map((track) => ({
    title: track.title,
    artist: track.artist,
    year: track.year,
    tags: track.tags.slice(0, 8)
  }));
  const tagHistogram = new Map<string, number>();
  for (const track of sampled) {
    for (const tag of track.tags) {
      if (!isPrimaryClusterTag(tag)) {
        continue;
      }
      tagHistogram.set(tag, (tagHistogram.get(tag) ?? 0) + 1);
    }
  }
  const topTags = [...tagHistogram.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 36)
    .map(([tag, count]) => ({ tag, count }));

  const prompt = JSON.stringify(
    {
      task: "Create low-cost weekly music playlist plans from track tags",
      constraints: {
        count: 8,
        targetSizeRange: [36, 72],
        maxPerArtistRange: [1, 2],
        requireMultiTagOverlap: true,
        avoidYearCentricGrouping: true,
        namingStyle:
          "Use evocative, emotionally descriptive names grounded in the actual musical vibe and tags. Avoid plain genre-only names and avoid random nonsense."
      },
      output: {
        format: "json",
        schema: {
          plans: [{ name: "string", description: "string", focusTags: ["string"], avoidTags: ["string"], targetSize: "number", maxPerArtist: "number" }]
        }
      },
      topTags,
      sampleTracks: sampled
    },
    null,
    2
  );

  const preferredModels = [
    normalizeGeminiModel(settings.geminiModel),
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-flash-lite-latest",
    "gemini-flash-latest",
    "gemini-3.1-flash-lite-preview",
    "gemini-3-flash-preview",
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite-001",
    "gemini-2.0-flash-001"
  ];
  const apiVersions = ["v1beta", "v1"];
  const listedModelsByVersion = new Map<string, string[]>();
  for (const apiVersion of apiVersions) {
    try {
      const listResponse = await fetch(
        `https://generativelanguage.googleapis.com/${apiVersion}/models?key=${encodeURIComponent(settings.geminiApiKey)}`,
        { signal: AbortSignal.timeout(8000) }
      );
      if (!listResponse.ok) {
        continue;
      }
      const listPayload = (await listResponse.json()) as {
        models?: Array<{ name?: string; supportedGenerationMethods?: string[] }>;
      };
      const available = (listPayload.models ?? [])
        .filter((model) => (model.supportedGenerationMethods ?? []).includes("generateContent"))
        .map((model) => model.name ?? "")
        .filter(Boolean)
        .map((name) => name.replace(/^models\//, ""));
      listedModelsByVersion.set(apiVersion, available);
    } catch {
      continue;
    }
  }
  const defaultListed = [...new Set(preferredModels)];
  let lastError = "Unknown Gemini failure";
  for (const apiVersion of apiVersions) {
    const listed = listedModelsByVersion.get(apiVersion) ?? defaultListed;
    const orderedModels = [
      ...preferredModels.filter((model, index) => preferredModels.indexOf(model) === index && listed.includes(model)),
      ...listed.filter((model) => !preferredModels.includes(model))
    ];
    for (const model of orderedModels) {
      for (const useJsonMime of [true, false]) {
        const response = await fetch(
          `https://generativelanguage.googleapis.com/${apiVersion}/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(settings.geminiApiKey)}`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            signal: AbortSignal.timeout(12000),
            body: JSON.stringify({
              generationConfig: {
                temperature: 0.3,
                topP: 0.8,
                ...(useJsonMime ? { responseMimeType: "application/json" } : {})
              },
              contents: [{ role: "user", parts: [{ text: prompt }] }]
            })
          }
        );
        const payload = (await response.json()) as {
          error?: { message?: string };
          candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }>;
        };
        if (!response.ok) {
          lastError = payload.error?.message ?? `Gemini request failed (${response.status})`;
          continue;
        }
        const rawText = payload.candidates?.[0]?.content?.parts?.map((part) => part.text ?? "").join("\n") ?? "";
        if (!rawText.trim()) {
          lastError = "Gemini returned empty content";
          continue;
        }
        const cleanedText = rawText
          .replace(/^```json\s*/i, "")
          .replace(/^```\s*/i, "")
          .replace(/\s*```$/i, "")
          .trim();
        try {
          const parsed = JSON.parse(cleanedText) as { plans?: unknown[] };
          const plans = (parsed.plans ?? [])
            .map((item) => sanitizeAiPlaylistPlan(item))
            .filter((item): item is AiPlaylistPlan => Boolean(item));
          if (plans.length) {
            if (model !== settings.geminiModel || apiVersion !== "v1beta") {
              appendLog({
                level: "info",
                scope: "playlist",
                message: "Gemini fallback path used for playlist planning",
                meta: { requestedModel: settings.geminiModel, activeModel: model, apiVersion, jsonMime: useJsonMime }
              });
            }
            return plans.slice(0, 10);
          }
          lastError = "Gemini response had no valid plans";
        } catch (error) {
          lastError = error instanceof Error ? error.message : "Failed to parse Gemini response";
        }
      }
    }
  }
  throw new Error(lastError);
}

async function generateAiPlaylistPlans(tracks: Array<{ id: number; title: string; artist: string; year: number | null; tags: string[] }>): Promise<AiPlaylistPlan[]> {
  if (!settings.geminiApiKey) {
    throw new Error("GEMINI_API_KEY is required for AI-only playlist generation");
  }
  const shuffled = [...tracks].sort(() => Math.random() - 0.5);
  try {
    const geminiPlans = await fetchGeminiPlaylistPlans(shuffled);
    if (geminiPlans.length) {
      return geminiPlans;
    }
    throw new Error("Gemini returned no valid playlist plans");
  } catch (error) {
    throw new Error(error instanceof Error ? error.message : "Gemini playlist planning failed", { cause: error });
  }
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

async function fetchLastFmTags(artist: string, title: string, album?: string | null): Promise<string[]> {
  if (!artist || !title) {
    return [];
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
  return uniqueTags([...trackTags, ...artistTags, ...albumTags]);
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

async function fetchMusicBrainzTags(artist: string, title: string): Promise<{ tags: string[]; year: number | null }> {
  if (!artist || !title) {
    return { tags: [], year: null };
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
      return { tags: [], year: null };
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
      return { tags: [], year: null };
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
      tags: uniqueTags([...recordingTags, ...artistTags]),
      year: Number.isNaN(year) ? null : year
    };
  } catch {
    return { tags: [], year: null };
  }
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
  const salt = randomUUID().replace(/-/g, "").slice(0, 12);
  const token = createHash("md5").update(`${connection.password}${salt}`).digest("hex");
  const query = new URLSearchParams({
    u: connection.username,
    t: token,
    s: salt,
    v: "1.16.1",
    c: "sortify",
    f: "json",
    ...params
  });
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
  onProgress?: (progress: Partial<Pick<ScanProgress, "scannedFiles" | "processedFiles" | "updatedTracks" | "errors">>) => void
) {
  const discovered = await fetchSubsonicSongs(connection);
  const existingRows = readTrackScanStateStmt.all() as Array<{
    path: string;
    title: string | null;
    artist: string | null;
    album: string | null;
    year: number | null;
    duration: number | null;
    tags_json: string;
  }>;
  const existingByPath = new Map<string, (typeof existingRows)[number]>();
  for (const row of existingRows) {
    existingByPath.set(row.path, row);
  }
  const now = new Date().toISOString();
  let updated = 0;
  let processed = 0;
  let skipped = 0;
  let errors = 0;
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
    const existingTags = existing ? tagsFromTrack({ ...existing, id: 0, path: trackPath, tags_json: existing.tags_json }) : [];
    const unchanged =
      existing &&
      (existing.title ?? "Unknown Title") === title &&
      (existing.artist ?? "Unknown Artist") === artist &&
      (existing.album ?? "Unknown Album") === album &&
      (existing.duration ?? null) === duration;
    const shouldHydrate = !unchanged || existingTags.length < 6;
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
      const baseTags = deriveBaseTags(title, artist, mergedYear, song.genre ? [song.genre] : [], null);
      const lastFmTags = await fetchLastFmTags(artist, title, album);
      const tags = uniqueTags([...baseTags, ...musicBrainz.tags, ...lastFmTags]);
      upsertTrackStmt.run({
        path: trackPath,
        title,
        artist,
        album,
        album_artist: artist,
        year: mergedYear,
        duration,
        tags_json: JSON.stringify(tags),
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
  appendLog({
    level: "info",
    scope: "scan",
    message: "Scan completed",
    meta: { source: connection.baseUrl, scannedFiles: discovered.length, processedFiles: processed, updatedTracks: updated, skippedTracks: skipped, errors }
  });
  return { scannedFiles: discovered.length, updatedTracks: updated, processedFiles: processed, errors };
}

function tagsFromTrack(track: TrackRecord): string[] {
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

function slugify(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
}

async function generateCandidates(tracks: TrackRecord[], desiredWeeklyPlaylists: number): Promise<PlaylistCandidate[]> {
  const enriched = tracks.map((track) => ({
    ...track,
    tags: tagsFromTrack(track)
  }));
  const candidates: PlaylistCandidate[] = [];
  const artistByTrackId = new Map<number, string>();
  const primaryTagsByTrackId = new Map<number, string[]>();

  for (const track of enriched) {
    artistByTrackId.set(track.id, track.artist ? normalizeTag(track.artist) : `track-${track.id}`);
    const primaryTags = uniqueTags(track.tags.filter((tag) => isPrimaryClusterTag(tag))).slice(0, 10);
    primaryTagsByTrackId.set(track.id, primaryTags);
  }

  const aiPlans = await generateAiPlaylistPlans(
    enriched.map((track) => ({
      id: track.id,
      title: track.title ?? "Unknown Title",
      artist: track.artist ?? "Unknown Artist",
      year: track.year ?? null,
      tags: primaryTagsByTrackId.get(track.id) ?? track.tags
    }))
  );

  for (const plan of aiPlans) {
    const centroid = uniqueTags(plan.focusTags.filter((tag) => isPrimaryClusterTag(tag)));
    if (centroid.length < 2) {
      continue;
    }
    const ranked = [...enriched]
      .map((track) => ({
        id: track.id,
        score: (() => {
          const tags = primaryTagsByTrackId.get(track.id) ?? track.tags;
          const overlapBoost = tags.filter((tag) => centroid.includes(tag)).length / centroid.length;
          const avoidPenalty = plan.avoidTags.some((tag) => tags.includes(tag)) ? 0.6 : 1;
          return (cosineScore(tags, centroid) * 0.8 + overlapBoost * 0.2) * avoidPenalty;
        })()
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, playlistPoolTargetSize * 3)
      .map((item) => item.id);
    const pooled = limitTracksByArtist(ranked, artistByTrackId, maxArtistPerPlaylist, plan.targetSize, maxArtistFallbackPerPlaylist);
    candidates.push({
      name: plan.name,
      slug: slugify(plan.name),
      description: plan.description,
      trackIds: pooled
    });
  }

  for (const [moodName, moodTags] of Object.entries(moodRules)) {
    const seed = enriched.filter((track) => track.tags.some((tag) => moodTags.includes(tag)));
    if (seed.length < 8) {
      continue;
    }
    const centroid = uniqueTags(seed.flatMap((track) => track.tags));
    const ranked = [...enriched]
      .map((track) => ({
        id: track.id,
        score: cosineScore(track.tags, centroid)
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, playlistPoolTargetSize * 2)
      .map((item) => item.id);
    const pooled = limitTracksByArtist(ranked, artistByTrackId, maxArtistPerPlaylist, 48, maxArtistFallbackPerPlaylist);
    const name = `${moodName} Flow`;
    candidates.push({
      name,
      slug: slugify(name),
      description: `${moodName} tracks ranked by tag similarity`,
      trackIds: pooled
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
    const similarTracks = [...enriched]
      .map((track) => ({
        id: track.id,
        score: cosineScore(primaryTagsByTrackId.get(track.id) ?? track.tags, seedTags)
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, playlistPoolTargetSize * 2)
      .map((item) => item.id);
    const pooled = limitTracksByArtist(similarTracks, artistByTrackId, maxArtistPerPlaylist, 54, maxArtistFallbackPerPlaylist);
    const name = `${seedArtist} Constellation`;
    candidates.push({
      name,
      slug: slugify(name),
      description: `Artists and tracks sharing signature tags with ${seedArtist}`,
      trackIds: pooled
    });
  }

  const leastPlayed = db
    .prepare("SELECT id, tags_json FROM tracks ORDER BY play_count ASC, RANDOM() LIMIT 120")
    .all() as Array<{ id: number; tags_json: string }>;
  if (leastPlayed.length > 24) {
    const favoriteTagsRows = db
      .prepare("SELECT tags_json FROM tracks WHERE favorite = 1 ORDER BY play_count DESC LIMIT 30")
      .all() as Array<{ tags_json: string }>;
    const favoriteTags = uniqueTags(
      favoriteTagsRows.flatMap((row) => {
        try {
          return JSON.parse(row.tags_json) as string[];
        } catch {
          return [];
        }
      })
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
        return { id: row.id, score: cosineScore(tags, favoriteTags) };
      })
      .sort((a, b) => b.score - a.score)
      .slice(0, playlistPoolTargetSize * 2)
      .map((row) => row.id);
    const pooled = limitTracksByArtist(ranked, artistByTrackId, maxArtistPerPlaylist, 48, maxArtistFallbackPerPlaylist);
    candidates.push({
      name: "Discovery Mix",
      slug: "discovery-mix",
      description: "Least-played tracks with high overlap to favorites",
      trackIds: pooled
    });
  }

  const limitedPlaylistCount = clamp(desiredWeeklyPlaylists, 1, 5);
  const limitedTrackCount = clamp(settings.maxTracksPerPlaylist, 5, 100);
  const minimumTrackCount = Math.min(limitedTrackCount, 12);
  return candidates
    .map((candidate, index) => ({
      ...candidate,
      slug: candidate.slug || `weekly-${index + 1}`,
      trackIds: limitTracksByArtist(
        [...new Set(candidate.trackIds)],
        artistByTrackId,
        maxArtistPerPlaylist,
        limitedTrackCount,
        maxArtistFallbackPerPlaylist
      )
    }))
    .filter((candidate) => candidate.trackIds.length >= minimumTrackCount)
    .filter((candidate, index, list) => list.findIndex((entry) => entry.slug === candidate.slug) === index)
    .slice(0, limitedPlaylistCount);
}

function replacePlaylists(candidates: PlaylistCandidate[]) {
  const now = new Date().toISOString();
  const transaction = db.transaction((list: PlaylistCandidate[]) => {
    markPlaylistsNotRecommendedStmt.run();
    for (const playlist of list) {
      const existing = findPlaylistBySlugStmt.get(playlist.slug) as PlaylistRecord | undefined;
      let playlistId = 0;
      if (existing) {
        playlistId = existing.id;
        updatePlaylistMetadataStmt.run({
          id: existing.id,
          name: playlist.name,
          description: playlist.description,
          updated_at: now,
          is_recommended: 1
        });
      } else {
        const result = insertPlaylistStmt.run({
          slug: playlist.slug,
          name: playlist.name,
          description: playlist.description,
          created_at: now,
          updated_at: now,
          mode: "live",
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
      .prepare("SELECT id FROM playlists WHERE is_recommended = 0 AND selected = 0")
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
    .prepare("SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at FROM playlists WHERE is_recommended = 1 ORDER BY name")
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
  for (const playlist of managedPlaylists) {
    const trackRows = db
      .prepare(
        `SELECT pt.track_id as trackId, t.path, t.artist FROM playlist_tracks pt
         JOIN tracks t ON t.id = pt.track_id
         WHERE pt.playlist_id = ?
         ORDER BY pt.position`
      )
      .all(playlist.id) as Array<{ trackId: number; path: string; artist: string | null }>;
    const weeklyTrackIds = weeklyRotatedTrackIds(trackRows, `${playlist.slug}:${weekKey}`, clamp(settings.maxTracksPerPlaylist, 5, 100));
    const pathByTrackId = new Map<number, string>();
    for (const row of trackRows) {
      pathByTrackId.set(row.trackId, row.path);
    }
    const songIds = weeklyTrackIds
      .map((trackId) => pathByTrackId.get(trackId) ?? "")
      .map((pathValue) => extractSubsonicSongId(pathValue))
      .filter((value): value is string => Boolean(value));
    const targetName = managedSubsonicPlaylistName(playlist);
    let remoteId = remoteByName.get(targetName) ?? "";
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
  for (const remote of remoteList.playlists?.playlist ?? []) {
    const isLegacyManagedPlaylist = remote.name.startsWith("Sortify - ");
    const isCurrentManagedPlaylist = managedNames.has(remote.name);
    if (!isLegacyManagedPlaylist && !isCurrentManagedPlaylist) {
      continue;
    }
    if (managedNames.has(remote.name)) {
      continue;
    }
    await subsonicRequest(connection, "deletePlaylist.view", { id: remote.id });
    removed += 1;
  }
  return { applied, removed, total: managedPlaylists.length };
}

async function refreshRecommendedPlaylists(weeklyPlaylistCount = workerState.weeklyPlaylistCount) {
  const tracks = db.prepare("SELECT id, path, title, artist, album, year, duration, tags_json FROM tracks").all() as TrackRecord[];
  const syncableTracks = tracks.filter((track) => Boolean(extractSubsonicSongId(track.path)));
  const candidates = await generateCandidates(syncableTracks, weeklyPlaylistCount);
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
    appendLog({
      level: "info",
      scope: "system",
      message: "Worker cycle completed",
      meta: { trigger, lastRunAt: workerState.lastRunAt, week: workerState.lastRunWeek }
    });
  } catch (error) {
    workerState.error = error instanceof Error ? error.message : "Worker cycle failed";
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
    navidromePassword: settings.navidromePassword,
    lastFmApiKey: settings.lastFmApiKey,
    geminiApiKey: settings.geminiApiKey,
    geminiModel: settings.geminiModel,
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
  if (body.geminiApiKey !== undefined) {
    settings.geminiApiKey = String(body.geminiApiKey ?? "").trim();
  }
  if (body.geminiModel !== undefined) {
    settings.geminiModel = normalizeGeminiModel(String(body.geminiModel ?? ""));
  }
  if (body.weeklyPlaylistCount !== undefined) {
    settings.weeklyPlaylistCount = clamp(Number(body.weeklyPlaylistCount), 1, 5);
    workerState.weeklyPlaylistCount = settings.weeklyPlaylistCount;
  }
  if (body.maxTracksPerPlaylist !== undefined) {
    settings.maxTracksPerPlaylist = clamp(Number(body.maxTracksPerPlaylist), 5, 100);
  }
  if (workerConnection && settings.navidromeUrl && settings.navidromeUsername && settings.navidromePassword) {
    workerConnection = {
      baseUrl: settings.navidromeUrl,
      username: settings.navidromeUsername,
      password: settings.navidromePassword
    };
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
  res.json({ tracks: trackCount.count, playlists: playlistCount.count });
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
      tags: tagsFromTrack(row)
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
  persistSettings();
  workerState.running = true;
  workerState.error = null;
  setWorkerScheduleHints();
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
    const result = await scanLibrary(connection);
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
      const result = await scanLibrary(connection, (progress) => {
        const active = scanJobs.get(id);
        if (!active) {
          return;
        }
        Object.assign(active, progress);
        scanJobs.set(id, active);
      });
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
  const { candidates } = await refreshRecommendedPlaylists(count);
  res.json({ generated: candidates.length, playlists: candidates.map((item) => ({ name: item.name, tracks: item.trackIds.length })) });
});

app.get("/api/playlists", (_req, res) => {
  const playlists = db
    .prepare(
      "SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at FROM playlists WHERE is_recommended = 1 ORDER BY name ASC"
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
    .prepare("SELECT id, mode, selected, sync_target FROM playlists WHERE id = ?")
    .get(id) as { id: number; mode: "live" | "frozen"; selected: number; sync_target: "subsonic" } | undefined;
  if (!existing) {
    res.status(404).json({ error: "Playlist not found" });
    return;
  }
  const modeInput = req.body?.mode;
  const selectedInput = req.body?.selected;
  const syncTargetInput = req.body?.syncTarget;
  const selected = typeof selectedInput === "boolean" ? (selectedInput ? 1 : 0) : existing.selected;
  let mode: "live" | "frozen" = modeInput === "frozen" ? "frozen" : modeInput === "live" ? "live" : existing.mode;
  if (selected === 1) {
    mode = "live";
  }
  const syncTarget: "subsonic" = syncTargetInput === "subsonic" || !syncTargetInput ? "subsonic" : existing.sync_target;
  db.prepare("UPDATE playlists SET mode = ?, selected = ?, sync_target = ?, updated_at = ? WHERE id = ?").run(
    mode,
    selected,
    syncTarget,
    new Date().toISOString(),
    id
  );
  if (existing.mode === "frozen" && mode === "live") {
    await refreshRecommendedPlaylists();
  }
  appendLog({
    level: "info",
    scope: "playlist",
    message: "Playlist settings updated",
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

app.listen(port, () => {
  console.log(`Sortify backend listening on http://localhost:${port}`);
});
