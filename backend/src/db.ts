import Database from "better-sqlite3";
import { dbFile } from "./config.js";
import { log } from "./state.js";

export const db = new Database(dbFile);
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
  fewest_listens_priority INTEGER NOT NULL DEFAULT 0,
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
if (!hasColumn("filter_json")) {
  db.exec("ALTER TABLE playlists ADD COLUMN filter_json TEXT NOT NULL DEFAULT ''");
}
if (!hasColumn("seeds_json")) {
  db.exec("ALTER TABLE playlists ADD COLUMN seeds_json TEXT NOT NULL DEFAULT ''");
}
if (!hasColumn("fewest_listens_priority")) {
  db.exec("ALTER TABLE playlists ADD COLUMN fewest_listens_priority INTEGER NOT NULL DEFAULT 0");
}
db.exec("UPDATE playlists SET mode = 'pinned' WHERE mode = 'locked'");
db.exec("UPDATE playlists SET mode = 'pinned' WHERE mode = 'frozen'");
db.exec("UPDATE playlists SET mode = 'dynamic' WHERE mode NOT IN ('dynamic', 'pinned')");
db.exec("UPDATE playlists SET updated_at = CASE WHEN updated_at = '' THEN created_at ELSE updated_at END");
log.info("database initialized", { dbFile: dbFile });

export const readTracksStmt = db.prepare(`
SELECT id, path, title, artist, album, year, duration, tags_json, audio_features_json, audio_vector_json, analysis_version, analysis_updated_at
FROM tracks
ORDER BY last_scanned_at DESC, id DESC
LIMIT ?
`);

export const upsertTrackStmt = db.prepare(`
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
export const updateTrackPlayStatsStmt = db.prepare("UPDATE tracks SET play_count = ?, favorite = ?, last_scanned_at = ? WHERE path = ?");
export const readTrackScanStateStmt = db.prepare(`
SELECT path, title, artist, album, year, duration, tags_json, audio_features_json, audio_vector_json, analysis_version, analysis_updated_at, play_count, favorite
FROM tracks
`);
export const deleteTrackByPathStmt = db.prepare("DELETE FROM tracks WHERE path = ?");

export const markPlaylistsNotRecommendedStmt = db.prepare("UPDATE playlists SET is_recommended = 0 WHERE mode = 'dynamic'");
export const findPlaylistBySlugStmt = db.prepare(
  `SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at,
          artwork_path, artwork_attribution, artwork_attribution_url, artwork_signature, artwork_updated_at
   FROM playlists WHERE slug = ?`
);
export const insertPlaylistStmt = db.prepare(`
INSERT INTO playlists (slug, name, description, created_at, updated_at, mode, selected, sync_target, is_recommended)
VALUES (@slug, @name, @description, @created_at, @updated_at, @mode, @selected, @sync_target, @is_recommended)
`);
export const updatePlaylistMetadataStmt = db.prepare(`
UPDATE playlists
SET name = @name,
    description = @description,
    updated_at = @updated_at,
    is_recommended = @is_recommended
WHERE id = @id
`);
export const refreshProtectedPlaylistStmt = db.prepare(`
UPDATE playlists
SET updated_at = @updated_at,
    is_recommended = 1
WHERE id = @id
`);
export const refreshProtectedPlaylistDescriptionStmt = db.prepare(`
UPDATE playlists
SET description = @description,
    updated_at = @updated_at,
    is_recommended = 1
WHERE id = @id
`);
export const updatePlaylistArtworkStmt = db.prepare(`
UPDATE playlists
SET artwork_path = @artwork_path,
    artwork_attribution = @artwork_attribution,
    artwork_attribution_url = @artwork_attribution_url,
    artwork_signature = @artwork_signature,
    artwork_updated_at = @artwork_updated_at
WHERE id = @id
`);
export const deletePlaylistByIdStmt = db.prepare("DELETE FROM playlists WHERE id = ?");
export const deletePlaylistTracksByPlaylistIdStmt = db.prepare("DELETE FROM playlist_tracks WHERE playlist_id = ?");
export const insertPlaylistTrackStmt = db.prepare(`
INSERT INTO playlist_tracks (playlist_id, track_id, position)
VALUES (?, ?, ?)
`);
export const insertPlaylistSnapshotStmt = db.prepare(`
INSERT INTO playlist_snapshots (playlist_id, slug, name, track_ids_json, snapshot_week, created_at)
VALUES (?, ?, ?, ?, ?, ?)
`);
export const readPlaylistTrackIdsStmt = db.prepare(`
SELECT track_id FROM playlist_tracks WHERE playlist_id = ? ORDER BY position
`);
export const readAllSettingsStmt = db.prepare("SELECT key, value FROM app_settings");
export const upsertSettingStmt = db.prepare(`
INSERT INTO app_settings (key, value)
VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`);
