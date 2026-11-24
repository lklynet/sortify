import express from "express";
import { db, readTracksStmt } from "../db.js";
import { appState, log } from "../state.js";
import { audioAnalysisVersion } from "../config.js";
import { buildTrackFilterWhere, syncablePathClause } from "../track-filter.js";
import { audioFeaturesFromTrack } from "../scan.js";
import { tagsFromTrack, audioVectorFromTrack } from "../playlist-generator.js";
import type { TrackRecord, AudioAnalysis } from "../types.js";

const router = express.Router();

router.get("/api/tracks", (req, res) => {
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

router.get("/api/tracks/all", (_req, res) => {
  const rows = db
    .prepare(
      `SELECT id, path, title, artist, album, year, duration, tags_json, audio_features_json, audio_vector_json, analysis_version, analysis_updated_at
       FROM tracks
       ORDER BY last_scanned_at DESC, id DESC`
    )
    .all() as TrackRecord[];
  res.json(
    rows.map((row) => ({
      ...row,
      tags: tagsFromTrack(row),
      audioFeatures: audioFeaturesFromTrack(row),
      audioVector: audioVectorFromTrack(row)
    }))
  );
});

router.get("/api/tracks/search", (req, res) => {
  const q = String(req.query.q ?? "").trim();
  if (!q || q.length < 2) {
    res.json([]);
    return;
  }
  const pattern = `%${q.replace(/[%_]/g, "\\$&")}%`;
  const rows = db
    .prepare(
      `SELECT id, path, title, artist, album, year, duration, tags_json, audio_features_json, audio_vector_json, analysis_version, analysis_updated_at
       FROM tracks
       WHERE title LIKE ? ESCAPE '\\' OR artist LIKE ? ESCAPE '\\' OR album LIKE ? ESCAPE '\\'
       ORDER BY last_scanned_at DESC, id DESC
       LIMIT 30`
    )
    .all(pattern, pattern, pattern) as TrackRecord[];
  res.json(
    rows.map((row) => ({
      ...row,
      tags: tagsFromTrack(row),
      audioFeatures: audioFeaturesFromTrack(row),
      audioVector: audioVectorFromTrack(row)
    }))
  );
});

function queryNumberParam(value: unknown): number | null {
  if (typeof value !== "string" || value.trim() === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

router.get("/api/tracks/filter", (req, res) => {
  const filters: Record<string, unknown> = {
    // The frontend sends tags as a single comma-joined value (see buildFilterUrl);
    // split each value so "tags=jazz,folk" behaves like "tags=jazz&tags=folk".
    selectedTags: (Array.isArray(req.query.tags)
      ? (req.query.tags as unknown[])
      : typeof req.query.tags === "string" && req.query.tags.trim()
        ? [req.query.tags]
        : []
    ).flatMap((tag) => String(tag).split(",")),
    bpmMin: queryNumberParam(req.query.bpmMin),
    bpmMax: queryNumberParam(req.query.bpmMax),
    keyQuery: typeof req.query.keyQuery === "string" ? req.query.keyQuery : "",
    keyMode: req.query.keyMode === "major" || req.query.keyMode === "minor" ? req.query.keyMode : "",
    energyMin: queryNumberParam(req.query.energyMin),
    energyMax: queryNumberParam(req.query.energyMax),
    yearMin: queryNumberParam(req.query.yearMin),
    yearMax: queryNumberParam(req.query.yearMax)
  };
  const { where, params } = buildTrackFilterWhere(filters);
  const limit = Math.max(1, Math.min(Math.floor(Number(req.query.limit ?? 100)) || 100, 500));
  const offset = Math.max(0, Math.floor(Number(req.query.offset ?? 0)) || 0);
  const totalRow = db.prepare(`SELECT COUNT(*) AS count FROM tracks WHERE ${where}`).get(...params) as { count: number };
  const rows = db
    .prepare(
      `SELECT id, title, artist, album, year, tags_json, audio_features_json
       FROM tracks
       WHERE ${where}
       ORDER BY last_scanned_at DESC, id DESC
       LIMIT ? OFFSET ?`
    )
    .all(...params, limit, offset) as Array<{
      id: number;
      title: string | null;
      artist: string | null;
      album: string | null;
      year: number | null;
      tags_json: string;
      audio_features_json: string;
    }>;
  res.json({
    total: totalRow.count,
    tracks: rows.map((row) => ({
      id: row.id,
      title: row.title ?? "",
      artist: row.artist ?? "",
      album: row.album ?? "",
      year: row.year,
      tags: tagsFromTrack(row),
      audioFeatures: audioFeaturesFromTrack(row)
    }))
  });
});

let sortedTagCountsCache: { sorted: Array<[string, number]>; at: number } | null = null;
const sortedTagCountsCacheMs = 60_000;

function getSortedTagCounts(): Array<[string, number]> {
  const now = Date.now();
  if (sortedTagCountsCache && now - sortedTagCountsCache.at < sortedTagCountsCacheMs) {
    return sortedTagCountsCache.sorted;
  }
  const rows = db.prepare("SELECT tags_json FROM tracks").all() as Array<{ tags_json: string }>;
  const counts = new Map<string, number>();
  for (const row of rows) {
    for (const tag of tagsFromTrack(row)) {
      counts.set(tag, (counts.get(tag) ?? 0) + 1);
    }
  }
  sortedTagCountsCache = {
    sorted: [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])),
    at: now
  };
  return sortedTagCountsCache.sorted;
}

router.get("/api/tracks/tag-suggestions", (req, res) => {
  const q = (typeof req.query.q === "string" ? req.query.q : "").trim().toLowerCase();
  const limit = Math.max(1, Math.min(Math.floor(Number(req.query.limit ?? 30)) || 30, 100));
  res.json(
    getSortedTagCounts()
      .filter(([tag]) => !q || tag.toLowerCase().includes(q))
      .slice(0, limit)
      .map(([tag, count]) => ({ tag, count }))
  );
});



export default router;
