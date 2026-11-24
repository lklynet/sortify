import express from "express";
import { db, readTracksStmt } from "../db.js";
import { appState, log } from "../state.js";
import { dbFile, audioAnalysisVersion } from "../config.js";

const router = express.Router();

router.get("/api/health", (_req, res) => {
  res.json({ ok: true, dbFile, navidromeUrl: appState.settings.navidromeUrl });
});


router.get("/api/stats", (_req, res) => {
  const trackCount = db.prepare("SELECT COUNT(*) as count FROM tracks").get() as { count: number };
  const playlistCount = db.prepare("SELECT COUNT(*) as count FROM playlists").get() as { count: number };
  const analyzedTrackCount = db
    .prepare("SELECT COUNT(*) as count FROM tracks WHERE analysis_version = ? AND audio_vector_json != '[]'")
    .get(audioAnalysisVersion) as { count: number };
  res.json({ tracks: trackCount.count, playlists: playlistCount.count, analyzedTracks: analyzedTrackCount.count });
});

let libraryStatsCache: { data: unknown; at: number } | null = null;
const libraryStatsCacheMs = 60_000;

router.get("/api/stats/library", (_req, res) => {
  const now = Date.now();
  if (libraryStatsCache && now - libraryStatsCache.at < libraryStatsCacheMs) {
    res.json(libraryStatsCache.data);
    return;
  }
  const rows = db.prepare("SELECT tags_json, audio_features_json FROM tracks WHERE audio_features_json != '{}'").all() as Array<{ tags_json: string; audio_features_json: string }>;
  const tagCounts = new Map<string, number>();
  const keyCounts = new Map<string, number>();
  const decadeCounts = new Map<string, number>();
  const languageCounts = new Map<string, number>();
  const genreCounts = new Map<string, number>();
  const bpmValues: number[] = [];
  const buckets: Record<string, number[]> = { energy: new Array(10).fill(0), valence: new Array(10).fill(0), danceability: new Array(10).fill(0), acousticness: new Array(10).fill(0), instrumentalness: new Array(10).fill(0), brightness: new Array(10).fill(0) };
  const bpmKeys = ["energy", "valence", "danceability", "acousticness", "instrumentalness", "brightness"] as const;
  for (const row of rows) {
    let tags: string[] = [];
    let features: Record<string, unknown> = {};
    try { tags = JSON.parse(row.tags_json) as string[]; } catch { /* empty */ }
    try { features = JSON.parse(row.audio_features_json) as Record<string, unknown>; } catch { /* empty */ }
    for (const tag of tags) {
      const count = (tagCounts.get(tag) ?? 0) + 1;
      tagCounts.set(tag, count);
      if (tag === "japanese" || tag === "korean" || tag === "chinese" || tag === "russian" || tag === "arabic" || tag === "hindi") {
        languageCounts.set(tag, count);
      }
      if (/^\d{4}s$/.test(tag)) {
        decadeCounts.set(tag, (decadeCounts.get(tag) ?? 0) + 1);
      }
    }
    const key = features.key as string | undefined;
    if (key) keyCounts.set(key, (keyCounts.get(key) ?? 0) + 1);
    const bpm = features.bpm as number | null;
    if (bpm && bpm > 0 && Number.isFinite(bpm)) bpmValues.push(bpm);
    for (const feature of bpmKeys) {
      const val = features[feature] as number | undefined;
      if (typeof val === "number" && Number.isFinite(val) && val >= 0 && val <= 1) {
        const bucket = Math.min(9, Math.floor(val * 10));
        buckets[feature][bucket] += 1;
      }
    }
  }
  const sortedTags = [...tagCounts.entries()].sort((a, b) => b[1] - a[1]);
  const topTags = sortedTags.slice(0, 30).map(([tag, count]) => ({ tag, count }));
  const topGenres = sortedTags.filter(([tag]) => !/^\d{4}s$/.test(tag) && !["japanese", "korean", "chinese", "russian", "arabic", "hindi"].includes(tag) && !["decade", "low tempo", "mid tempo", "high energy", "chill", "dance", "minor key", "major key", "bright", "dark", "melancholic", "euphoric", "positive", "moody", "acoustic", "organic", "electronic", "processed", "energetic", "gentle", "calm", "laid-back", "driving", "fast tempo", "danceable", "groove", "rhythmic", "textural", "warm", "uplifting", "instrumental", "cover", "rarity", "remix", "live", "year"].includes(tag)).slice(0, 20).map(([genre, count]) => ({ genre, count }));
  const keyDist = [...keyCounts.entries()].sort((a, b) => {
    const numA = parseInt(a[0], 10) || 0;
    const numB = parseInt(b[0], 10) || 0;
    return numA - numB || a[0].localeCompare(b[0]);
  }).map(([key, count]) => ({ key, count }));
  const decadeSpread = [...decadeCounts.entries()].sort((a, b) => a[0].localeCompare(b[0])).map(([decade, count]) => ({ decade, count }));
  const langs = [...languageCounts.entries()].sort((a, b) => b[1] - a[1]).map(([language, count]) => ({ language, count }));
  const bpmSorted = [...bpmValues].sort((a, b) => a - b);
  const data = {
    trackCount: rows.length,
    analyzedCount: rows.length,
    topTags,
    audioDistributions: buckets,
    keyDistribution: keyDist,
    decadeSpread,
    languageBreakdown: langs,
    topGenres,
    bpmStats: {
      min: bpmSorted[0] ?? 0,
      max: bpmSorted[bpmSorted.length - 1] ?? 0,
      avg: bpmSorted.length ? Math.round(bpmSorted.reduce((s, v) => s + v, 0) / bpmSorted.length) : 0,
      median: bpmSorted.length ? bpmSorted[Math.floor(bpmSorted.length / 2)] : 0
    }
  };
  libraryStatsCache = { data, at: now };
  res.json(data);
});

router.get("/api/ops", (req, res) => {
  const limit = Number(req.query.limit ?? 200);
  const bounded = Math.max(1, Math.min(limit, 500));
  res.json(appState.operationLogs.slice(-bounded).reverse());
});


export default router;
