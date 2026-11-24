import express from "express";
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { db, readTracksStmt, deletePlaylistByIdStmt, deletePlaylistTracksByPlaylistIdStmt, insertPlaylistTrackStmt, refreshProtectedPlaylistStmt, updatePlaylistArtworkStmt, insertPlaylistStmt } from "../db.js";
import { appState, log } from "../state.js";
import { generatedArtworkDir, playlistArtworkRenderVersion } from "../config.js";
import { appendLog, clamp, syncWorkerConnectionFromSettings } from "../settings.js";
import { getManagedPlaylistNamesBySlug, refreshRecommendedPlaylists, syncSubsonicPlaylists, syncSinglePlaylist, syncPlaylistArtwork, replacePlaylists, buildFilterDescription } from "../playlists.js";
import { audioFeaturesFromTrack, resolveConnection, deleteGeneratedArtworkFile, extractSubsonicSongId, hashText } from "../scan.js";
import { uploadNavidromePlaylistArtwork, subsonicRequest, navidromeLogin, type SubsonicConnection } from "../subsonic-client.js";
import { buildTrackFilterWhere, syncablePathClause } from "../track-filter.js";
import { generateCandidates, tagsFromTrack, audioVectorFromTrack, type PlaylistCandidate } from "../playlist-generator.js";
import { renderPlaylistArtwork, sanitizeFilename } from "../artwork-renderer.js";
import { normalizeTag } from "../tags.js";
import type { TrackRecord, PlaylistRecord, AudioAnalysis } from "../types.js";

const router = express.Router();

router.post("/api/playlists/generate", async (req, res) => {
  const requestedCount = Number(req.body?.weeklyPlaylistCount ?? appState.workerState.weeklyPlaylistCount);
  const count = clamp(requestedCount, 0, 8);
  const { candidates } = await refreshRecommendedPlaylists(count, randomUUID());
  res.json({ generated: candidates.length, playlists: candidates.map((item) => ({ name: item.name, tracks: item.trackIds.length })) });
});

router.get("/api/playlists", (_req, res) => {
  const playlists = db
    .prepare(
      `SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, fewest_listens_priority, updated_at,
              artwork_path, artwork_attribution, artwork_attribution_url, artwork_signature, artwork_updated_at,
              filter_json, seeds_json
       FROM playlists
       WHERE is_recommended = 1 OR mode = 'pinned'
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
    let seedIds: number[] = [];
    try {
      seedIds = (JSON.parse(playlist.seeds_json || "[]") as unknown[]).filter((v): v is number => Number.isInteger(v) && (v as number) > 0);
    } catch { seedIds = []; }
    const seeds = seedIds.length > 0
      ? db.prepare(`SELECT id, title, artist FROM tracks WHERE id IN (${seedIds.map(() => "?").join(",")})`).all(...seedIds) as Array<{ id: number; title: string; artist: string }>
      : [];
    return {
      ...playlist,
      kind: seedIds.length > 0 ? "radio" : playlist.filter_json ? "filter" : "standard",
      seedCount: seedIds.length,
      fewestListensPriority: playlist.fewest_listens_priority === 1,
      seeds,
      artworkUrl: playlist.artwork_path || null,
      artworkAttribution: playlist.artwork_attribution || null,
      artworkAttributionUrl: playlist.artwork_attribution_url || null,
      tracks
    };
  });
  res.json(response);
});

router.get("/api/playlists/snapshots", (_req, res) => {
  const snapshots = db
    .prepare(
      `SELECT ps.id, ps.playlist_id, ps.slug, ps.name, ps.track_ids_json, ps.snapshot_week, ps.created_at,
              COUNT(pt.track_id) as track_count
       FROM playlist_snapshots ps
       LEFT JOIN playlist_tracks pt ON pt.playlist_id = ps.playlist_id
       WHERE ps.playlist_id IN (SELECT id FROM playlists WHERE is_recommended = 1 OR mode = 'pinned')
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

router.patch("/api/playlists/:id", async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Invalid playlist id" });
    return;
  }
  const existing = db
    .prepare("SELECT id, slug, mode, selected, sync_target, is_recommended, fewest_listens_priority FROM playlists WHERE id = ?")
    .get(id) as
    | {
        id: number;
        slug: string;
        mode: "dynamic" | "pinned";
        selected: number;
        sync_target: "subsonic";
        is_recommended: number;
        fewest_listens_priority: number;
      }
    | undefined;
  if (!existing) {
    res.status(404).json({ error: "Playlist not found" });
    return;
  }
  const modeInput = req.body?.mode;
  const selectedInput = req.body?.selected;
  const syncTargetInput = req.body?.syncTarget;
  const fewestInput = req.body?.fewestListensPriority;
  const selected = typeof selectedInput === "boolean" ? (selectedInput ? 1 : 0) : existing.selected;
  const mode: "dynamic" | "pinned" =
    modeInput === "pinned" || modeInput === "dynamic" ? modeInput : existing.mode;
  const syncTarget: "subsonic" = syncTargetInput === "subsonic" || !syncTargetInput ? "subsonic" : existing.sync_target;
  const fewestListensPriority = typeof fewestInput === "boolean" ? (fewestInput ? 1 : 0) : existing.fewest_listens_priority;
  db.prepare("UPDATE playlists SET mode = ?, selected = ?, sync_target = ?, is_recommended = ?, fewest_listens_priority = ?, updated_at = ? WHERE id = ?").run(
    mode,
    selected,
    syncTarget,
    mode === "dynamic" ? existing.is_recommended : 1,
    fewestListensPriority,
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

router.delete("/api/playlists/:id", async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Invalid playlist id" });
    return;
  }
  const existing = db
    .prepare("SELECT id, name, artwork_path FROM playlists WHERE id = ?")
    .get(id) as { id: number; name: string; artwork_path: string } | undefined;
  if (!existing) {
    res.status(404).json({ error: "Playlist not found" });
    return;
  }
  deleteGeneratedArtworkFile(existing.artwork_path);
  deletePlaylistByIdStmt.run(id);
  const connection = resolveConnection(req.body?.connection ?? req.body);
  if (connection) {
    const remoteList = await subsonicRequest<{ playlists?: { playlist?: Array<{ id: string; name: string }> } }>(connection, "getPlaylists.view");
    const remoteId = remoteList.playlists?.playlist?.find((p) => p.name === existing.name)?.id;
    if (remoteId) {
      await subsonicRequest(connection, "deletePlaylist.view", { id: remoteId });
    }
  }
  appendLog({
    level: "info",
    scope: "playlist",
    message: "Playlist deleted",
    meta: { id }
  });
  res.json({ ok: true });
});

router.post("/api/playlists/:id/refresh", async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Invalid playlist id" });
    return;
  }
  const existing = db
    .prepare("SELECT id, slug, name, mode, filter_json, seeds_json, artwork_path, fewest_listens_priority FROM playlists WHERE id = ?")
    .get(id) as { id: number; slug: string; name: string; mode: string; filter_json: string; seeds_json: string; artwork_path: string; fewest_listens_priority: number } | undefined;
  if (!existing) {
    res.status(404).json({ error: "Playlist not found" });
    return;
  }
  const tracks = db
    .prepare("SELECT id, path, title, artist, album, year, duration, tags_json, audio_features_json, audio_vector_json, analysis_version, analysis_updated_at, play_count, favorite FROM tracks")
    .all() as TrackRecord[];
  const syncableTracks = tracks.filter((track) => Boolean(extractSubsonicSongId(track.path)));
  const enrichedTracks = syncableTracks.map((track) => ({
    id: track.id,
    title: track.title,
    artist: track.artist,
    album: track.album,
    year: track.year,
    tags_json: track.tags_json,
    audio_features_json: track.audio_features_json,
    play_count: track.play_count,
    favorite: track.favorite,
    tags: tagsFromTrack(track)
  }));
  let newTrackIds: number[] = [];
  if (existing.filter_json) {
    let filters: Record<string, unknown> = {};
    try { filters = JSON.parse(existing.filter_json) as Record<string, unknown>; } catch { /* ignore */ }
    const filterPool = enrichedTracks.filter((track) => {
      if (Array.isArray(filters.selectedTags) && (filters.selectedTags as string[]).length > 0) {
        const needles = (filters.selectedTags as string[]).map((t) => String(t).toLowerCase());
        if (!needles.every((needle) => track.tags.some((tag) => tag.toLowerCase().includes(needle)))) return false;
      }
      if (typeof filters.bpmMin === "number" && !Number.isNaN(filters.bpmMin as number)) {
        const af = audioFeaturesFromTrack({ audio_features_json: track.audio_features_json } as TrackRecord);
        if ((af?.bpm ?? 0) < (filters.bpmMin as number)) return false;
      }
      if (typeof filters.bpmMax === "number" && !Number.isNaN(filters.bpmMax as number)) {
        const af = audioFeaturesFromTrack({ audio_features_json: track.audio_features_json } as TrackRecord);
        if ((af?.bpm ?? 0) > (filters.bpmMax as number)) return false;
      }
      if (typeof filters.keyQuery === "string" && filters.keyQuery) {
        const af = audioFeaturesFromTrack({ audio_features_json: track.audio_features_json } as TrackRecord);
        const qk = (filters.keyQuery as string).toLowerCase();
        if (!(af?.key ?? "").toLowerCase().includes(qk) && !(af?.camelotKey ?? "").toLowerCase().includes(qk)) return false;
      }
      if (typeof filters.energyMin === "number" && !Number.isNaN(filters.energyMin as number)) {
        const af = audioFeaturesFromTrack({ audio_features_json: track.audio_features_json } as TrackRecord);
        if ((af?.energy ?? 0) * 100 < (filters.energyMin as number)) return false;
      }
      if (typeof filters.energyMax === "number" && !Number.isNaN(filters.energyMax as number)) {
        const af = audioFeaturesFromTrack({ audio_features_json: track.audio_features_json } as TrackRecord);
        if ((af?.energy ?? 0) * 100 > (filters.energyMax as number)) return false;
      }
      if (typeof filters.yearMin === "number" && !Number.isNaN(filters.yearMin as number)) {
        if ((track.year ?? 0) < (filters.yearMin as number)) return false;
      }
      if (typeof filters.yearMax === "number" && !Number.isNaN(filters.yearMax as number)) {
        if ((track.year ?? 0) > (filters.yearMax as number)) return false;
      }
      if (typeof filters.keyMode === "string" && filters.keyMode) {
        const af = audioFeaturesFromTrack({ audio_features_json: track.audio_features_json } as TrackRecord);
        const key = (af?.key ?? "").toLowerCase();
        const isMinor = key.includes("m") && !key.includes("major");
        const isMajor = key && !isMinor;
        if (filters.keyMode === "minor" && !isMinor) return false;
        if (filters.keyMode === "major" && !isMajor) return false;
      }
      return true;
    });
    const poolIds = filterPool.slice(0, appState.settings.maxTracksPerPlaylist).map((t) => t.id);
    let radioSeedIds: number[] = [];
    try {
      radioSeedIds = (JSON.parse(existing.seeds_json || "[]") as unknown[]).filter((v): v is number => Number.isInteger(v) && (v as number) > 0);
    } catch { radioSeedIds = []; }
    if (radioSeedIds.length > 0) {
      const seen = new Set<number>(radioSeedIds);
      newTrackIds = [...radioSeedIds, ...poolIds.filter((trackId) => !seen.has(trackId))].slice(0, appState.settings.maxTracksPerPlaylist);
    } else {
      newTrackIds = poolIds;
    }
    if (existing.fewest_listens_priority && newTrackIds.length > 0) {
      const pcRows = db.prepare(`SELECT id, play_count FROM tracks WHERE id IN (${newTrackIds.map(() => "?").join(",")})`).all(...newTrackIds) as Array<{ id: number; play_count: number }>;
      const pcById = new Map<number, number>(pcRows.map((row) => [row.id, row.play_count]));
      newTrackIds.sort((a, b) => (pcById.get(a) ?? 0) - (pcById.get(b) ?? 0));
    }
  } else {
    const candidates = await generateCandidates({
      tracks: enrichedTracks as any,
      desiredWeeklyPlaylists: 1,
      maxTracksPerPlaylist: appState.settings.maxTracksPerPlaylist,
      generationKey: randomUUID(),
      pinnedSlugs: new Set<string>(),
      leastPlayedRows: [],
      favoriteRows: [],
      fewestListensPriority: Boolean(existing.fewest_listens_priority)
    });
    if (candidates.length > 0) {
      newTrackIds = candidates[0].trackIds.slice(0, appState.settings.maxTracksPerPlaylist);
    }
  }
  if (newTrackIds.length === 0) {
    res.status(422).json({ error: "No tracks match the playlist criteria" });
    return;
  }
  const now = new Date().toISOString();
  db.transaction(() => {
    deletePlaylistTracksByPlaylistIdStmt.run(id);
    newTrackIds.forEach((trackId, position) => {
      insertPlaylistTrackStmt.run(id, trackId, position + 1);
    });
    refreshProtectedPlaylistStmt.run({ id, updated_at: now });
  })();
  const signature = hashText(`${playlistArtworkRenderVersion}|${existing.slug}|${existing.name}|${existing.mode}|${newTrackIds.join(",")}`);
  const imageUrl = "https://picsum.photos/1200";
  const rendered = await renderPlaylistArtwork(existing.name, imageUrl);
  const fileName = `${sanitizeFilename(existing.slug)}-${signature.slice(0, 12)}.jpg`;
  const localPath = path.join(generatedArtworkDir, fileName);
  fs.writeFileSync(localPath, rendered);
  if (existing.artwork_path) {
    const previousFile = path.join(generatedArtworkDir, path.basename(existing.artwork_path));
    if (fs.existsSync(previousFile) && previousFile !== localPath) {
      fs.unlinkSync(previousFile);
    }
  }
  updatePlaylistArtworkStmt.run({
    id,
    artwork_path: `/generated-artwork/${fileName}`,
    artwork_attribution: "",
    artwork_attribution_url: "",
    artwork_signature: signature,
    artwork_updated_at: now
  });
  const connection = resolveConnection(req.body?.connection ?? req.body);
  if (connection) {
    await syncSinglePlaylist(connection, id, now);
  }
  appendLog({
    level: "info",
    scope: "playlist",
    message: "Playlist refreshed individually",
    meta: { id, name: existing.name, tracks: newTrackIds.length }
  });
  res.json({ ok: true, tracks: newTrackIds.length });
});

router.post("/api/playlists/:id/artwork", async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Invalid playlist id" });
    return;
  }
  const playlist = db
    .prepare("SELECT id, slug, name, artwork_path FROM playlists WHERE id = ?")
    .get(id) as { id: number; slug: string; name: string; artwork_path: string } | undefined;
  if (!playlist) {
    res.status(404).json({ error: "Playlist not found" });
    return;
  }
  const imageUrl = "https://picsum.photos/1200";
  const rendered = await renderPlaylistArtwork(playlist.name, imageUrl);
  const newSignature = hashText(`${playlistArtworkRenderVersion}|${playlist.slug}|${playlist.name}|cover-only|${Date.now()}`);
  const fileName = `${sanitizeFilename(playlist.slug)}-${newSignature.slice(0, 12)}.jpg`;
  const localPath = path.join(generatedArtworkDir, fileName);
  fs.writeFileSync(localPath, rendered);
  if (playlist.artwork_path) {
    const previousFile = path.join(generatedArtworkDir, path.basename(playlist.artwork_path));
    if (fs.existsSync(previousFile)) fs.unlinkSync(previousFile);
  }
  updatePlaylistArtworkStmt.run({
    id: playlist.id,
    artwork_path: `/generated-artwork/${fileName}`,
    artwork_attribution: "",
    artwork_attribution_url: "",
    artwork_signature: newSignature,
    artwork_updated_at: new Date().toISOString()
  });
  const connection = resolveConnection(req.body?.connection ?? req.body);
  if (connection) {
    const token = await navidromeLogin(connection);
    const remoteList = await subsonicRequest<{ playlists?: { playlist?: Array<{ id: string; name: string }> } }>(connection, "getPlaylists.view");
    const remoteId = remoteList.playlists?.playlist?.find((p) => p.name === playlist.name)?.id;
    if (remoteId) {
      await uploadNavidromePlaylistArtwork(connection, remoteId, token, rendered, fileName);
    }
  }
  res.json({ ok: true, artworkUrl: `/generated-artwork/${fileName}` });
});

router.post("/api/playlists/filter", async (req, res) => {
  const name = String(req.body?.name ?? "").trim();
  const filters = req.body?.filters;
  if (!name || !filters || typeof filters !== "object") {
    res.status(400).json({ error: "Name and filters are required" });
    return;
  }
  const now = new Date().toISOString();
  const slug = `filter-${sanitizeSlug(name)}-${Date.now().toString(36)}`;
  const description = buildFilterDescription(filters);
  let playlistId = 0;
  const { where, params } = buildTrackFilterWhere(filters);
  const cappedRows = db
    .prepare(`SELECT id FROM tracks WHERE ${where} AND ${syncablePathClause()} ORDER BY id LIMIT ?`)
    .all(...params, appState.settings.maxTracksPerPlaylist) as Array<{ id: number }>;
  const cappedTrackIds = cappedRows.map((row) => row.id);
  const artworkPath = await renderFilterPlaylistArtwork(slug, name, cappedTrackIds);
  db.transaction(() => {
    const result = insertPlaylistStmt.run({
      slug,
      name,
      description,
      created_at: now,
      updated_at: now,
      mode: "pinned",
      selected: 0,
      sync_target: "subsonic",
      is_recommended: 1
    });
    playlistId = Number(result.lastInsertRowid);
    db.prepare("UPDATE playlists SET filter_json = ? WHERE id = ?").run(JSON.stringify(filters), playlistId);
    if (artworkPath) {
      const signature = hashText(`${playlistArtworkRenderVersion}|${slug}|${name}|pinned|${cappedTrackIds.join(",")}`);
      db.prepare("UPDATE playlists SET artwork_path = ?, artwork_signature = ?, artwork_updated_at = ? WHERE id = ?").run(artworkPath, signature, now, playlistId);
    }
    cappedTrackIds.forEach((trackId, position) => {
      insertPlaylistTrackStmt.run(playlistId, trackId, position + 1);
    });
  })();
  appendLog({
    level: "info",
    scope: "playlist",
    message: `Filter playlist created`,
    meta: { id: playlistId, name, tracks: cappedTrackIds.length }
  });
  const connection = resolveConnection(req.body?.connection ?? req.body);
  if (connection) {
    await syncSinglePlaylist(connection, playlistId, now);
  }
  res.json({ ok: true, id: playlistId, name, slug });
});

/**
 * Radio playlists: derive a track filter from the shared tags/features of a
 * handful of seed tracks. Ranges are padded so the result behaves like a
 * radio station (loose) rather than an exact match on the seeds.
 */
type RadioSeedRow = {
  id: number;
  title: string | null;
  artist: string | null;
  tags_json: string;
  audio_features_json: string;
  year: number | null;
};

function parseSeedTags(tagsJson: string): string[] {
  try {
    const parsed = JSON.parse(tagsJson || "[]") as unknown;
    if (!Array.isArray(parsed)) return [];
    const seen = new Set<string>();
    const tags: string[] = [];
    for (const raw of parsed) {
      const tag = normalizeTag(String(raw ?? ""));
      if (tag && !seen.has(tag)) {
        seen.add(tag);
        tags.push(tag);
      }
    }
    return tags;
  } catch {
    return [];
  }
}

function parseSeedFeature(audioFeaturesJson: string, field: "bpm" | "energy"): number | null {
  try {
    const parsed = JSON.parse(audioFeaturesJson || "{}") as Record<string, unknown>;
    const value = parsed[field];
    return typeof value === "number" && Number.isFinite(value) ? value : null;
  } catch {
    return null;
  }
}

function deriveRadioFilter(seeds: RadioSeedRow[]): Record<string, unknown> {
  const filters: Record<string, unknown> = {};
  // Tags present in every seed first; otherwise tags in at least half the seeds.
  const tagCounts = new Map<string, number>();
  for (const seed of seeds) {
    for (const tag of parseSeedTags(seed.tags_json)) {
      tagCounts.set(tag, (tagCounts.get(tag) ?? 0) + 1);
    }
  }
  const seedCount = seeds.length;
  let tagPool = [...tagCounts.entries()].filter(([, count]) => count === seedCount);
  if (tagPool.length === 0) {
    tagPool = [...tagCounts.entries()]
      .filter(([, count]) => count >= Math.ceil(seedCount / 2))
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6);
  }
  if (tagPool.length === 0) {
    tagPool = [...tagCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 3);
  }
  if (tagPool.length > 0) {
    filters.selectedTags = tagPool.map(([tag]) => tag);
  }
  const bpms = seeds
    .map((seed) => parseSeedFeature(seed.audio_features_json, "bpm"))
    .filter((value): value is number => value !== null);
  if (bpms.length >= 2) {
    const min = Math.min(...bpms);
    const max = Math.max(...bpms);
    filters.bpmMin = Math.max(1, Math.round(min - Math.max(5, Math.round(min * 0.1))));
    filters.bpmMax = Math.round(max + Math.max(5, Math.round(max * 0.1)));
  }
  const energies = seeds
    .map((seed) => parseSeedFeature(seed.audio_features_json, "energy"))
    .filter((value): value is number => value !== null);
  if (energies.length >= 2) {
    filters.energyMin = Math.max(0, Math.round((Math.min(...energies) - 0.05) * 100));
    filters.energyMax = Math.min(100, Math.round((Math.max(...energies) + 0.05) * 100));
  }
  const years = seeds
    .map((seed) => seed.year)
    .filter((value): value is number => typeof value === "number" && value > 1900 && value <= 2100);
  if (years.length >= 2) {
    filters.yearMin = Math.min(...years) - 2;
    filters.yearMax = Math.max(...years) + 2;
  }
  return filters;
}

function radioSeedDescription(filters: Record<string, unknown>, seedCount: number): string {
  return `${buildFilterDescription(filters)} · ${seedCount} radio seeds`;
}

function loadRadioSeeds(seedIds: number[]): RadioSeedRow[] {
  const placeholders = seedIds.map(() => "?").join(",");
  return db
    .prepare(`SELECT id, title, artist, tags_json, audio_features_json, year FROM tracks WHERE id IN (${placeholders})`)
    .all(...seedIds) as RadioSeedRow[];
}

function parseRadioSeedIds(input: unknown): number[] {
  if (!Array.isArray(input)) return [];
  return [...new Set(input.map((value) => Number(value)).filter((value) => Number.isInteger(value) && value > 0))];
}

router.post("/api/playlists/radio", async (req, res) => {
  const name = String(req.body?.name ?? "").trim();
  const seedTrackIds = parseRadioSeedIds(req.body?.seedTrackIds);
  if (!name || name.length > 80) {
    res.status(400).json({ error: "A playlist name (max 80 characters) is required" });
    return;
  }
  if (seedTrackIds.length < 1 || seedTrackIds.length > 12) {
    res.status(400).json({ error: "Select between 1 and 12 seed tracks" });
    return;
  }
  const seeds = loadRadioSeeds(seedTrackIds);
  if (seeds.length === 0) {
    res.status(400).json({ error: "No valid seed tracks found in the library" });
    return;
  }
  const filters = deriveRadioFilter(seeds);
  const now = new Date().toISOString();
  const slug = `radio-${sanitizeSlug(name)}-${Date.now().toString(36)}`;
  const description = radioSeedDescription(filters, seeds.length);
  const { where, params } = buildTrackFilterWhere(filters);
  const matchRows = db
    .prepare(`SELECT id FROM tracks WHERE ${where} AND ${syncablePathClause()} ORDER BY RANDOM() LIMIT ?`)
    .all(...params, appState.settings.maxTracksPerPlaylist * 2) as Array<{ id: number }>;
  const seedSet = new Set(seeds.map((seed) => seed.id));
  const trackIds = [...seedTrackIds.filter((id) => seedSet.has(id)), ...matchRows.map((row) => row.id).filter((id) => !seedSet.has(id))].slice(0, appState.settings.maxTracksPerPlaylist);
  if (trackIds.length < 5) {
    res.status(422).json({ error: "Too few tracks match these seeds — try adding more seeds" });
    return;
  }
  let playlistId = 0;
  db.transaction(() => {
    const result = insertPlaylistStmt.run({
      slug,
      name,
      description,
      created_at: now,
      updated_at: now,
      mode: "pinned",
      selected: 0,
      sync_target: "subsonic",
      is_recommended: 1
    });
    playlistId = Number(result.lastInsertRowid);
    db.prepare("UPDATE playlists SET filter_json = ?, seeds_json = ? WHERE id = ?").run(JSON.stringify(filters), JSON.stringify(seeds.map((seed) => seed.id)), playlistId);
    trackIds.forEach((trackId, position) => {
      insertPlaylistTrackStmt.run(playlistId, trackId, position + 1);
    });
  })();
  const artworkPath = await renderFilterPlaylistArtwork(slug, name, trackIds);
  if (artworkPath) {
    const signature = hashText(`${playlistArtworkRenderVersion}|${slug}|${name}|pinned|${trackIds.join(",")}`);
    db.prepare("UPDATE playlists SET artwork_path = ?, artwork_signature = ?, artwork_updated_at = ? WHERE id = ?").run(artworkPath, signature, now, playlistId);
  }
  appendLog({
    level: "info",
    scope: "playlist",
    message: "Radio playlist created",
    meta: { id: playlistId, name, seeds: seeds.length, tracks: trackIds.length }
  });
  const connection = resolveConnection(req.body?.connection ?? req.body);
  if (connection) {
    await syncSinglePlaylist(connection, playlistId, now);
  }
  res.json({ ok: true, id: playlistId, name, slug, seedCount: seeds.length, trackCount: trackIds.length });
});

router.put("/api/playlists/:id/seeds", (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) {
    res.status(400).json({ error: "Invalid playlist id" });
    return;
  }
  const existing = db
    .prepare("SELECT id, slug, name, seeds_json FROM playlists WHERE id = ?")
    .get(id) as { id: number; slug: string; name: string; seeds_json: string } | undefined;
  if (!existing || !existing.seeds_json) {
    res.status(404).json({ error: "Radio playlist not found" });
    return;
  }
  const seedTrackIds = parseRadioSeedIds(req.body?.seedTrackIds);
  if (seedTrackIds.length < 1 || seedTrackIds.length > 12) {
    res.status(400).json({ error: "Select between 1 and 12 seed tracks" });
    return;
  }
  const seeds = loadRadioSeeds(seedTrackIds);
  if (seeds.length === 0) {
    res.status(400).json({ error: "No valid seed tracks found in the library" });
    return;
  }
  const filters = deriveRadioFilter(seeds);
  const description = radioSeedDescription(filters, seeds.length);
  db.prepare("UPDATE playlists SET filter_json = ?, seeds_json = ?, description = ?, updated_at = ? WHERE id = ?")
    .run(JSON.stringify(filters), JSON.stringify(seeds.map((seed) => seed.id)), description, new Date().toISOString(), id);
  appendLog({
    level: "info",
    scope: "playlist",
    message: "Radio playlist seeds updated",
    meta: { id, name: existing.name, seeds: seeds.length }
  });
  res.json({ ok: true, seedCount: seeds.length, description });
});

async function renderFilterPlaylistArtwork(slug: string, title: string, trackIds: number[]): Promise<string> {
  if (!trackIds.length) return "";
  try {
    const imageUrl = "https://picsum.photos/1200";
    const rendered = await renderPlaylistArtwork(title, imageUrl);
    const signature = hashText(`${playlistArtworkRenderVersion}|${slug}|${title}|pinned|${trackIds.join(",")}`);
    const fileName = `${sanitizeFilename(slug)}-${signature.slice(0, 12)}.jpg`;
    const localPath = path.join(generatedArtworkDir, fileName);
    fs.writeFileSync(localPath, rendered);
    return `/generated-artwork/${fileName}`;
  } catch {
    return "";
  }
}

function sanitizeSlug(input: string): string {
  return input.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 60);
}


router.post("/api/playlists/apply", async (req, res) => {
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



export default router;
