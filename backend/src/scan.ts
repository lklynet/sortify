import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { seededRandom, hashSeed, limitTracksByArtist, maxArtistPerPlaylist, maxArtistFallbackPerPlaylist, audioVectorFromTrack, tagsFromTrack } from "./playlist-generator.js";
import fs from "node:fs";
import path from "node:path";
import { deriveBaseTags, mergeRankedTagSources, normalizeTag, uniqueTags } from "./tags.js";
import { fetchSubsonicSongs, buildSubsonicStreamUrl, getSubsonicConnection, type SubsonicConnection } from "./subsonic-client.js";
import { fetchLastFmTags } from "./lastfm.js";
import { fetchMusicBrainzTags } from "./musicbrainz.js";
import { audioAnalysisScriptFile, audioAnalysisEnabled, audioAnalysisSampleSeconds, audioAnalysisTimeoutMs, audioAnalysisVersion, generatedArtworkDir, dbFile } from "./config.js";
import { appState, log } from "./state.js";
import { db, upsertTrackStmt, readTrackScanStateStmt, deleteTrackByPathStmt, updateTrackPlayStatsStmt } from "./db.js";
import { clamp, syncWorkerConnectionFromSettings, appendLog } from "./settings.js";
import type { TrackRecord, AudioAnalysis, PlaylistRecord, ScanProgress } from "./types.js";

export const lastFmArtistTagsCache = new Map<string, string[]>();
export const lastFmAlbumTagsCache = new Map<string, string[]>();
export const musicBrainzArtistTagsCache = new Map<string, string[]>();

export function isoWeekKey(now = new Date()): string {
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const weekNumber = Math.ceil(((date.getTime() - yearStart.getTime()) / 86400000 + 1) / 7);
  return `${date.getUTCFullYear()}-W${String(weekNumber).padStart(2, "0")}`;
}

export function weeklyRotatedTrackIds(
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

export function extractSubsonicSongId(pathValue: string): string | null {
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


export function hashText(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}


export function deleteGeneratedArtworkFile(artworkPath: string | null | undefined) {
  if (!artworkPath) {
    return;
  }
  const filePath = path.join(generatedArtworkDir, path.basename(artworkPath));
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
}

export function pruneGeneratedArtworkFiles() {
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


export function audioFeaturesFromTrack(track: Pick<TrackRecord, "audio_features_json">): AudioAnalysis | null {
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

export function hasFreshAudioAnalysis(track: Pick<TrackRecord, "analysis_version" | "audio_features_json" | "audio_vector_json"> | undefined) {
  if (!track || track.analysis_version !== audioAnalysisVersion) {
    return false;
  }
  return Boolean(audioFeaturesFromTrack(track)?.moodTags?.length) && audioVectorFromTrack(track).length >= 8;
}

export async function analyzeTrackAudio(connection: SubsonicConnection, songId: string): Promise<{ features: AudioAnalysis; vector: number[] } | null> {
  if (!audioAnalysisEnabled || !fs.existsSync(audioAnalysisScriptFile)) {
    return null;
  }
  const streamUrl = buildSubsonicStreamUrl(connection, songId);
  const pythonBin = process.env.SORTIFY_PYTHON || "python3";
  return await new Promise((resolve, reject) => {
    const child = spawn(pythonBin, [audioAnalysisScriptFile, "--url", streamUrl, "--sample-seconds", String(audioAnalysisSampleSeconds)], {
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

export function resolveConnection(input: unknown): SubsonicConnection {
  return getSubsonicConnection(input, {
    baseUrl: appState.settings.navidromeUrl,
    username: appState.settings.navidromeUsername,
    password: appState.settings.navidromePassword
  });
}

// Parse an optional debug scan limit (max tracks to scan). Absent/invalid -> full scan.
export function parseScanLimit(value: unknown): number | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isInteger(n) || n <= 0) {
    return undefined;
  }
  return Math.min(n, 100000);
}

export async function scanLibrary(
  connection: SubsonicConnection,
  onProgress?: (progress: Partial<Pick<ScanProgress, "scannedFiles" | "processedFiles" | "updatedTracks" | "errors">>) => void,
  options?: { forceRefreshAnalysis?: boolean; limit?: number }
) {
  const forceRefreshAnalysis = Boolean(options?.forceRefreshAnalysis);
  const scanLimit = parseScanLimit(options?.limit);
  const isLimitedScan = scanLimit !== undefined;
  const discovered = await fetchSubsonicSongs(connection, scanLimit);
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
    play_count: number;
    favorite: number;
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
    meta: { source: connection.baseUrl, discoveredFiles: discovered.length, scanLimit: scanLimit ?? null }
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
    const playCountVal = typeof song.playCount === "number" && Number.isFinite(song.playCount) ? Math.max(0, Math.floor(song.playCount)) : 0;
    const favoriteVal = typeof song.starred === "boolean" ? (song.starred ? 1 : 0) : (existing?.favorite ?? 0);
    if (!shouldHydrate) {
      processed += 1;
      skipped += 1;
      updateTrackPlayStatsStmt.run(playCountVal, favoriteVal, now, trackPath);
      if (processed % 100 === 0 || processed === discovered.length) {
        onProgress?.({ scannedFiles: discovered.length, processedFiles: processed, updatedTracks: updated, errors });
      }
      continue;
    }
    try {
      const [musicBrainz, analyzedAudio, lastFmTags] = await Promise.all([
        fetchMusicBrainzTags(artist, title, musicBrainzArtistTagsCache),
        analyzeTrackAudio(connection, song.id).catch((error) => {
          appendLog({
            level: "warn",
            scope: "scan",
            message: "Audio analysis skipped for track",
            meta: { songId: song.id, title: song.title, error: error instanceof Error ? error.message : "Unknown error" }
          });
          return null;
        }),
        fetchLastFmTags(appState.settings.lastFmApiKey, artist, title, album, { artistTags: lastFmArtistTagsCache, albumTags: lastFmAlbumTagsCache })
      ]);
      const mergedYear = year ?? musicBrainz.year ?? null;
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
      updateTrackPlayStatsStmt.run(playCountVal, favoriteVal, now, trackPath);
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
  // A limited (debug) scan only sees a subset of the library, so pruning un-discovered
  // tracks here would delete the rest of the library. Skip the prune when limited.
  if (stalePaths.length && !isLimitedScan) {
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
  } else if (isLimitedScan && stalePaths.length) {
    appendLog({
      level: "info",
      scope: "scan",
      message: "Limited scan: skipping stale-track prune (only a subset was scanned)",
      meta: { scanLimit, skippedStalePaths: stalePaths.length }
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
