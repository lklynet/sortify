import fs from "node:fs";
import path from "node:path";
import { generateCandidates, hashSeed, seededRandom, limitTracksByArtist, tagsFromTrack, audioVectorFromTrack, maxArtistPerPlaylist, maxArtistFallbackPerPlaylist, type PlaylistCandidate } from "./playlist-generator.js";
import { buildSubsonicStreamUrl, getSubsonicConnection, subsonicRequest, navidromeLogin, uploadNavidromePlaylistArtwork, fetchSubsonicSongs, type SubsonicConnection } from "./subsonic-client.js";
import { renderPlaylistArtwork, sanitizeFilename } from "./artwork-renderer.js";
import { buildTrackFilterWhere, syncablePathClause } from "./track-filter.js";
import { deleteGeneratedArtworkFile, pruneGeneratedArtworkFiles, resolveConnection, audioFeaturesFromTrack, isoWeekKey, weeklyRotatedTrackIds, extractSubsonicSongId, hashText } from "./scan.js";
import { syncWorkerConnectionFromSettings, clamp, appendLog } from "./settings.js";
import { generatedArtworkDir, playlistArtworkRenderVersion } from "./config.js";
import { appState, log } from "./state.js";
import { db, readTracksStmt, insertPlaylistStmt, updatePlaylistMetadataStmt, refreshProtectedPlaylistStmt, refreshProtectedPlaylistDescriptionStmt, updatePlaylistArtworkStmt, deletePlaylistByIdStmt, deletePlaylistTracksByPlaylistIdStmt, insertPlaylistTrackStmt, insertPlaylistSnapshotStmt, readPlaylistTrackIdsStmt, markPlaylistsNotRecommendedStmt, findPlaylistBySlugStmt } from "./db.js";
import type { TrackRecord, PlaylistRecord, AudioAnalysis } from "./types.js";

export function getManagedPlaylistNamesBySlug() {
  const rows = db
    .prepare("SELECT slug, name FROM playlists WHERE is_recommended = 1 OR mode = 'pinned'")
    .all() as Array<{ slug: string; name: string }>;
  return new Map(rows.map((row) => [row.slug, row.name]));
}

export function replacePlaylists(candidates: PlaylistCandidate[]) {
  const now = new Date().toISOString();
  appState.previousRecommendedPlaylistNamesBySlug = getManagedPlaylistNamesBySlug();
  const transaction = db.transaction((list: PlaylistCandidate[]) => {
    markPlaylistsNotRecommendedStmt.run();
    for (const playlist of list) {
      const existing = findPlaylistBySlugStmt.get(playlist.slug) as PlaylistRecord | undefined;
      let playlistId = 0;
      if (existing) {
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
      const cappedTrackIds = existing?.mode === "pinned" ? playlist.trackIds.slice(0, appState.settings.maxTracksPerPlaylist) : playlist.trackIds;
      cappedTrackIds.forEach((trackId, position) => {
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

export async function syncPlaylistArtwork(
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
  const imageUrl = "https://picsum.photos/1200";
  const artworkAttribution = "";
  const artworkAttributionUrl = "";
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

export async function syncSinglePlaylist(connection: SubsonicConnection, playlistId: number, now: string) {
  const playlist = db
    .prepare(
      `SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at,
              artwork_path, artwork_attribution, artwork_attribution_url, artwork_signature, artwork_updated_at
       FROM playlists WHERE id = ?`
    )
    .get(playlistId) as PlaylistRecord | undefined;
  if (!playlist) return;
  const trackRows = db
    .prepare(
      `SELECT pt.track_id as trackId, t.path, t.artist FROM playlist_tracks pt
       JOIN tracks t ON t.id = pt.track_id
       WHERE pt.playlist_id = ?
       ORDER BY pt.position`
    )
    .all(playlist.id) as Array<{ trackId: number; path: string; artist: string | null }>;
  const weekKey = isoWeekKey();
  const weeklyTrackIds = weeklyRotatedTrackIds(trackRows, `${playlist.slug}:${weekKey}`, clamp(appState.settings.maxTracksPerPlaylist, 5, 100));
  const pathByTrackId = new Map<number, string>();
  for (const row of trackRows) {
    pathByTrackId.set(row.trackId, row.path);
  }
  const songIds = weeklyTrackIds
    .map((trackId) => pathByTrackId.get(trackId) ?? "")
    .map((pathValue) => extractSubsonicSongId(pathValue))
    .filter((value): value is string => Boolean(value));
  const remoteList = await subsonicRequest<{ playlists?: { playlist?: Array<{ id: string; name: string }> } }>(
    connection,
    "getPlaylists.view"
  );
  const remoteByName = new Map<string, string>();
  for (const rp of remoteList.playlists?.playlist ?? []) {
    remoteByName.set(rp.name, rp.id);
  }
  let remoteId = remoteByName.get(playlist.name) ?? "";
  if (!remoteId) {
    await subsonicRequest(connection, "createPlaylist.view", { name: playlist.name });
    const refreshed = await subsonicRequest<{ playlists?: { playlist?: Array<{ id: string; name: string }> } }>(
      connection,
      "getPlaylists.view"
    );
    for (const rp of refreshed.playlists?.playlist ?? []) {
      if (rp.name === playlist.name) {
        remoteId = rp.id;
        break;
      }
    }
  }
  if (!remoteId) return;
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
  for (const songId of songIds) {
    await subsonicRequest(connection, "updatePlaylist.view", { playlistId: remoteId, songIdToAdd: songId });
  }
  const token = await navidromeLogin(connection);
  await syncPlaylistArtwork(
    connection,
    playlist,
    remoteId,
    playlist.name,
    playlist.description ?? "",
    weeklyTrackIds,
    trackRows,
    async () => token
  );
  appState.previousRecommendedPlaylistNamesBySlug.set(playlist.slug, playlist.name);
}

export async function syncSubsonicPlaylists(connection: SubsonicConnection) {
  const managedPlaylists = db
    .prepare(
      `SELECT id, slug, name, description, mode, selected, sync_target, is_recommended, updated_at,
              artwork_path, artwork_attribution, artwork_attribution_url, artwork_signature, artwork_updated_at
       FROM playlists
       WHERE is_recommended = 1 OR mode = 'pinned'
       ORDER BY CASE mode WHEN 'pinned' THEN 1 ELSE 2 END, name`
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
  const previousManagedNames = new Set(appState.previousRecommendedPlaylistNamesBySlug.values());
  for (const playlist of managedPlaylists) {
    const trackRows = db
      .prepare(
        `SELECT pt.track_id as trackId, t.path, t.artist FROM playlist_tracks pt
         JOIN tracks t ON t.id = pt.track_id
         WHERE pt.playlist_id = ?
         ORDER BY pt.position`
      )
      .all(playlist.id) as Array<{ trackId: number; path: string; artist: string | null }>;
    const weeklyTrackIds = weeklyRotatedTrackIds(trackRows, `${playlist.slug}:${weekKey}`, clamp(appState.settings.maxTracksPerPlaylist, 5, 100));
    const pathByTrackId = new Map<number, string>();
    for (const row of trackRows) {
      pathByTrackId.set(row.trackId, row.path);
    }
    const songIds = weeklyTrackIds
      .map((trackId) => pathByTrackId.get(trackId) ?? "")
      .map((pathValue) => extractSubsonicSongId(pathValue))
      .filter((value): value is string => Boolean(value));
    const targetName = playlist.name;
    const previousName = appState.previousRecommendedPlaylistNamesBySlug.get(playlist.slug) ?? "";
    let remoteId = remoteByName.get(targetName) ?? "";
    if (!remoteId && previousName) {
      remoteId = remoteByName.get(previousName) ?? "";
      if (remoteId && previousName !== targetName) {
        await subsonicRequest(connection, "updatePlaylist.view", { playlistId: remoteId, name: targetName });
        remoteByName.delete(previousName);
        remoteByName.set(targetName, remoteId);
      }
    }
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
  appState.previousRecommendedPlaylistNamesBySlug = new Map(managedPlaylists.map((playlist) => [playlist.slug, playlist.name]));
  return { applied, removed, total: managedPlaylists.length };
}

export async function refreshRecommendedPlaylists(
  weeklyPlaylistCount = appState.workerState.weeklyPlaylistCount,
  generationKey = isoWeekKey()
) {
  if (weeklyPlaylistCount <= 0) {
    appendLog({
      level: "info",
      scope: "playlist",
      message: "Playlist generation skipped (count set to 0)",
      meta: { weeklyPlaylistCount }
    });
    return { candidates: [], sourceTracks: 0 };
  }
  const pinnedSlugs = new Set(
    (
      db.prepare("SELECT slug FROM playlists WHERE mode = 'pinned'").all() as Array<{
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
    album: track.album,
    year: track.year,
    tags_json: track.tags_json,
    audio_features_json: track.audio_features_json,
    audio_vector_json: track.audio_vector_json,
    play_count: track.play_count,
    favorite: track.favorite,
    tags: tagsFromTrack(track)
  }));
  const leastPlayedRows = db.prepare("SELECT id, tags_json, audio_features_json, audio_vector_json, play_count FROM tracks ORDER BY play_count ASC, RANDOM() LIMIT 160").all() as Array<{ id: number; tags_json: string; audio_features_json: string; audio_vector_json: string; play_count: number }>;
  const favoriteRows = db.prepare("SELECT id, tags_json FROM tracks WHERE favorite = 1 ORDER BY play_count DESC LIMIT 30").all() as Array<{ id: number; tags_json: string }>;
  const maxTracksPerPlaylist = appState.settings.maxTracksPerPlaylist;
  const fewestPrioritySlugs = new Set(
    (db.prepare("SELECT slug FROM playlists WHERE fewest_listens_priority = 1").all() as Array<{ slug: string }>).map((row) => row.slug)
  );
  const candidates = await generateCandidates({
    tracks: enrichedTracks,
    desiredWeeklyPlaylists: weeklyPlaylistCount,
    maxTracksPerPlaylist,
    generationKey,
    pinnedSlugs,
    leastPlayedRows,
    favoriteRows,
    fewestListensPrioritySlugs: fewestPrioritySlugs
  });
  const filterPlaylists = db
    .prepare("SELECT id, slug, name, filter_json, seeds_json, fewest_listens_priority FROM playlists WHERE mode = 'pinned' AND filter_json != ''")
    .all() as Array<{ id: number; slug: string; name: string; filter_json: string; seeds_json: string; fewest_listens_priority: number }>;
  for (const fp of filterPlaylists) {
    let filters: Record<string, unknown> = {};
    try { filters = JSON.parse(fp.filter_json) as Record<string, unknown>; } catch { continue; }
    let seedIds: number[] = [];
    try {
      seedIds = (JSON.parse(fp.seeds_json || "[]") as unknown[]).filter((v): v is number => Number.isInteger(v) && (v as number) > 0);
    } catch { seedIds = []; }
    const { where, params } = buildTrackFilterWhere(filters);
    const rows = db
      .prepare(`SELECT id FROM tracks WHERE ${where} AND ${syncablePathClause()} ORDER BY id LIMIT ?`)
      .all(...params, Math.max(appState.settings.maxTracksPerPlaylist - seedIds.length, 1)) as Array<{ id: number }>;
    const seedSet = new Set(seedIds);
    const trackIds = [...seedIds, ...rows.map((row) => row.id).filter((id) => !seedSet.has(id))].slice(0, appState.settings.maxTracksPerPlaylist);
    if (fp.fewest_listens_priority && trackIds.length > 0) {
      const pcRows = db.prepare(`SELECT id, play_count FROM tracks WHERE id IN (${trackIds.map(() => "?").join(",")})`).all(...trackIds) as Array<{ id: number; play_count: number }>;
      const pcById = new Map<number, number>(pcRows.map((row) => [row.id, row.play_count]));
      trackIds.sort((a, b) => (pcById.get(a) ?? 0) - (pcById.get(b) ?? 0));
    }
    if (trackIds.length < 5) continue;
    const description = seedIds.length > 0
      ? `${buildFilterDescription(filters)} · ${seedIds.length} radio seeds`
      : buildFilterDescription(filters);
    refreshProtectedPlaylistDescriptionStmt.run({ id: fp.id, description, updated_at: new Date().toISOString() });
    candidates.push({
      slug: fp.slug,
      name: fp.name,
      description,
      source: "cluster",
      signatureTags: [],
      audioVector: [],
      trackIds
    });
  }
  replacePlaylists(candidates);
  appendLog({
    level: "info",
    scope: "playlist",
    message: "Recommended playlists refreshed",
    meta: { generated: candidates.length, sourceTracks: syncableTracks.length, weeklyPlaylistCount, maxTracksPerPlaylist }
  });
  return { candidates, sourceTracks: syncableTracks.length };
}

export function buildFilterDescription(filters: Record<string, unknown>): string {
  const parts: string[] = [];
  if (Array.isArray(filters.selectedTags) && (filters.selectedTags as string[]).length > 0) {
    parts.push((filters.selectedTags as string[]).join(", "));
  }
  if (typeof filters.bpmMin === "number" && !Number.isNaN(filters.bpmMin as number)) {
    parts.push(`BPM ≥ ${filters.bpmMin}`);
  }
  if (typeof filters.bpmMax === "number" && !Number.isNaN(filters.bpmMax as number)) {
    parts.push(`BPM ≤ ${filters.bpmMax}`);
  }
  if (typeof filters.keyQuery === "string" && filters.keyQuery) {
    parts.push(`key: ${filters.keyQuery}`);
  }
  if (typeof filters.keyMode === "string" && filters.keyMode) {
    parts.push(filters.keyMode as string);
  }
  if (typeof filters.energyMin === "number" && !Number.isNaN(filters.energyMin as number)) {
    parts.push(`energy ≥ ${filters.energyMin}%`);
  }
  if (typeof filters.energyMax === "number" && !Number.isNaN(filters.energyMax as number)) {
    parts.push(`energy ≤ ${filters.energyMax}%`);
  }
  if (typeof filters.yearMin === "number" && !Number.isNaN(filters.yearMin as number)) {
    parts.push(`${filters.yearMin}–`);
  }
  if (typeof filters.yearMax === "number" && !Number.isNaN(filters.yearMax as number)) {
    parts.push(`–${filters.yearMax}`);
  }
  return parts.join(" · ") || "Filter playlist";
}
