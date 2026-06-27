import { isPrimaryClusterTag, normalizeTag, tagWeight, uniqueTags } from "./tags.js";

export type PlaylistCandidate = {
  name: string;
  slug: string;
  description: string;
  trackIds: number[];
  source: "cluster" | "mood" | "artist" | "discovery";
  signatureTags: string[];
  audioVector: number[];
};

const playlistPoolTargetSize = 72;
export const maxArtistPerPlaylist = 1;
export const maxArtistFallbackPerPlaylist = 2;

export function hashSeed(input: string): number {
  let hash = 2166136261;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

export function seededRandom(seed: number): () => number {
  let value = seed || 1;
  return () => {
    value += 0x6d2b79f5;
    let result = Math.imul(value ^ (value >>> 15), value | 1);
    result ^= result + Math.imul(result ^ (result >>> 7), result | 61);
    return ((result ^ (result >>> 14)) >>> 0) / 4294967296;
  };
}

export function tagsFromTrack(track: { tags_json: string }): string[] {
  try {
    const parsed = JSON.parse(track.tags_json) as string[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function audioVectorFromTrack(track: { audio_vector_json: string }): number[] {
  try {
    const parsed = JSON.parse(track.audio_vector_json) as number[];
    return Array.isArray(parsed) ? parsed.filter((value) => Number.isFinite(value)).map((value) => Number(value)) : [];
  } catch {
    return [];
  }
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

export function limitTracksByArtist(
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

function titleCaseWords(value: string): string {
  return value
    .split(/[\s-]+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function vibePrefix(audioVector: number[]): string {
  const energy = audioVector[1] ?? 0;
  const valence = audioVector[3] ?? 0;
  const acousticness = audioVector[4] ?? 0;
  const instrumentalness = audioVector[5] ?? 0;
  if (energy > 0.66) return "Drive";
  if (energy < 0.35) return "Calm";
  if (valence > 0.64) return "Bright";
  if (valence < 0.36) return "Moody";
  if (acousticness > 0.62) return "Warm";
  if (instrumentalness > 0.62) return "Textural";
  return "";
}

function formatTopTags(tags: string[], count: number): string {
  const formatted = tags
    .slice(0, count)
    .map(titleCaseWords)
    .filter((tag, index, list) => list.findIndex((t) => t.toLowerCase() === tag.toLowerCase()) === index);
  return formatted.join(" & ");
}

function buildPlaylistName(input: {
  source: PlaylistCandidate["source"];
  signatureTags: string[];
  audioVector: number[];
  seedArtist?: string;
  moodName?: string;
}): { name: string; description: string } {
  const description = input.signatureTags.slice(0, 3).join(", ");
  const prefix = vibePrefix(input.audioVector);

  if (input.source === "artist") {
    const artist = input.seedArtist ?? "Artist";
    return { name: `${artist} Mix`, description };
  }

  if (input.source === "discovery") {
    const topTag = formatTopTags(input.signatureTags, 1);
    return { name: `Deep Cuts: ${topTag || "Discovery"}`, description };
  }

  const tagLine = formatTopTags(input.signatureTags, 2);
  if (!tagLine) {
    return { name: prefix || "Mixed", description };
  }

  if (input.source === "mood") {
    const moodName = input.moodName ?? input.signatureTags[0] ? titleCaseWords(input.signatureTags[0]) : "Mood";
    if (prefix) {
      return { name: `${moodName} — ${tagLine}`, description };
    }
    return { name: `${moodName} ${tagLine}`, description };
  }

  if (prefix) {
    return { name: `${prefix}: ${tagLine}`, description };
  }

  return { name: tagLine, description };
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

type GenerateCandidatesConfig = {
  tracks: Array<{
    id: number;
    title: string | null;
    artist: string | null;
    tags_json: string;
    audio_vector_json: string;
    play_count: number;
    favorite: number;
    tags: string[];
  }>;
  desiredWeeklyPlaylists: number;
  maxTracksPerPlaylist: number;
  generationKey: string;
  pinnedSlugs: Set<string>;
  lockedSlugs: Set<string>;
  leastPlayedRows: Array<{
    id: number;
    tags_json: string;
    audio_vector_json: string;
    play_count: number;
  }>;
  favoriteRows: Array<{
    id: number;
    tags_json: string;
  }>;
};

export async function generateCandidates(
  config: GenerateCandidatesConfig
): Promise<PlaylistCandidate[]> {
  const { tracks: enriched, desiredWeeklyPlaylists, maxTracksPerPlaylist, generationKey, pinnedSlugs, lockedSlugs, leastPlayedRows, favoriteRows } = config;

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
        clamp(maxTracksPerPlaylist * 2, 24, playlistPoolTargetSize),
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
      clamp(maxTracksPerPlaylist * 2, 24, playlistPoolTargetSize),
      maxArtistFallbackPerPlaylist
    );
    if (pooled.length < 16) {
      continue;
    }
    const { name, description } = buildPlaylistName({
      source: "cluster",
      signatureTags: centroid,
      audioVector: seedAudioVector
    });
    candidates.push({
      name,
      slug: `cluster-${slugify(primaryTag)}`,
      description,
      trackIds: pooled,
      source: "cluster",
      signatureTags: centroid,
      audioVector: averageAudioVector(pooled, audioVectorByTrackId)
    });
  }

  const moods: Array<{ name: string; filter: (v: number[]) => boolean }> = [
    { name: "High Energy", filter: (v) => v[1] > 0.70 },
    { name: "Chill", filter: (v) => v[1] < 0.30 },
    { name: "Melancholic", filter: (v) => v[3] < 0.30 },
    { name: "Bright", filter: (v) => v[3] > 0.70 },
    { name: "Driving", filter: (v) => v[1] > 0.60 && v[1] < 0.85 && v[3] > 0.36 },
    { name: "Textural", filter: (v) => v[4] > 0.60 || v[5] > 0.60 }
  ];
  for (const mood of moods) {
    const seed = enriched.filter((track) => {
      const vec = audioVectorByTrackId.get(track.id);
      return vec && vec.length >= 6 && mood.filter(vec);
    });
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
      centroid.slice(0, 10),
      moodAudioVector,
      trackTagsByTrackId,
      audioVectorByTrackId,
      artistByTrackId,
      playCountByTrackId,
      favoriteByTrackId,
      maxPlayCount
    );
    const signatureTags = topTagsFromTrackIds(pooled, primaryTagsByTrackId, 5);
    const pooledAudioVector = averageAudioVector(pooled, audioVectorByTrackId);
    const { name, description } = buildPlaylistName({
      source: "mood",
      signatureTags,
      audioVector: pooledAudioVector,
      moodName: mood.name
    });
    candidates.push({
      name,
      slug: `mood-${slugify(mood.name)}`,
      description,
      trackIds: pooled,
      source: "mood",
      signatureTags,
      audioVector: pooledAudioVector
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
    const { name, description } = buildPlaylistName({
      source: "artist",
      signatureTags: seedTags,
      audioVector,
      seedArtist
    });
    candidates.push({
      name,
      slug: `artist-${slugify(seedArtist)}`,
      description,
      trackIds: pooled,
      source: "artist",
      signatureTags: seedTags,
      audioVector
    });
  }

  if (leastPlayedRows.length > 24) {
    const favoriteTags = uniqueTags(
      favoriteRows.flatMap((row) => {
        try {
          return JSON.parse(row.tags_json) as string[];
        } catch {
          return [];
        }
      })
    );
    const favoriteAudioVector = averageAudioVector(
      favoriteRows.map((row) => row.id),
      audioVectorByTrackId
    );
    const ranked = leastPlayedRows
      .map((row) => {
        const tags = (() => {
          try {
            return JSON.parse(row.tags_json) as string[];
          } catch {
            return [];
          }
        })();
        const audioVec = audioVectorFromTrack(row);
        const noveltyScore = maxPlayCount > 0 ? 1 - Math.log1p(Math.max(0, row.play_count)) / Math.log1p(maxPlayCount) : 0.5;
        return {
          id: row.id,
          score: cosineScore(tags, favoriteTags) * 0.68 + cosineNumberScore(audioVec, favoriteAudioVector) * 0.2 + noveltyScore * 0.12
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
    const discoveryTags = favoriteTags.slice(0, 8);
    const { name, description } = buildPlaylistName({
      source: "discovery",
      signatureTags: discoveryTags,
      audioVector
    });
    candidates.push({
      name,
      slug: "discovery-mix",
      description,
      trackIds: pooled,
      source: "discovery",
      signatureTags: discoveryTags,
      audioVector
    });
  }

  const limitedPlaylistCount = clamp(desiredWeeklyPlaylists, 1, 5) + pinnedSlugs.size;
  const limitedTrackCount = clamp(maxTracksPerPlaylist, 5, 100);
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
