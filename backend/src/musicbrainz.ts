import { uniqueTags } from "./tags.js";

type MusicBrainzTagResult = {
  recordingTags: string[];
  artistTags: string[];
  year: number | null;
};

async function fetchMusicBrainzArtistTags(cache: Map<string, string[]>, artistMbid: string): Promise<string[]> {
  if (!artistMbid) {
    return [];
  }
  if (cache.has(artistMbid)) {
    return cache.get(artistMbid) ?? [];
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
      cache.set(artistMbid, []);
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
    cache.set(artistMbid, tags);
    return tags;
  } catch {
    cache.set(artistMbid, []);
    return [];
  }
}

export async function fetchMusicBrainzTags(artist: string, title: string, cache: Map<string, string[]>): Promise<MusicBrainzTagResult> {
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
    const artistTags = await fetchMusicBrainzArtistTags(cache, artistMbid);
    return {
      recordingTags,
      artistTags,
      year: Number.isNaN(year) ? null : year
    };
  } catch {
    return { recordingTags: [], artistTags: [], year: null };
  }
}
