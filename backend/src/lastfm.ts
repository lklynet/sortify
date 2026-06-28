import { normalizeTag, uniqueTags } from "./tags.js";
import { createLogger } from "./logger.js";
import { createRateLimiter } from "./rate-limiter.js";

const log = createLogger("lastfm");
const rateLimit = createRateLimiter(250);

async function fetchLastFmTagsFromApi(apiKey: string, params: URLSearchParams): Promise<string[]> {
  if (!apiKey) {
    return [];
  }
  params.set("api_key", apiKey);
  params.set("format", "json");
  params.set("autocorrect", "1");
  const url = `https://ws.audioscrobbler.com/2.0/?${params.toString()}`;
  try {
    const response = await rateLimit(() => fetch(url, { signal: AbortSignal.timeout(3500) }));
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
  } catch (err) {
    log.warn("lastfm tags fetch failed", { method: params.get("method"), artist: params.get("artist"), track: params.get("track"), err });
    return [];
  }
}

type LastFmTagResult = {
  trackTags: string[];
  artistTags: string[];
  albumTags: string[];
};

export async function fetchLastFmTags(
  apiKey: string,
  artist: string,
  title: string,
  album: string | null | undefined,
  caches: {
    artistTags: Map<string, string[]>;
    albumTags: Map<string, string[]>;
  }
): Promise<LastFmTagResult> {
  if (!artist || !title) {
    return { trackTags: [], artistTags: [], albumTags: [] };
  }
  const normalizedArtist = normalizeTag(artist);
  const normalizedAlbum = album ? normalizeTag(album) : "";

  const trackTagsPromise = fetchLastFmTagsFromApi(
    apiKey,
    new URLSearchParams({ method: "track.getTopTags", artist, track: title })
  );

  const artistTagsPromise = (() => {
    if (caches.artistTags.has(normalizedArtist)) {
      return Promise.resolve(caches.artistTags.get(normalizedArtist) ?? []);
    }
    return fetchLastFmTagsFromApi(
      apiKey,
      new URLSearchParams({ method: "artist.getTopTags", artist })
    ).then((tags) => {
      caches.artistTags.set(normalizedArtist, tags);
      return tags;
    });
  })();

  const albumTagsPromise = (() => {
    if (!album || album === "Unknown Album") {
      return Promise.resolve([] as string[]);
    }
    const key = `${normalizedArtist}::${normalizedAlbum}`;
    if (caches.albumTags.has(key)) {
      return Promise.resolve(caches.albumTags.get(key) ?? []);
    }
    return fetchLastFmTagsFromApi(
      apiKey,
      new URLSearchParams({ method: "album.getTopTags", artist, album })
    ).then((tags) => {
      caches.albumTags.set(key, tags);
      return tags;
    });
  })();

  const [trackTags, artistTags, albumTags] = await Promise.all([trackTagsPromise, artistTagsPromise, albumTagsPromise]);
  return { trackTags, artistTags, albumTags };
}
