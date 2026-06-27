import { normalizeTag, uniqueTags } from "./tags.js";

async function fetchLastFmTagsFromApi(apiKey: string, params: URLSearchParams): Promise<string[]> {
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

async function fetchLastFmArtistSearchImage(
  apiKey: string,
  artist: string,
  isUsableImage: (url: string) => Promise<boolean>
): Promise<{ imageUrl: string; artistUrl: string } | null> {
  const normalizedArtist = normalizeTag(artist);
  if (!apiKey || !normalizedArtist) {
    return null;
  }
  const params = new URLSearchParams({
    method: "artist.search",
    artist,
    api_key: apiKey,
    format: "json",
    limit: "10"
  });
  try {
    const response = await fetch(`https://ws.audioscrobbler.com/2.0/?${params.toString()}`, {
      signal: AbortSignal.timeout(3500)
    });
    if (!response.ok) {
      return null;
    }
    const payload = (await response.json()) as {
      results?: {
        artistmatches?: {
          artist?: Array<{
            name?: string;
            url?: string;
            image?: Array<{ "#text"?: string; size?: string }>;
            image_small?: string;
          }> | {
            name?: string;
            url?: string;
            image?: Array<{ "#text"?: string; size?: string }>;
            image_small?: string;
          };
        };
      };
    };
    const rawMatches = payload.results?.artistmatches?.artist;
    const matches = Array.isArray(rawMatches) ? rawMatches : rawMatches ? [rawMatches] : [];
    const sortedMatches = matches
      .map((match) => ({
        ...match,
        normalizedName: normalizeTag(match.name ?? "")
      }))
      .sort((a, b) => {
        const aExact = a.normalizedName === normalizedArtist ? 0 : 1;
        const bExact = b.normalizedName === normalizedArtist ? 0 : 1;
        return aExact - bExact;
      });
    for (const match of sortedMatches) {
      const candidateUrls = [
        ...((match.image ?? []).map((entry) => (entry["#text"] ?? "").trim()).filter(Boolean)),
        (match.image_small ?? "").trim()
      ].filter(Boolean);
      for (const imageUrl of candidateUrls) {
        if (await isUsableImage(imageUrl)) {
          return {
            imageUrl,
            artistUrl: (match.url ?? "").trim()
          };
        }
      }
    }
    return null;
  } catch {
    return null;
  }
}

export async function fetchLastFmArtistImage(
  apiKey: string,
  artist: string,
  cache: Map<string, { imageUrl: string; artistUrl: string } | null>,
  isUsableImage: (url: string) => Promise<boolean>
): Promise<{ imageUrl: string; artistUrl: string } | null> {
  const normalizedArtist = normalizeTag(artist);
  if (!apiKey || !normalizedArtist) {
    return null;
  }
  if (cache.has(normalizedArtist)) {
    return cache.get(normalizedArtist) ?? null;
  }
  const searchResult = await fetchLastFmArtistSearchImage(apiKey, artist, isUsableImage);
  if (searchResult) {
    cache.set(normalizedArtist, searchResult);
    return searchResult;
  }
  const params = new URLSearchParams({
    method: "artist.getInfo",
    artist,
    api_key: apiKey,
    format: "json",
    autocorrect: "1"
  });
  try {
    const response = await fetch(`https://ws.audioscrobbler.com/2.0/?${params.toString()}`, {
      signal: AbortSignal.timeout(3500)
    });
    if (!response.ok) {
      cache.set(normalizedArtist, null);
      return null;
    }
    const payload = (await response.json()) as {
      artist?: {
        url?: string;
        image?: Array<{ "#text"?: string; size?: string }>;
      };
    };
    const images = payload.artist?.image ?? [];
    const preferredSizes = ["mega", "extralarge", "large", "medium", "small"];
    const match = preferredSizes
      .map((size) => images.find((image) => image.size === size && (image["#text"] ?? "").trim()))
      .find(Boolean)
      ?? images.find((image) => (image["#text"] ?? "").trim());
    const imageUrl = (match?.["#text"] ?? "").trim();
    const artistUrl = (payload.artist?.url ?? "").trim();
    const result = imageUrl ? { imageUrl, artistUrl } : null;
    cache.set(normalizedArtist, result);
    return result;
  } catch {
    cache.set(normalizedArtist, null);
    return null;
  }
}
