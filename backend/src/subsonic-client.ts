import { createHash, randomUUID } from "node:crypto";
import { createLogger } from "./logger.js";

const log = createLogger("subsonic");

export type SubsonicConnection = {
  baseUrl: string;
  username: string;
  password: string;
};

type SubsonicQueryParams = Record<string, string | readonly string[]>;

export type SubsonicSong = {
  id: string;
  title?: string;
  artist?: string;
  album?: string;
  year?: number;
  duration?: number;
  genre?: string;
};

function buildSubsonicQuery(connection: SubsonicConnection, params: SubsonicQueryParams = {}, includeFormat = true) {
  const salt = randomUUID().replace(/-/g, "").slice(0, 12);
  const token = createHash("md5").update(`${connection.password}${salt}`).digest("hex");
  const query = new URLSearchParams({
    u: connection.username,
    t: token,
    s: salt,
    v: "1.16.1",
    c: "sortify",
    ...(includeFormat ? { f: "json" } : {})
  });
  for (const [key, value] of Object.entries(params)) {
    if (typeof value === "string") {
      query.append(key, value);
    } else {
      for (const item of value) {
        query.append(key, item);
      }
    }
  }
  return query;
}

export function buildSubsonicStreamUrl(connection: SubsonicConnection, songId: string) {
  return `${connection.baseUrl}/rest/stream.view?${buildSubsonicQuery(connection, { id: songId }, false).toString()}`;
}

export function getSubsonicConnection(input: unknown, defaults: { baseUrl: string; username: string; password: string }): SubsonicConnection {
  const body = (input ?? {}) as Record<string, unknown>;
  const baseUrl = String(body.baseUrl ?? defaults.baseUrl).trim().replace(/\/+$/, "");
  const username = String(body.username ?? defaults.username).trim();
  const password = String(body.password ?? defaults.password).trim();
  if (!baseUrl || !username || !password) {
    throw new Error("Subsonic connection requires baseUrl, username, and password");
  }
  return { baseUrl, username, password };
}

export async function subsonicRequest<T>(connection: SubsonicConnection, endpoint: string, params: SubsonicQueryParams = {}): Promise<T> {
  const query = buildSubsonicQuery(connection, params);
  const response = await fetch(`${connection.baseUrl}/rest/${endpoint}?${query.toString()}`, {
    signal: AbortSignal.timeout(12000)
  });
  if (!response.ok) {
    log.warn("subsonic request failed", { endpoint, status: response.status });
    throw new Error(`Subsonic request failed: ${response.status}`);
  }
  const payload = (await response.json()) as {
    "subsonic-response"?: {
      status?: string;
      error?: { message?: string };
    } & T;
  };
  const envelope = payload["subsonic-response"];
  if (!envelope || envelope.status !== "ok") {
    const errMsg = envelope?.error?.message ?? "Subsonic API error";
    log.warn("subsonic API error", { endpoint, error: errMsg });
    throw new Error(errMsg);
  }
  return envelope as T;
}

export async function navidromeLogin(connection: SubsonicConnection): Promise<string> {
  const response = await fetch(`${connection.baseUrl}/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      username: connection.username,
      password: connection.password
    }),
    signal: AbortSignal.timeout(12_000)
  });
  const payload = (await response.json()) as { token?: string; error?: string };
  if (!response.ok || !payload.token) {
    log.warn("navidrome login failed", { status: response.status, error: payload.error ?? "no token" });
    throw new Error(payload.error ?? "Navidrome login failed");
  }
  return payload.token;
}

export async function uploadNavidromePlaylistArtwork(
  connection: SubsonicConnection,
  playlistId: string,
  token: string,
  image: Buffer,
  fileName: string
) {
  const form = new FormData();
  form.append("image", new Blob([new Uint8Array(image)], { type: "image/jpeg" }), fileName);
  const response = await fetch(`${connection.baseUrl}/api/playlist/${encodeURIComponent(playlistId)}/image`, {
    method: "POST",
    headers: {
      "X-ND-Authorization": `Bearer ${token}`
    },
    body: form,
    signal: AbortSignal.timeout(20_000)
  });
  if (!response.ok) {
    const body = await response.text();
    log.warn("artwork upload failed", { playlistId, status: response.status, error: body });
    throw new Error(body || `Artwork upload failed: ${response.status}`);
  }
}

export async function fetchSubsonicSongs(connection: SubsonicConnection): Promise<SubsonicSong[]> {
  const songs: SubsonicSong[] = [];
  const pageSize = 500;
  log.debug("fetching songs from subsonic", { baseUrl: connection.baseUrl });
  let offset = 0;
  while (true) {
    const list = await subsonicRequest<{ albumList2?: { album?: Array<{ id: string }> } }>(connection, "getAlbumList2.view", {
      type: "alphabeticalByName",
      size: String(pageSize),
      offset: String(offset)
    });
    const albums = list.albumList2?.album ?? [];
    if (!albums.length) {
      break;
    }
    for (const album of albums) {
      const detail = await subsonicRequest<{ album?: { song?: SubsonicSong[] } }>(connection, "getAlbum.view", { id: album.id });
      songs.push(...(detail.album?.song ?? []));
    }
    if (albums.length < pageSize) {
      break;
    }
    offset += pageSize;
  }
  log.debug("fetched songs from subsonic", { count: songs.length });
  return songs;
}
