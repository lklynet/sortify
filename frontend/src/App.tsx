import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Pin } from "lucide-react";
import "./App.css";

type Stats = { tracks: number; playlists: number; analyzedTracks: number };
type AudioFeatures = {
  bpm: number | null;
  energy: number;
  danceability: number;
  valence: number;
  acousticness: number;
  instrumentalness: number;
  brightness: number;
  rhythmicDensity: number;
  loudness: number;
  moodTags: string[];
  durationSampled: number;
};
type OperationLog = {
  id: string;
  timestamp: string;
  level: "info" | "warn" | "error";
  scope: "scan" | "playlist" | "system";
  message: string;
  meta?: Record<string, unknown>;
};
type Playlist = {
  id: number;
  slug: string;
  name: string;
  description: string;
  mode: "dynamic" | "pinned";
  updated_at: string;
  artworkUrl: string | null;
  artworkAttribution: string | null;
  artworkAttributionUrl: string | null;
  tracks: Array<{ id: number; title: string; artist: string; album: string; path: string }>;
};
type Snapshot = {
  id: number;
  playlistId: number;
  slug: string;
  name: string;
  trackIds: number[];
  trackCount: number;
  snapshotWeek: string;
  createdAt: string;
};
type LibraryStats = {
  trackCount: number;
  analyzedCount: number;
  topTags: Array<{ tag: string; count: number }>;
  audioDistributions: Record<string, number[]>;
  keyDistribution: Array<{ key: string; count: number }>;
  decadeSpread: Array<{ decade: string; count: number }>;
  languageBreakdown: Array<{ language: string; count: number }>;
  topGenres: Array<{ genre: string; count: number }>;
  bpmStats: { min: number; max: number; avg: number; median: number };
};
type Track = {
  id: number;
  title: string;
  artist: string;
  album: string;
  tags: string[];
  audioFeatures: AudioFeatures | null;
  audioVector: number[];
  analysis_updated_at?: string | null;
};
type WorkerState = {
  running: boolean;
  weeklyPlaylistCount: number;
  lastRunAt: string | null;
  lastRunWeek: string | null;
  nextRunAt: string | null;
  error: string | null;
  connectionConfigured: boolean;
  cycleSchedule?: string;
};
type Settings = {
  navidromeUrl: string;
  navidromeUsername: string;
  navidromePassword: string;
  hasNavidromePassword: boolean;
  lastFmApiKey: string;
  hasLastFmApiKey: boolean;
  weeklyPlaylistCount: number;
  maxTracksPerPlaylist: number;
  cycleSchedule: string;
};
type ConfirmRefreshModal =
  | {
      action: "analysis" | "playlists";
      title: string;
      message: string;
      confirmLabel: string;
    }
  | null;

const apiBase = import.meta.env.VITE_API_BASE ?? "";

async function callApi<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${apiBase}${url}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) }
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error ?? "Request failed");
  }
  return payload as T;
}

function assetUrl(url: string | null) {
  if (!url) {
    return "";
  }
  if (url.startsWith("http://") || url.startsWith("https://")) {
    return url;
  }
  return `${apiBase}${url}`;
}

function formatPercent(value: number) {
  return `${Math.round((Number.isFinite(value) ? value : 0) * 100)}%`;
}

function formatBpm(value: number | null) {
  return value && Number.isFinite(value) ? `${Math.round(value)} BPM` : "BPM n/a";
}

function hasAudioAnalysis(track: Track) {
  return Boolean(track.audioFeatures && Array.isArray(track.audioFeatures.moodTags));
}

function App() {
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [settings, setSettings] = useState<Settings>({
    navidromeUrl: "",
    navidromeUsername: "",
    navidromePassword: "",
    hasNavidromePassword: false,
    lastFmApiKey: "",
    hasLastFmApiKey: false,
    weeklyPlaylistCount: 3,
    maxTracksPerPlaylist: 20,
    cycleSchedule: "weekly"
  });
  const [settingsDraft, setSettingsDraft] = useState<Settings>({
    navidromeUrl: "",
    navidromeUsername: "",
    navidromePassword: "",
    hasNavidromePassword: false,
    lastFmApiKey: "",
    hasLastFmApiKey: false,
    weeklyPlaylistCount: 3,
    maxTracksPerPlaylist: 20,
    cycleSchedule: "weekly"
  });
  const [settingsDirty, setSettingsDirty] = useState(false);
  const [stats, setStats] = useState<Stats>({ tracks: 0, playlists: 0, analyzedTracks: 0 });
  const [logs, setLogs] = useState<OperationLog[]>([]);
  const [tracks, setTracks] = useState<Track[]>([]);
  const [playlists, setPlaylists] = useState<Playlist[]>([]);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [libraryStats, setLibraryStats] = useState<LibraryStats | null>(null);
  const [playlistTab, setPlaylistTab] = useState<"playlists" | "history" | "stats" | "filter">("playlists");
  const [worker, setWorker] = useState<WorkerState | null>(null);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [toastEntry, setToastEntry] = useState<OperationLog | null>(null);
  const [confirmRefreshModal, setConfirmRefreshModal] = useState<ConfirmRefreshModal>(null);
  const playlistReorderOnNextLoadRef = useRef(true);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<Track[] | null>(null);
  const [expandedTrackId, setExpandedTrackId] = useState<number | null>(null);
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [allTracks, setAllTracks] = useState<Track[]>([]);
  const [allTracksLoaded, setAllTracksLoaded] = useState(false);
  const [filterParams, setFilterParams] = useState({ selectedTags: [] as string[], bpmMin: NaN, bpmMax: NaN, keyQuery: "", keyMode: "" as "" | "major" | "minor", energyMin: NaN, energyMax: NaN, yearMin: NaN, yearMax: NaN });
  const [tagInputText, setTagInputText] = useState("");
  const [tagInputFocused, setTagInputFocused] = useState(false);
  const [savingFilterPlaylist, setSavingFilterPlaylist] = useState(false);
  const [filterPlaylistName, setFilterPlaylistName] = useState("");

  useEffect(() => {
    if (!searchQuery.trim()) {
      setSearchResults(null);
      return;
    }
    setExpandedTrackId(null);
    const q = searchQuery.trim();
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(() => {
      callApi<Track[]>(`/api/tracks/search?q=${encodeURIComponent(q)}`)
        .then(setSearchResults)
        .catch(() => null);
    }, 300);
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    };
  }, [searchQuery]);
  const seenToastIdsRef = useRef<Set<string>>(new Set());

  useEffect(() => {
    if (playlistTab === "filter" && !allTracksLoaded) {
      callApi<Track[]>("/api/tracks/all")
        .then((t) => { setAllTracks(t); setAllTracksLoaded(true); })
        .catch(() => null);
    }
  }, [playlistTab, allTracksLoaded]);

  const tagSuggestions = (() => {
    const sourceTracks = filterParams.selectedTags.length > 0
      ? allTracks.filter((t) => filterParams.selectedTags.every((needle) => t.tags.some((tag) => tag.toLowerCase().includes(needle))))
      : allTracks;
    const set = new Set<string>();
    for (const t of sourceTracks) {
      for (const tag of t.tags) set.add(tag);
    }
    const pool = [...set].sort((a, b) => a.localeCompare(b)).filter((t) => !filterParams.selectedTags.includes(t));
    if (!tagInputText.trim()) return pool.slice(0, 20);
    const q = tagInputText.toLowerCase();
    return pool.filter((tag) => tag.toLowerCase().includes(q)).slice(0, 20);
  })();

  const addTag = (tag: string) => {
    const clean = tag.trim().toLowerCase();
    if (!clean) return;
    setFilterParams((p) => {
      if (p.selectedTags.includes(clean)) return p;
      return { ...p, selectedTags: [...p.selectedTags, clean] };
    });
    setTagInputText("");
  };

  const removeTag = (tag: string) => {
    setFilterParams((p) => ({ ...p, selectedTags: p.selectedTags.filter((t) => t !== tag) }));
  };

  const filteredTracks = (() => {
    if (allTracks.length === 0) return allTracks;
    const p = filterParams;
    const hasFilters = p.selectedTags.length > 0 || !Number.isNaN(p.bpmMin) || !Number.isNaN(p.bpmMax) || p.keyQuery || p.keyMode || !Number.isNaN(p.energyMin) || !Number.isNaN(p.energyMax) || !Number.isNaN(p.yearMin) || !Number.isNaN(p.yearMax);
    if (!hasFilters) return allTracks;
    return allTracks.filter((track) => {
      if (p.selectedTags.length > 0) {
        if (!p.selectedTags.every((needle) => track.tags.some((tag) => tag.toLowerCase().includes(needle)))) return false;
      }
      if (!Number.isNaN(p.bpmMin) && (track.audioFeatures?.bpm ?? 0) < p.bpmMin) return false;
      if (!Number.isNaN(p.bpmMax) && (track.audioFeatures?.bpm ?? 0) > p.bpmMax) return false;
      if (p.keyQuery) {
        const qk = p.keyQuery.toLowerCase();
        if (!(track.audioFeatures?.key ?? "").toLowerCase().includes(qk) && !(track.audioFeatures?.camelotKey ?? "").toLowerCase().includes(qk)) return false;
      }
      if (p.keyMode) {
        const key = (track.audioFeatures?.key ?? "").toLowerCase();
        const isKeyMinor = key.includes("m") && !key.includes("major");
        const isKeyMajor = key && !isKeyMinor;
        if (p.keyMode === "minor" && !isKeyMinor) return false;
        if (p.keyMode === "major" && !isKeyMajor) return false;
      }
      if (!Number.isNaN(p.energyMin) && (track.audioFeatures?.energy ?? 0) * 100 < p.energyMin) return false;
      if (!Number.isNaN(p.energyMax) && (track.audioFeatures?.energy ?? 0) * 100 > p.energyMax) return false;
      if (!Number.isNaN(p.yearMin) && (track.year ?? 0) < p.yearMin) return false;
      if (!Number.isNaN(p.yearMax) && (track.year ?? 0) > p.yearMax) return false;
      return true;
    });
  })();
  const hasLoadedLogsRef = useRef(false);

  const hasNavidromePassword = Boolean(settingsDraft.navidromePassword.trim() || settingsDraft.hasNavidromePassword);
  const hasSubsonicConnection = Boolean(
    settingsDraft.navidromeUrl.trim() && settingsDraft.navidromeUsername.trim() && hasNavidromePassword
  );
  const startReady = hasSubsonicConnection;
  const keyStatus = useMemo(
    () => ({
      lastfm: Boolean(settingsDraft.lastFmApiKey.trim() || settingsDraft.hasLastFmApiKey)
    }),
    [settingsDraft.hasLastFmApiKey, settingsDraft.lastFmApiKey]
  );
  const mergeSettingsDraft = useCallback(
    (nextSettings: Settings, currentDraft: Settings) => ({
      ...nextSettings,
      navidromePassword: currentDraft.navidromePassword,
      lastFmApiKey: currentDraft.lastFmApiKey,
      hasNavidromePassword: nextSettings.hasNavidromePassword || currentDraft.navidromePassword.trim().length > 0,
      hasLastFmApiKey: nextSettings.hasLastFmApiKey || currentDraft.lastFmApiKey.trim().length > 0
    }),
    []
  );
  const settingsPayload = useMemo(() => {
    const nextSettings: Record<string, string | number> = {
      navidromeUrl: settingsDraft.navidromeUrl.trim(),
      navidromeUsername: settingsDraft.navidromeUsername.trim(),
      weeklyPlaylistCount: settingsDraft.weeklyPlaylistCount,
      maxTracksPerPlaylist: settingsDraft.maxTracksPerPlaylist,
      cycleSchedule: settingsDraft.cycleSchedule
    };
    if (settingsDraft.navidromePassword.trim()) {
      nextSettings.navidromePassword = settingsDraft.navidromePassword.trim();
    }
    if (settingsDraft.lastFmApiKey.trim()) {
      nextSettings.lastFmApiKey = settingsDraft.lastFmApiKey.trim();
    }
    return nextSettings;
  }, [settingsDraft]);
  const connectionPayload = useMemo(() => {
    const payload: Record<string, string> = {
      baseUrl: settingsDraft.navidromeUrl.trim(),
      username: settingsDraft.navidromeUsername.trim()
    };
    if (settingsDraft.navidromePassword.trim()) {
      payload.password = settingsDraft.navidromePassword.trim();
    }
    return payload;
  }, [settingsDraft.navidromePassword, settingsDraft.navidromeUrl, settingsDraft.navidromeUsername]);

  const loadDashboardData = useCallback(async () => {
    const [fetchedStats, fetchedTracks, fetchedPlaylists, fetchedSnapshots, fetchedLibraryStats, fetchedWorker, fetchedSettings] = await Promise.all([
      callApi<Stats>("/api/stats"),
      callApi<Track[]>("/api/tracks?limit=20"),
      callApi<Playlist[]>("/api/playlists"),
      callApi<Snapshot[]>("/api/playlists/snapshots"),
      callApi<LibraryStats>("/api/stats/library"),
      callApi<WorkerState>("/api/worker"),
      callApi<Settings>("/api/settings")
    ]);
    setStats(fetchedStats);
    setTracks(fetchedTracks);
    setSnapshots(fetchedSnapshots);
    setLibraryStats(fetchedLibraryStats);
    setPlaylists((currentPlaylists) => {
      if (playlistReorderOnNextLoadRef.current || !currentPlaylists.length) {
        playlistReorderOnNextLoadRef.current = false;
        return fetchedPlaylists;
      }
      const fetchedById = new Map(fetchedPlaylists.map((playlist) => [playlist.id, playlist]));
      const preservedOrder = currentPlaylists
        .map((playlist) => fetchedById.get(playlist.id))
        .filter((playlist): playlist is Playlist => Boolean(playlist));
      const existingIds = new Set(currentPlaylists.map((playlist) => playlist.id));
      const appended = fetchedPlaylists.filter((playlist) => !existingIds.has(playlist.id));
      return [...preservedOrder, ...appended];
    });
    setWorker(fetchedWorker);
    setSettings(fetchedSettings);
    if (!settingsOpen || !settingsDirty) {
      setSettingsDraft((currentDraft) => mergeSettingsDraft(fetchedSettings, currentDraft));
      setSettingsDirty(false);
    }
  }, [mergeSettingsDraft, settingsDirty, settingsOpen]);

  const loadLogs = useCallback(async () => {
    const fetchedLogs = await callApi<OperationLog[]>("/api/ops?limit=120");
    setLogs(fetchedLogs);
  }, []);

  useEffect(() => {
    Promise.all([loadDashboardData(), loadLogs()]).catch((loadError) => {
      setError(loadError instanceof Error ? loadError.message : "Failed loading data");
    });
  }, [loadDashboardData, loadLogs]);

  useEffect(() => {
    const timer = setInterval(() => {
      Promise.all([loadDashboardData(), loadLogs()]).catch(() => null);
    }, 4000);
    return () => clearInterval(timer);
  }, [loadDashboardData, loadLogs]);

  useEffect(() => {
    if (!logs.length) {
      return;
    }
    if (!hasLoadedLogsRef.current) {
      hasLoadedLogsRef.current = true;
      seenToastIdsRef.current = new Set(logs.map((entry) => entry.id));
      return;
    }
    const nextToast = logs.find((entry) => !seenToastIdsRef.current.has(entry.id));
    if (!nextToast) {
      return;
    }
    seenToastIdsRef.current.add(nextToast.id);
    setToastEntry(nextToast);
  }, [logs]);

  useEffect(() => {
    if (!toastEntry) {
      return;
    }
    const timer = window.setTimeout(() => {
      setToastEntry((current) => (current?.id === toastEntry.id ? null : current));
    }, 4000);
    return () => window.clearTimeout(timer);
  }, [toastEntry]);

  const runAction = async (action: string, fn: () => Promise<void>) => {
    setBusyAction(action);
    setError(null);
    try {
      await fn();
      await Promise.all([loadDashboardData(), loadLogs()]);
    } catch (actionError) {
      setError(actionError instanceof Error ? actionError.message : "Action failed");
    } finally {
      setBusyAction(null);
    }
  };

  const persistSettings = useCallback(async () => {
    const savedSettings = await callApi<Settings>("/api/settings", {
      method: "PATCH",
      body: JSON.stringify(settingsPayload)
    });
    setSettings(savedSettings);
    setSettingsDraft((currentDraft) => mergeSettingsDraft(savedSettings, currentDraft));
    setSettingsDirty(false);
    return savedSettings;
  }, [mergeSettingsDraft, settingsPayload]);

  const onStartWorker = () =>
    runAction("start", async () => {
      await persistSettings();
      await callApi("/api/worker/start", {
        method: "POST",
        body: JSON.stringify({
          ...connectionPayload,
          weeklyPlaylistCount: settingsDraft.weeklyPlaylistCount
        })
      });
    });

  const onStopWorker = () =>
    runAction("stop", async () => {
      await callApi("/api/worker/stop", { method: "POST", body: JSON.stringify({}) });
    });

  const saveSettings = () =>
    runAction("save-settings", async () => {
      await persistSettings();
    });

  const refreshAnalyzedTracks = () =>
    runAction("refresh-analysis", async () => {
      await persistSettings();
      await callApi("/api/scan", {
        method: "POST",
        body: JSON.stringify({ ...connectionPayload, forceRefreshAnalysis: true })
      });
    });

  const refreshPlaylists = () =>
    runAction("refresh-playlists", async () => {
      await persistSettings();
      await callApi("/api/playlists/generate", {
        method: "POST",
        body: JSON.stringify({ weeklyPlaylistCount: settingsDraft.weeklyPlaylistCount })
      });
      if (hasSubsonicConnection) {
        await callApi("/api/playlists/apply", {
          method: "POST",
          body: JSON.stringify(connectionPayload)
        });
      }
      playlistReorderOnNextLoadRef.current = true;
    });

  const updatePlaylistMode = (playlistId: number, mode: Playlist["mode"]) =>
    runAction(`playlist-mode-${playlistId}-${mode}`, async () => {
      await callApi(`/api/playlists/${playlistId}`, {
        method: "PATCH",
        body: JSON.stringify({ mode })
      });
    });

  const deletePlaylist = (playlistId: number) =>
    runAction(`playlist-delete-${playlistId}`, async () => {
      const body = hasSubsonicConnection ? JSON.stringify({ connection: connectionPayload }) : undefined;
      await callApi(`/api/playlists/${playlistId}`, { method: "DELETE", body });
      setPlaylists((prev) => prev.filter((p) => p.id !== playlistId));
    });

  const refreshSinglePlaylist = (playlistId: number) =>
    runAction(`playlist-refresh-${playlistId}`, async () => {
      const body = hasSubsonicConnection ? JSON.stringify({ connection: connectionPayload }) : undefined;
      await callApi(`/api/playlists/${playlistId}/refresh`, { method: "POST", body });
      playlistReorderOnNextLoadRef.current = true;
      const fetched = await callApi<Playlist[]>("/api/playlists");
      if (fetched) setPlaylists(fetched);
    });

  const regenerateArtwork = (playlistId: number) =>
    runAction(`artwork-${playlistId}`, async () => {
      const body = hasSubsonicConnection ? JSON.stringify({ connection: connectionPayload }) : undefined;
      const result = await callApi<{ artworkUrl: string }>(`/api/playlists/${playlistId}/artwork`, { method: "POST", body });
      if (result?.artworkUrl) {
        setPlaylists((prev) => prev.map((p) => p.id === playlistId ? { ...p, artworkUrl: result.artworkUrl } : p));
      }
    });

  const saveFilterPlaylist = (name: string) =>
    runAction("save-filter-playlist", async () => {
      const payload: Record<string, unknown> = { name, filters: filterParams };
      if (hasSubsonicConnection) payload.connection = connectionPayload;
      await callApi("/api/playlists/filter", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      setSavingFilterPlaylist(false);
      setFilterPlaylistName("");
      const fetched = await callApi("/api/playlists");
      if (fetched) setPlaylists(fetched);
    });

  const openRefreshConfirmation = (action: "analysis" | "playlists") => {
    setConfirmRefreshModal(
      action === "analysis"
        ? {
            action,
            title: "Refresh analyzed tracks?",
            message: "This will run a full scan and force audio analysis to run again for discovered tracks.",
            confirmLabel: "Refresh analysis"
          }
        : {
            action,
            title: "Refresh playlists?",
            message: "This will generate a fresh set of recommended playlists and apply them to Navidrome if syncing is configured.",
            confirmLabel: "Refresh playlists"
          }
    );
  };

  const confirmRefreshAction = async () => {
    const pendingAction = confirmRefreshModal?.action;
    setConfirmRefreshModal(null);
    if (pendingAction === "analysis") {
      await refreshAnalyzedTracks();
      return;
    }
    if (pendingAction === "playlists") {
      await refreshPlaylists();
    }
  };

  return (
    <main className="layout">
      <header className="app-header">
        <div className="brand-lockup">
          <img className="app-logo" src="/sortify_logo.svg" alt="Sortify" />
          <div className="app-title">
            <h1>Sortify</h1>
          </div>
        </div>
        <div className="header-controls">
          <span className={`worker-pill ${worker?.running ? "on" : "off"}`}>{worker?.running ? "Worker On" : "Worker Off"}</span>
          <button
            className="settings-button"
            onClick={() => {
              setSettingsOpen((current) => {
                const next = !current;
                if (next) {
                  setSettingsDraft((currentDraft) => mergeSettingsDraft(settings, currentDraft));
                  setSettingsDirty(false);
                }
                return next;
              });
            }}
            disabled={busyAction !== null}
          >
            ⚙
          </button>
        </div>
      </header>

      {settingsOpen ? (
        <section className="panel settings-dropdown">
          <div className="panel-header">
            <h3>Settings</h3>
          </div>
          <div className="panel-content">
            <div className="settings-groups">
              <section className="settings-group">
                <div className="settings-group-header">
                  <h4>Navidrome</h4>
                </div>
                <div className="controls-grid">
                  <div className="field">
                    <label htmlFor="navidromeUrl">Navidrome URL</label>
                    <input
                      id="navidromeUrl"
                      value={settingsDraft.navidromeUrl}
                      onChange={(event) => {
                        setSettingsDraft((prev) => ({ ...prev, navidromeUrl: event.target.value }));
                        setSettingsDirty(true);
                      }}
                      placeholder="http://localhost:4533"
                    />
                  </div>
                  <div className="field">
                    <label htmlFor="navidromeUsername">Username</label>
                    <input
                      id="navidromeUsername"
                      value={settingsDraft.navidromeUsername}
                      onChange={(event) => {
                        setSettingsDraft((prev) => ({ ...prev, navidromeUsername: event.target.value }));
                        setSettingsDirty(true);
                      }}
                      placeholder="admin"
                    />
                  </div>
                  <div className="field">
                    <label htmlFor="navidromePassword">Password</label>
                    <input
                      id="navidromePassword"
                      type="password"
                      value={settingsDraft.navidromePassword}
                      onChange={(event) => {
                        setSettingsDraft((prev) => ({ ...prev, navidromePassword: event.target.value }));
                        setSettingsDirty(true);
                      }}
                      placeholder={settingsDraft.hasNavidromePassword ? "****************" : "enter password"}
                    />
                  </div>
                </div>
              </section>

              <section className="settings-group">
                <div className="settings-group-header">
                  <h4>API Keys</h4>
                </div>
                <div className="controls-grid">
                  <div className="field">
                    <label htmlFor="lastFmApiKey">Last.fm API Key</label>
                    <input
                      id="lastFmApiKey"
                      type="password"
                      value={settingsDraft.lastFmApiKey}
                      onChange={(event) => {
                        setSettingsDraft((prev) => ({ ...prev, lastFmApiKey: event.target.value }));
                        setSettingsDirty(true);
                      }}
                      placeholder={settingsDraft.hasLastFmApiKey ? "****************" : "enter Last.fm key"}
                    />
                  </div>
                </div>
              </section>

              <section className="settings-group">
                <div className="settings-group-header">
                  <h4>App Settings</h4>
                </div>
                <div className="controls-grid settings-compact-grid">
                  <div className="field">
                    <label htmlFor="weeklyPlaylistCount">Weekly playlists</label>
                    <select
                      id="weeklyPlaylistCount"
                      value={settingsDraft.weeklyPlaylistCount}
                      onChange={(event) => {
                        setSettingsDraft((prev) => ({ ...prev, weeklyPlaylistCount: Math.max(0, Math.min(8, Number(event.target.value))) }));
                        setSettingsDirty(true);
                      }}
                    >
                      {[0, 1, 2, 3, 4, 5, 6, 7, 8].map((count) => (
                        <option key={count} value={count}>
                          {count === 0 ? "None" : count}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="field">
                    <label htmlFor="maxTracksPerPlaylist">Max tracks per playlist</label>
                    <input
                      id="maxTracksPerPlaylist"
                      type="number"
                      min={5}
                      max={100}
                      value={settingsDraft.maxTracksPerPlaylist}
                      onChange={(event) => {
                        const value = Number(event.target.value);
                        setSettingsDraft((prev) => ({ ...prev, maxTracksPerPlaylist: Math.max(5, Math.min(100, Number.isFinite(value) ? value : 20)) }));
                        setSettingsDirty(true);
                      }}
                    />
                  </div>
                  <div className="field">
                    <label htmlFor="cycleSchedule">Cycle schedule</label>
                    <select
                      id="cycleSchedule"
                      value={settingsDraft.cycleSchedule}
                      onChange={(event) => {
                        setSettingsDraft((prev) => ({ ...prev, cycleSchedule: event.target.value }));
                        setSettingsDirty(true);
                      }}
                    >
                      <option value="every-12-hours">Every 12 hours</option>
                      <option value="daily">Daily</option>
                      <option value="every-3-days">Every 3 days</option>
                      <option value="twice-weekly">Twice weekly</option>
                      <option value="weekly">Weekly</option>
                    </select>
                  </div>
                </div>
              </section>
            </div>
            <div className="actions">
              <button onClick={saveSettings} disabled={busyAction !== null}>
                {busyAction === "save-settings" ? "Saving…" : "Save Settings"}
              </button>
            </div>
          </div>
        </section>
      ) : null}

      <section className="panel control-primary">
        <div className="panel-content">
          <div className="primary-top">
            <div>
              <h2>Automation Worker</h2>
            </div>
            {worker?.running ? (
              <button onClick={onStopWorker} disabled={busyAction !== null} className="primary-action stop">
                {busyAction === "stop" ? "Stopping…" : "Stop"}
              </button>
            ) : (
              <button onClick={onStartWorker} disabled={!startReady || busyAction !== null} className="primary-action start">
                {busyAction === "start" ? "Starting…" : "Start"}
              </button>
            )}
          </div>
          <div className="signal-row">
            <span className={`signal ${hasSubsonicConnection ? "ok" : "bad"}`}>Navidrome {hasSubsonicConnection ? "Ready" : "Missing"}</span>
            <span className={`signal ${keyStatus.lastfm ? "ok" : "warn"}`}>Last.fm {keyStatus.lastfm ? "Ready" : "Optional"}</span>
            <span className={`signal ${stats.analyzedTracks ? "ok" : "warn"}`}>Analyzed {stats.analyzedTracks}/{stats.tracks}</span>
            <span className="signal neutral">Schedule {settings.cycleSchedule === "every-12-hours" ? "12h" : settings.cycleSchedule === "daily" ? "Daily" : settings.cycleSchedule === "every-3-days" ? "3d" : settings.cycleSchedule === "twice-weekly" ? "2×/wk" : "Weekly"}</span>
            <span className="signal neutral">Max Tracks {settings.maxTracksPerPlaylist}</span>
          </div>
          {worker ? (
            <p className="status">
              Worker {worker.running ? "running" : "stopped"} · next refresh{" "}
              {worker.nextRunAt ? new Date(worker.nextRunAt).toLocaleString() : "n/a"}
            </p>
          ) : null}
          {worker?.lastRunAt ? <p className="status">Last completed cycle {new Date(worker.lastRunAt).toLocaleString()}</p> : null}
          {worker?.error ? <p className="error">{worker.error}</p> : null}
          {error ? <p className="error">{error}</p> : null}
        </div>
      </section>

      <section className="content-grid">
        <article className="panel">
          <div className="panel-header panel-header-actions">
            <h3>Recently tagged tracks</h3>
            <div className="panel-header-right">
              <input
                className="searchInput"
                type="search"
                placeholder="Search tracks…"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
              <button
                className="ghost-button icon-button"
                onClick={() => openRefreshConfirmation("analysis")}
                disabled={!hasSubsonicConnection || busyAction !== null}
                aria-label="Force refresh analysis"
                title="Force refresh analysis"
              >
                {busyAction === "refresh-analysis" ? "…" : "↻"}
              </button>
            </div>
          </div>
          <div className="panel-content" style={{ padding: 0 }}>
            {(() => {
              const displayTracks = searchResults ?? tracks;
              if (searchResults !== null && displayTracks.length === 0) {
                return <p className="emptyState">No tracks found for "{searchQuery}".</p>;
              }
              return (
                <ul className="trackList">
                  {displayTracks.map((track) => {
                    const expanded = expandedTrackId === track.id;
                    const analysis = hasAudioAnalysis(track) ? track.audioFeatures : null;
                    return (
                      <li key={track.id}>
                        <div
                          className={`trackRow ${expanded ? "trackRowExpanded" : ""}`}
                          onClick={() => setExpandedTrackId(expanded ? null : track.id)}
                          role="button"
                          tabIndex={0}
                          onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setExpandedTrackId(expanded ? null : track.id); } }}
                        >
                          <div className="trackHead">
                            <div>
                              <strong>{track.title || "Unknown Title"}</strong>
                              <span>
                                {track.artist || "Unknown Artist"} • {track.album || "Unknown Album"}
                              </span>
                            </div>
                            <span className={`trackAnalysisPill ${analysis ? "on" : "off"}`}>
                              {analysis ? "Analyzed" : "Metadata"}
                            </span>
                          </div>
                          <p>{track.tags.slice(0, 6).join(" · ") || "untagged"}</p>
                        </div>
                        {expanded && (
                          <div className="trackDetail">
                            {track.tags.length > 0 && (
                              <div className="trackDetailSection">
                                <strong>Tags</strong>
                                <p>{track.tags.join(" · ")}</p>
                              </div>
                            )}
                            {analysis ? (
                              <>
                                <div className="trackDetailSection">
                                  <strong>Audio Features</strong>
                                  <div className="analysisMetrics">
                                    <span>{formatBpm(analysis.bpm)}</span>
                                    <span>Energy {formatPercent(analysis.energy)}</span>
                                    <span>Dance {formatPercent(analysis.danceability)}</span>
                                    <span>Valence {formatPercent(analysis.valence)}</span>
                                    <span>Acoustic {formatPercent(analysis.acousticness)}</span>
                                    <span>Instrumental {formatPercent(analysis.instrumentalness)}</span>
                                  </div>
                                </div>
                                {analysis.key && (
                                  <div className="trackDetailSection">
                                    <strong>Key</strong>
                                    <p>{analysis.key}{analysis.camelotKey ? ` (${analysis.camelotKey})` : ""}{analysis.keyConfidence ? ` · confidence ${formatPercent(analysis.keyConfidence)}` : ""}</p>
                                  </div>
                                )}
                                {analysis.moodTags?.length > 0 && (
                                  <div className="trackDetailSection">
                                    <strong>Mood Tags</strong>
                                    <p>{analysis.moodTags.join(" · ")}</p>
                                  </div>
                                )}
                              </>
                            ) : (
                              <div className="trackDetailSection">
                                <strong>Audio Features</strong>
                                <p className="analysisFallback">Not available for this track yet.</p>
                              </div>
                            )}
                            {track.year && (
                              <div className="trackDetailSection">
                                <strong>Year</strong>
                                <p>{track.year}</p>
                              </div>
                            )}
                          </div>
                        )}
                      </li>
                    );
                  })}
                </ul>
              );
            })()}
          </div>
        </article>

        <article className="panel">
          <div className="panel-header panel-header-actions">
            <div className="panel-tabs">
              <button
                className={`tab ${playlistTab === "playlists" ? "active" : ""}`}
                onClick={() => setPlaylistTab("playlists")}
              >
                Playlists
              </button>
              <button
                className={`tab ${playlistTab === "history" ? "active" : ""}`}
                onClick={() => setPlaylistTab("history")}
              >
                History
              </button>
              <button
                className={`tab ${playlistTab === "stats" ? "active" : ""}`}
                onClick={() => setPlaylistTab("stats")}
              >
                Stats
              </button>
              <button
                className={`tab ${playlistTab === "filter" ? "active" : ""}`}
                onClick={() => setPlaylistTab("filter")}
              >
                Filter
              </button>
            </div>
            {playlistTab === "playlists" ? (
              <button
                className="ghost-button icon-button"
                onClick={() => openRefreshConfirmation("playlists")}
                disabled={busyAction !== null}
                aria-label="Force refresh playlists"
                title="Force refresh playlists"
              >
                {busyAction === "refresh-playlists" ? "…" : "↻"}
              </button>
            ) : null}
          </div>
          <div className="panel-content" style={{ padding: 0 }}>
            {playlistTab === "playlists" ? (
            <ul className="playlistList">
              {playlists.map((playlist) => (
                <li key={playlist.id}>
                  <div className="playlistCard">
                    <div className="playlistArtworkWrap">
                      {playlist.artworkUrl ? (
                        <img className="playlistArtwork" src={assetUrl(playlist.artworkUrl)} alt={`${playlist.name} cover art`} />
                      ) : (
                        <div className="playlistArtwork playlistArtworkFallback" aria-hidden="true">
                          {playlist.name.slice(0, 1).toUpperCase()}
                        </div>
                      )}
                      <div className="playlistArtworkOverlay">
                        <button
                          type="button"
                          className="ghost-button playlistArtworkRefreshBtn"
                          onClick={(e) => { e.stopPropagation(); regenerateArtwork(playlist.id); }}
                          disabled={busyAction !== null}
                          aria-label={`Regenerate cover for ${playlist.name}`}
                          title="Regenerate cover"
                        >
                          {busyAction === `artwork-${playlist.id}` ? "…" : "↻"}
                        </button>
                      </div>
                    </div>
                    <div className="playlistBody">
                      <div className="playlistTopRow">
                        <div>
                          <div className="playlistHead">
                            <strong>{playlist.name}</strong>
                          </div>
                        </div>
                        {(() => {
                          const isPinned = playlist.mode === "pinned";
                          const refreshActionId = `playlist-refresh-${playlist.id}`;
                          return (
                            <>
                              <button
                                type="button"
                                className="ghost-button playlistRefreshBtn"
                                onClick={() => refreshSinglePlaylist(playlist.id)}
                                disabled={busyAction !== null}
                                aria-label={`Refresh ${playlist.name}`}
                                title={`Refresh ${playlist.name}`}
                              >
                                {busyAction === refreshActionId ? "…" : "↻"}
                              </button>
                              <button
                                type="button"
                                className={`ghost-button playlistPinBtn ${playlist.mode}`}
                                onClick={() => updatePlaylistMode(playlist.id, isPinned ? "dynamic" : "pinned")}
                                disabled={busyAction !== null}
                                aria-label={`${isPinned ? "Unpin" : "Pin"} ${playlist.name}`}
                                title={isPinned ? "Click to unpin" : "Click to pin"}
                              >
                                {busyAction === `playlist-mode-${playlist.id}-${isPinned ? "dynamic" : "pinned"}` ? "…" : <Pin className="playlistPinIcon" aria-hidden="true" />}
                              </button>
                              <button
                                type="button"
                                className="ghost-button playlistDeleteBtn"
                                onClick={() => deletePlaylist(playlist.id)}
                                disabled={busyAction !== null}
                                aria-label={`Delete playlist ${playlist.name}`}
                                title="Delete playlist"
                              >
                                {busyAction === `playlist-delete-${playlist.id}` ? "…" : "×"}
                              </button>
                            </>
                          );
                        })()}
                      </div>
                      <p>{playlist.description}</p>
                      {playlist.artworkAttribution && playlist.artworkAttributionUrl ? (
                        <a className="playlistAttribution" href={playlist.artworkAttributionUrl} target="_blank" rel="noreferrer">
                          {playlist.artworkAttribution}
                        </a>
                      ) : null}
                      <div className="playlistTracks">
                        {playlist.tracks.map((track) => (
                          <span key={track.id}>
                            {track.title || "Untitled"} — {track.artist || "Unknown"}
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                </li>
              ))}
            </ul>
            ) : playlistTab === "history" ? (
            snapshots.length === 0 ? (
              <p className="emptyState">No history yet. Old playlists will appear here after the next weekly refresh.</p>
            ) : (
            <ul className="playlistList">
              {snapshots.map((s) => (
                <li key={s.id}>
                  <div className="playlistCard">
                    <div className="playlistArtwork playlistArtworkFallback" aria-hidden="true">
                      {s.name.slice(0, 1).toUpperCase()}
                    </div>
                    <div className="playlistBody">
                      <div className="playlistTopRow">
                        <div className="playlistHead">
                          <strong>{s.name}</strong>
                          <span className="snapshotMeta">
                            Week {s.snapshotWeek} · {s.trackCount} tracks · {new Date(s.createdAt).toLocaleDateString()}
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                </li>
              ))}
            </ul>
            )
            ) : playlistTab === "stats" ? (
              libraryStats ? (
                <div className="statsPanel">
                  <div className="statSection">
                    <h4>Top Genres</h4>
                    <div className="statBars">
                      {libraryStats.topGenres.map((g) => (
                        <div key={g.genre} className="statBar">
                          <span className="statBarLabel">{g.genre}</span>
                          <span className="statBarTrack">
                            <span className="statBarFill" style={{ width: `${Math.max(1, (g.count / libraryStats.topGenres[0].count) * 100)}%` }} />
                          </span>
                          <span className="statBarCount">{g.count}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="statSection">
                    <h4>Top Tags</h4>
                    <div className="statBars">
                      {libraryStats.topTags.slice(0, 15).map((t) => (
                        <div key={t.tag} className="statBar">
                          <span className="statBarLabel">{t.tag}</span>
                          <span className="statBarTrack">
                            <span className="statBarFill statBarFillTag" style={{ width: `${Math.max(1, (t.count / libraryStats.topTags[0].count) * 100)}%` }} />
                          </span>
                          <span className="statBarCount">{t.count}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="statSection">
                    <h4>Audio Distribution</h4>
                    <div className="statGrid">
                      {Object.entries(libraryStats.audioDistributions).map(([feature, buckets]) => (
                        <div key={feature} className="statBar statBarColumn">
                          <span className="statBarLabel">{feature.charAt(0).toUpperCase() + feature.slice(1)}</span>
                          <div className="statColumnBars">
                            {buckets.map((count, i) => (
                              <span key={i} className="statColumnBar" style={{ height: `${Math.max(1, (count / Math.max(1, ...buckets)) * 100)}%` }} />
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                  {libraryStats.keyDistribution.length > 0 ? (
                    <div className="statSection">
                      <h4>Key Distribution</h4>
                      <div className="statBars">
                        {libraryStats.keyDistribution.map((k) => (
                          <div key={k.key} className="statBar">
                            <span className="statBarLabel">{k.key}</span>
                            <span className="statBarTrack">
                              <span className={`statBarFill ${k.key.endsWith("A") ? "statBarFillMinor" : "statBarFillMajor"}`} style={{ width: `${Math.max(1, (k.count / Math.max(1, ...libraryStats.keyDistribution.map((x) => x.count))) * 100)}%` }} />
                            </span>
                            <span className="statBarCount">{k.count}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  {libraryStats.decadeSpread.length > 0 ? (
                    <div className="statSection">
                      <h4>Decades</h4>
                      <div className="statBars">
                        {libraryStats.decadeSpread.map((d) => (
                          <div key={d.decade} className="statBar">
                            <span className="statBarLabel">{d.decade}</span>
                            <span className="statBarTrack">
                              <span className="statBarFill statBarFillDecade" style={{ width: `${Math.max(1, (d.count / Math.max(1, ...libraryStats.decadeSpread.map((x) => x.count))) * 100)}%` }} />
                            </span>
                            <span className="statBarCount">{d.count}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  {libraryStats.languageBreakdown.length > 0 ? (
                    <div className="statSection">
                      <h4>Languages</h4>
                      <div className="statBars">
                        {libraryStats.languageBreakdown.map((l) => (
                          <div key={l.language} className="statBar">
                            <span className="statBarLabel">{l.language}</span>
                            <span className="statBarTrack">
                              <span className="statBarFill statBarFillLang" style={{ width: `${Math.max(1, (l.count / Math.max(1, ...libraryStats.languageBreakdown.map((x) => x.count))) * 100)}%` }} />
                            </span>
                            <span className="statBarCount">{l.count}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  <div className="statSection">
                    <h4>BPM Stats</h4>
                    <p className="statSummary">
                      Range {Math.round(libraryStats.bpmStats.min)}–{Math.round(libraryStats.bpmStats.max)} BPM · Average {libraryStats.bpmStats.avg} BPM · Median {libraryStats.bpmStats.median} BPM
                    </p>
                  </div>
                </div>
              ) : (
                <p className="emptyState">Loading stats…</p>
              )
            ) : (
              <>
              <div className="filterPanel">
                <div className="filterControls">
                  <div className="filterRow">
                    <div className="tagInputWrap">
                      {filterParams.selectedTags.map((tag) => (
                        <span key={tag} className="tagBadge" onClick={() => removeTag(tag)} role="button" tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); removeTag(tag); } }}>
                          {tag} <span aria-hidden="true">&times;</span>
                        </span>
                      ))}
                      <input
                        className="filterInput tagInputField"
                        type="text"
                        placeholder={filterParams.selectedTags.length ? "" : "Add tags…"}
                        value={tagInputText}
                        onChange={(e) => {
                          const val = e.target.value;
                          if (val.includes(",")) {
                            const parts = val.split(",").map((s) => s.trim()).filter(Boolean);
                            for (const part of parts) addTag(part);
                            return;
                          }
                          setTagInputText(val);
                        }}
                        onFocus={() => setTagInputFocused(true)}
                        onBlur={() => setTimeout(() => setTagInputFocused(false), 200)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter") { e.preventDefault(); addTag(tagInputText); }
                          if (e.key === "Backspace" && !tagInputText && filterParams.selectedTags.length) {
                            removeTag(filterParams.selectedTags[filterParams.selectedTags.length - 1]);
                          }
                        }}
                      />
                      {(tagInputFocused || tagInputText) && tagSuggestions.length > 0 && (
                        <div className="tagSuggestions">
                          {tagSuggestions.map((tag) => (
                            <div
                              key={tag}
                              className="tagSuggestion"
                              onMouseDown={(e) => { e.preventDefault(); addTag(tag); }}
                            >
                              {tag}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                    <input
                      className="filterInput"
                      type="text"
                      placeholder="Key (e.g. C, 8B)"
                      value={filterParams.keyQuery}
                      onChange={(e) => setFilterParams((p) => ({ ...p, keyQuery: e.target.value }))}
                    />
                    <select
                      className="filterInput filterInputKeyMode"
                      value={filterParams.keyMode}
                      onChange={(e) => setFilterParams((p) => ({ ...p, keyMode: e.target.value as "" | "major" | "minor" }))}
                    >
                      <option value="">Any key</option>
                      <option value="major">Major</option>
                      <option value="minor">Minor</option>
                    </select>
                    <span className="filterCount">{filteredTracks.length} track{filteredTracks.length !== 1 ? "s" : ""}</span>
                  </div>
                  <div className="filterRow">
                    <input
                      className="filterInput filterInputSmall"
                      type="number"
                      placeholder="BPM min"
                      value={Number.isNaN(filterParams.bpmMin) ? "" : filterParams.bpmMin}
                      onChange={(e) => setFilterParams((p) => ({ ...p, bpmMin: e.target.value === "" ? NaN : Number(e.target.value) }))}
                    />
                    <input
                      className="filterInput filterInputSmall"
                      type="number"
                      placeholder="BPM max"
                      value={Number.isNaN(filterParams.bpmMax) ? "" : filterParams.bpmMax}
                      onChange={(e) => setFilterParams((p) => ({ ...p, bpmMax: e.target.value === "" ? NaN : Number(e.target.value) }))}
                    />
                    <input
                      className="filterInput filterInputSmall"
                      type="number"
                      placeholder="Energy min"
                      min="0"
                      max="100"
                      value={Number.isNaN(filterParams.energyMin) ? "" : filterParams.energyMin}
                      onChange={(e) => setFilterParams((p) => ({ ...p, energyMin: e.target.value === "" ? NaN : Number(e.target.value) }))}
                    />
                    <input
                      className="filterInput filterInputSmall"
                      type="number"
                      placeholder="Energy max"
                      min="0"
                      max="100"
                      value={Number.isNaN(filterParams.energyMax) ? "" : filterParams.energyMax}
                      onChange={(e) => setFilterParams((p) => ({ ...p, energyMax: e.target.value === "" ? NaN : Number(e.target.value) }))}
                    />
                    <input
                      className="filterInput filterInputSmall"
                      type="number"
                      placeholder="Year min"
                      value={Number.isNaN(filterParams.yearMin) ? "" : filterParams.yearMin}
                      onChange={(e) => setFilterParams((p) => ({ ...p, yearMin: e.target.value === "" ? NaN : Number(e.target.value) }))}
                    />
                    <input
                      className="filterInput filterInputSmall"
                      type="number"
                      placeholder="Year max"
                      value={Number.isNaN(filterParams.yearMax) ? "" : filterParams.yearMax}
                      onChange={(e) => setFilterParams((p) => ({ ...p, yearMax: e.target.value === "" ? NaN : Number(e.target.value) }))}
                    />
                  </div>
                  {savingFilterPlaylist ? (
                    <div className="filterRow">
                      <input
                        className="filterInput filterInputSmall"
                        type="text"
                        placeholder="Playlist name"
                        value={filterPlaylistName}
                        onChange={(e) => setFilterPlaylistName(e.target.value)}
                        onKeyDown={(e) => { if (e.key === "Enter" && filterPlaylistName.trim()) saveFilterPlaylist(filterPlaylistName.trim()); if (e.key === "Escape") { setSavingFilterPlaylist(false); setFilterPlaylistName(""); } }}
                        autoFocus
                      />
                      <button
                        className="ghost-button filterSaveBtn"
                        disabled={!filterPlaylistName.trim() || busyAction !== null}
                        onClick={() => saveFilterPlaylist(filterPlaylistName.trim())}
                      >
                        {busyAction === "save-filter-playlist" ? "…" : "Save"}
                      </button>
                      <button
                        className="ghost-button filterSaveBtn"
                        onClick={() => { setSavingFilterPlaylist(false); setFilterPlaylistName(""); }}
                      >
                        Cancel
                      </button>
                    </div>
                  ) : (
                    <div className="filterRow">
                      <button
                        className="ghost-button filterSaveBtn"
                        disabled={filteredTracks.length === 0 || busyAction !== null}
                        onClick={() => setSavingFilterPlaylist(true)}
                      >
                        Save as Playlist
                      </button>
                    </div>
                  )}
                </div>
                </div>
                <div className="filterResults">
                  {!allTracksLoaded ? (
                    <p className="emptyState">Loading tracks…</p>
                  ) : filteredTracks.length === 0 ? (
                    <p className="emptyState">No tracks match these filters.</p>
                  ) : (
                    <ul className="trackList">
                      {filteredTracks.map((track) => {
                        const expanded = expandedTrackId === track.id;
                        const analysis = hasAudioAnalysis(track) ? track.audioFeatures : null;
                        return (
                          <li key={track.id}>
                            <div
                              className={`trackRow ${expanded ? "trackRowExpanded" : ""}`}
                              onClick={() => setExpandedTrackId(expanded ? null : track.id)}
                              role="button"
                              tabIndex={0}
                              onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setExpandedTrackId(expanded ? null : track.id); } }}
                            >
                              <div className="trackHead">
                                <div>
                                  <strong>{track.title || "Unknown Title"}</strong>
                                  <span>
                                    {track.artist || "Unknown Artist"} • {track.album || "Unknown Album"}
                                  </span>
                                </div>
                                <span className={`trackAnalysisPill ${analysis ? "on" : "off"}`}>
                                  {analysis ? "Analyzed" : "Metadata"}
                                </span>
                              </div>
                              <p>{track.tags.slice(0, 6).join(" · ") || "untagged"}</p>
                            </div>
                            {expanded && (
                              <div className="trackDetail">
                                {track.tags.length > 0 && (
                                  <div className="trackDetailSection">
                                    <strong>Tags</strong>
                                    <p>{track.tags.join(" · ")}</p>
                                  </div>
                                )}
                                {analysis ? (
                                  <>
                                    <div className="trackDetailSection">
                                      <strong>Audio Features</strong>
                                      <div className="analysisMetrics">
                                        <span>{formatBpm(analysis.bpm)}</span>
                                        <span>Energy {formatPercent(analysis.energy)}</span>
                                        <span>Dance {formatPercent(analysis.danceability)}</span>
                                        <span>Valence {formatPercent(analysis.valence)}</span>
                                        <span>Acoustic {formatPercent(analysis.acousticness)}</span>
                                        <span>Instrumental {formatPercent(analysis.instrumentalness)}</span>
                                      </div>
                                    </div>
                                    {analysis.key && (
                                      <div className="trackDetailSection">
                                        <strong>Key</strong>
                                        <p>{analysis.key}{analysis.camelotKey ? ` (${analysis.camelotKey})` : ""}{analysis.keyConfidence ? ` · confidence ${formatPercent(analysis.keyConfidence)}` : ""}</p>
                                      </div>
                                    )}
                                    {analysis.moodTags?.length > 0 && (
                                      <div className="trackDetailSection">
                                        <strong>Mood Tags</strong>
                                        <p>{analysis.moodTags.join(" · ")}</p>
                                      </div>
                                    )}
                                  </>
                                ) : (
                                  <div className="trackDetailSection">
                                    <strong>Audio Features</strong>
                                    <p className="analysisFallback">Not available for this track yet.</p>
                                  </div>
                                )}
                                {track.year && (
                                  <div className="trackDetailSection">
                                    <strong>Year</strong>
                                    <p>{track.year}</p>
                                  </div>
                                )}
                              </div>
                            )}
                          </li>
                        );
                      })}
                    </ul>
                  )}
                </div>
              </>
            )}
          </div>
        </article>
      </section>

      {toastEntry ? (
        <aside className={`toast ${toastEntry.level}`}>
          <div className="toast-head">
            <strong>
              {toastEntry.scope} · {toastEntry.level}
            </strong>
            <span>{new Date(toastEntry.timestamp).toLocaleTimeString()}</span>
          </div>
          <p>{toastEntry.message}</p>
        </aside>
      ) : null}
      {confirmRefreshModal ? (
        <div className="modal-backdrop" role="presentation" onClick={() => setConfirmRefreshModal(null)}>
          <div className="modal-card" role="dialog" aria-modal="true" aria-labelledby="refresh-confirm-title" onClick={(event) => event.stopPropagation()}>
            <h3 id="refresh-confirm-title">{confirmRefreshModal.title}</h3>
            <p>{confirmRefreshModal.message}</p>
            <div className="modal-actions">
              <button className="modal-cancel" onClick={() => setConfirmRefreshModal(null)} disabled={busyAction !== null}>
                Cancel
              </button>
              <button onClick={() => void confirmRefreshAction()} disabled={busyAction !== null}>
                {confirmRefreshModal.confirmLabel}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </main>
  );
}

export default App;
