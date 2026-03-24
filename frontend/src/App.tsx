import { useCallback, useEffect, useMemo, useState } from "react";
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
  updated_at: string;
  tracks: Array<{ id: number; title: string; artist: string; album: string; path: string }>;
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
  cycleRunning: boolean;
  weeklyPlaylistCount: number;
  lastRunAt: string | null;
  lastRunWeek: string | null;
  nextRunAt: string | null;
  error: string | null;
  connectionConfigured: boolean;
};
type Settings = {
  navidromeUrl: string;
  navidromeUsername: string;
  navidromePassword: string;
  hasNavidromePassword: boolean;
  lastFmApiKey: string;
  hasLastFmApiKey: boolean;
  geminiApiKey: string;
  hasGeminiApiKey: boolean;
  geminiModel: string;
  weeklyPlaylistCount: number;
  maxTracksPerPlaylist: number;
};

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
    geminiApiKey: "",
    hasGeminiApiKey: false,
    geminiModel: "models/gemini-flash-lite-latest",
    weeklyPlaylistCount: 3,
    maxTracksPerPlaylist: 20
  });
  const [settingsDraft, setSettingsDraft] = useState<Settings>({
    navidromeUrl: "",
    navidromeUsername: "",
    navidromePassword: "",
    hasNavidromePassword: false,
    lastFmApiKey: "",
    hasLastFmApiKey: false,
    geminiApiKey: "",
    hasGeminiApiKey: false,
    geminiModel: "models/gemini-flash-lite-latest",
    weeklyPlaylistCount: 3,
    maxTracksPerPlaylist: 20
  });
  const [settingsDirty, setSettingsDirty] = useState(false);
  const [stats, setStats] = useState<Stats>({ tracks: 0, playlists: 0, analyzedTracks: 0 });
  const [logs, setLogs] = useState<OperationLog[]>([]);
  const [tracks, setTracks] = useState<Track[]>([]);
  const [playlists, setPlaylists] = useState<Playlist[]>([]);
  const [worker, setWorker] = useState<WorkerState | null>(null);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [toastEntry, setToastEntry] = useState<OperationLog | null>(null);

  const hasNavidromePassword = Boolean(settingsDraft.navidromePassword.trim() || settingsDraft.hasNavidromePassword);
  const hasSubsonicConnection = Boolean(
    settingsDraft.navidromeUrl.trim() && settingsDraft.navidromeUsername.trim() && hasNavidromePassword
  );
  const hasGemini = Boolean(settingsDraft.geminiApiKey.trim() || settingsDraft.hasGeminiApiKey);
  const startReady = hasSubsonicConnection && hasGemini;
  const keyStatus = useMemo(
    () => ({
      lastfm: Boolean(settingsDraft.lastFmApiKey.trim() || settingsDraft.hasLastFmApiKey),
      gemini: Boolean(settingsDraft.geminiApiKey.trim() || settingsDraft.hasGeminiApiKey)
    }),
    [settingsDraft.geminiApiKey, settingsDraft.hasGeminiApiKey, settingsDraft.hasLastFmApiKey, settingsDraft.lastFmApiKey]
  );
  const recentAnalyzedTracks = useMemo(() => tracks.filter((track) => hasAudioAnalysis(track)).length, [tracks]);
  const mergeSettingsDraft = useCallback(
    (nextSettings: Settings, currentDraft: Settings) => ({
      ...nextSettings,
      navidromePassword: currentDraft.navidromePassword,
      lastFmApiKey: currentDraft.lastFmApiKey,
      geminiApiKey: currentDraft.geminiApiKey,
      hasNavidromePassword: nextSettings.hasNavidromePassword || currentDraft.navidromePassword.trim().length > 0,
      hasLastFmApiKey: nextSettings.hasLastFmApiKey || currentDraft.lastFmApiKey.trim().length > 0,
      hasGeminiApiKey: nextSettings.hasGeminiApiKey || currentDraft.geminiApiKey.trim().length > 0
    }),
    []
  );
  const settingsPayload = useMemo(() => {
    const nextSettings: Record<string, string | number> = {
      navidromeUrl: settingsDraft.navidromeUrl.trim(),
      navidromeUsername: settingsDraft.navidromeUsername.trim(),
      geminiModel: settingsDraft.geminiModel.trim().replace(/^models\//, ""),
      weeklyPlaylistCount: settingsDraft.weeklyPlaylistCount,
      maxTracksPerPlaylist: settingsDraft.maxTracksPerPlaylist
    };
    if (settingsDraft.navidromePassword.trim()) {
      nextSettings.navidromePassword = settingsDraft.navidromePassword.trim();
    }
    if (settingsDraft.lastFmApiKey.trim()) {
      nextSettings.lastFmApiKey = settingsDraft.lastFmApiKey.trim();
    }
    if (settingsDraft.geminiApiKey.trim()) {
      nextSettings.geminiApiKey = settingsDraft.geminiApiKey.trim();
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
    const [fetchedStats, fetchedTracks, fetchedPlaylists, fetchedWorker, fetchedSettings] = await Promise.all([
      callApi<Stats>("/api/stats"),
      callApi<Track[]>("/api/tracks?limit=20"),
      callApi<Playlist[]>("/api/playlists"),
      callApi<WorkerState>("/api/worker"),
      callApi<Settings>("/api/settings")
    ]);
    setStats(fetchedStats);
    setTracks(fetchedTracks);
    setPlaylists(fetchedPlaylists);
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
    setToastEntry(logs[0]);
  }, [logs]);

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

  const onStartWorker = () =>
    runAction("start", async () => {
      const savedSettings = await callApi<Settings>("/api/settings", {
        method: "PATCH",
        body: JSON.stringify(settingsPayload)
      });
      setSettings(savedSettings);
      setSettingsDraft((currentDraft) => mergeSettingsDraft(savedSettings, currentDraft));
      setSettingsDirty(false);
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
      const savedSettings = await callApi<Settings>("/api/settings", {
        method: "PATCH",
        body: JSON.stringify(settingsPayload)
      });
      setSettings(savedSettings);
      setSettingsDraft((currentDraft) => mergeSettingsDraft(savedSettings, currentDraft));
      setSettingsDirty(false);
    });

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
                  <div className="field">
                    <label htmlFor="geminiApiKey">Gemini API Key</label>
                    <input
                      id="geminiApiKey"
                      type="password"
                      value={settingsDraft.geminiApiKey}
                      onChange={(event) => {
                        setSettingsDraft((prev) => ({ ...prev, geminiApiKey: event.target.value }));
                        setSettingsDirty(true);
                      }}
                      placeholder={settingsDraft.hasGeminiApiKey ? "****************" : "enter Gemini key"}
                    />
                  </div>
                  <div className="field">
                    <label htmlFor="geminiModel">Gemini Model</label>
                    <input
                      id="geminiModel"
                      value={settingsDraft.geminiModel}
                      onChange={(event) => {
                        setSettingsDraft((prev) => ({ ...prev, geminiModel: event.target.value.replace(/^models\//, "") }));
                        setSettingsDirty(true);
                      }}
                      placeholder="gemini-flash-lite-latest"
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
                        setSettingsDraft((prev) => ({ ...prev, weeklyPlaylistCount: Math.max(1, Math.min(5, Number(event.target.value))) }));
                        setSettingsDirty(true);
                      }}
                    >
                      {[1, 2, 3, 4, 5].map((count) => (
                        <option key={count} value={count}>
                          {count}
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
            <span className={`signal ${keyStatus.gemini ? "ok" : "bad"}`}>Gemini {keyStatus.gemini ? "Ready" : "Missing"}</span>
            <span className={`signal ${keyStatus.lastfm ? "ok" : "warn"}`}>Last.fm {keyStatus.lastfm ? "Ready" : "Optional"}</span>
            <span className={`signal ${stats.analyzedTracks ? "ok" : "warn"}`}>Analyzed {stats.analyzedTracks}/{stats.tracks}</span>
            <span className="signal neutral">Playlists/Week {settings.weeklyPlaylistCount}</span>
            <span className="signal neutral">Max Tracks {settings.maxTracksPerPlaylist}</span>
          </div>
          {worker ? (
            <p className="status">
              Worker {worker.running ? "running" : "stopped"} · cycle {worker.cycleRunning ? "active" : "idle"} · next refresh{" "}
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
          <div className="panel-header">
            <h3>Recent tagged tracks · {tracks.length} / {stats.tracks} · analyzed {recentAnalyzedTracks}</h3>
          </div>
          <div className="panel-content" style={{ padding: 0 }}>
            <ul className="trackList">
              {tracks.map((track) => (
                <li key={track.id}>
                  {(() => {
                    const analysis = hasAudioAnalysis(track) ? track.audioFeatures : null;
                    return (
                      <>
                  <div className="trackHead">
                    <div>
                      <strong>{track.title || "Unknown Title"}</strong>
                      <span>
                        {track.artist || "Unknown Artist"} • {track.album || "Unknown Album"}
                      </span>
                    </div>
                    <span className={`trackAnalysisPill ${analysis ? "on" : "off"}`}>
                      {analysis ? "Analyzed" : "Metadata only"}
                    </span>
                  </div>
                  <p>{track.tags.slice(0, 6).join(" · ") || "untagged"}</p>
                  {analysis ? (
                    <div className="analysisBlock">
                      <div className="analysisMetrics">
                        <span>{formatBpm(analysis.bpm)}</span>
                        <span>Energy {formatPercent(analysis.energy)}</span>
                        <span>Dance {formatPercent(analysis.danceability)}</span>
                        <span>Valence {formatPercent(analysis.valence)}</span>
                      </div>
                      <div className="analysisTags">
                        {(analysis.moodTags ?? []).slice(0, 4).map((tag) => (
                          <span key={tag}>{tag}</span>
                        ))}
                      </div>
                    </div>
                  ) : (
                    <p className="analysisFallback">
                      Audio analysis is not available for this track yet.
                    </p>
                  )}
                      </>
                    );
                  })()}
                </li>
              ))}
            </ul>
          </div>
        </article>

        <article className="panel">
          <div className="panel-header">
            <h3>Current playlists · {settingsDraft.weeklyPlaylistCount} / week · max {settingsDraft.maxTracksPerPlaylist} tracks</h3>
          </div>
          <div className="panel-content" style={{ padding: 0 }}>
            <ul className="playlistList">
              {playlists.map((playlist) => (
                <li key={playlist.id}>
                  <div className="playlistHead">
                    <strong>{playlist.name}</strong>
                    <span>{playlist.tracks.length} tracks</span>
                  </div>
                  <p>{playlist.description}</p>
                  <div className="playlistTracks">
                    {playlist.tracks.slice(0, 5).map((track) => (
                      <span key={track.id}>
                        {track.title || "Untitled"} — {track.artist || "Unknown"}
                      </span>
                    ))}
                  </div>
                </li>
              ))}
            </ul>
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
    </main>
  );
}

export default App;
