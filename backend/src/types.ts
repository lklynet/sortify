export type TrackRecord = {
  id: number;
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
};

export type PlaylistRecord = {
  id: number;
  slug: string;
  name: string;
  description: string | null;
  mode: "dynamic" | "pinned";
  selected: number;
  sync_target: "subsonic";
  is_recommended: number;
  fewest_listens_priority: number;
  updated_at: string;
  artwork_path: string;
  artwork_attribution: string;
  artwork_attribution_url: string;
  artwork_signature: string;
  artwork_updated_at: string;
  filter_json?: string;
  seeds_json?: string;
};

export type ScanProgress = {
  id: string;
  status: "queued" | "running" | "completed" | "failed";
  libraryPath: string;
  startedAt: string;
  finishedAt: string | null;
  scannedFiles: number;
  processedFiles: number;
  updatedTracks: number;
  errors: number;
  message: string | null;
};

export type WorkerState = {
  running: boolean;
  weeklyPlaylistCount: number;
  lastRunAt: string | null;
  lastRunWeek: string | null;
  nextRunAt: string | null;
  error: string | null;
};

export type AppSettings = {
  navidromeUrl: string;
  navidromeUsername: string;
  navidromePassword: string;
  lastFmApiKey: string;
  weeklyPlaylistCount: number;
  maxTracksPerPlaylist: number;
  cycleSchedule: string;
  smtpEnabled: boolean;
  smtpHost: string;
  smtpPort: number;
  smtpSecure: boolean;
  smtpUser: string;
  smtpPassword: string;
  smtpFrom: string;
  smtpTo: string;
  smtpTlsInsecure: boolean;
};

export type OperationLogEntry = {
  id: string;
  timestamp: string;
  level: "info" | "warn" | "error";
  scope: "scan" | "playlist" | "system";
  message: string;
  meta?: Record<string, unknown>;
};

export type AudioAnalysis = {
  version: string;
  durationSampled: number;
  bpm: number | null;
  energy: number;
  danceability: number;
  valence: number;
  acousticness: number;
  instrumentalness: number;
  brightness: number;
  rhythmicDensity: number;
  loudness: number;
  key?: string | null;
  keyConfidence?: number;
  camelotKey?: string | null;
  moodTags: string[];
};
