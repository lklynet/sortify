import express from "express";
import { randomUUID } from "node:crypto";
import { appState, log } from "../state.js";
import { scanLibrary, resolveConnection, parseScanLimit } from "../scan.js";
import { refreshRecommendedPlaylists, syncSubsonicPlaylists } from "../playlists.js";
import { enqueueScan } from "../scheduler.js";
import { syncWorkerConnectionFromSettings, persistSettings, appendLog } from "../settings.js";
import type { ScanProgress } from "../types.js";
import type { SubsonicConnection } from "../subsonic-client.js";

const router = express.Router();

router.post("/api/scan", async (req, res) => {
  try {
    const connection = resolveConnection(req.body);
    const result = await scanLibrary(connection, undefined, { forceRefreshAnalysis: Boolean(req.body?.forceRefreshAnalysis), limit: parseScanLimit(req.body?.limit) });
    await refreshRecommendedPlaylists();
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error instanceof Error ? error.message : "Scan failed" });
  }
});

router.post("/api/scan/start", (req, res) => {
  let connection: SubsonicConnection;
  try {
    connection = resolveConnection(req.body);
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : "Invalid Subsonic connection" });
    return;
  }
  const id = randomUUID();
  const startedAt = new Date().toISOString();
  const job: ScanProgress = {
    id,
    status: "queued",
    libraryPath: connection.baseUrl,
    startedAt,
    finishedAt: null,
    scannedFiles: 0,
    processedFiles: 0,
    updatedTracks: 0,
    errors: 0,
    message: null
  };
  appState.scanJobs.set(id, job);
  appState.activeScanJobId = id;
  const honkerJobId = enqueueScan(connection);
  appendLog({
    level: "info",
    scope: "scan",
    message: "Scan job created",
    meta: { id, honkerJobId, source: connection.baseUrl }
  });
  void (async () => {
    const current = appState.scanJobs.get(id);
    if (!current) {
      return;
    }
    current.status = "running";
    appState.scanJobs.set(id, current);
    try {
      const result = await scanLibrary(
        connection,
        (progress) => {
        const active = appState.scanJobs.get(id);
        if (!active) {
          return;
        }
        Object.assign(active, progress);
        appState.scanJobs.set(id, active);
        },
        { forceRefreshAnalysis: Boolean(req.body?.forceRefreshAnalysis), limit: parseScanLimit(req.body?.limit) }
      );
      const completed = appState.scanJobs.get(id);
      if (!completed) {
        return;
      }
      completed.status = "completed";
      completed.finishedAt = new Date().toISOString();
      completed.scannedFiles = result.scannedFiles;
      completed.processedFiles = result.processedFiles;
      completed.updatedTracks = result.updatedTracks;
      completed.errors = result.errors;
      appState.scanJobs.set(id, completed);
      if (appState.activeScanJobId === id) {
        appState.activeScanJobId = null;
      }
      appendLog({
        level: "info",
        scope: "scan",
        message: "Scan job completed",
        meta: { id, scannedFiles: result.scannedFiles, processedFiles: result.processedFiles, updatedTracks: result.updatedTracks, errors: result.errors }
      });
      await refreshRecommendedPlaylists();
      await syncSubsonicPlaylists(connection);
      appendLog({
        level: "info",
        scope: "playlist",
        message: "Managed playlists synced after scan",
        meta: { subsonic: connection.baseUrl }
      });
    } catch (error) {
      const failed = appState.scanJobs.get(id);
      if (!failed) {
        return;
      }
      failed.status = "failed";
      failed.finishedAt = new Date().toISOString();
      failed.message = error instanceof Error ? error.message : "Scan failed";
      appState.scanJobs.set(id, failed);
      if (appState.activeScanJobId === id) {
        appState.activeScanJobId = null;
      }
      appendLog({
        level: "error",
        scope: "scan",
        message: "Scan job failed",
        meta: { id, error: failed.message }
      });
    }
  })();
  res.status(202).json({ id, status: job.status });
});

router.get("/api/scan/active", (_req, res) => {
  if (appState.activeScanJobId) {
    const activeJob = appState.scanJobs.get(appState.activeScanJobId);
    if (activeJob && (activeJob.status === "queued" || activeJob.status === "running")) {
      res.json(activeJob);
      return;
    }
    appState.activeScanJobId = null;
  }
  const fallback = [...appState.scanJobs.values()]
    .filter((job) => job.status === "queued" || job.status === "running")
    .sort((a, b) => b.startedAt.localeCompare(a.startedAt))[0];
  if (!fallback) {
    res.json({ active: null });
    return;
  }
  appState.activeScanJobId = fallback.id;
  res.json(fallback);
});

router.get("/api/scan/:id", (req, res) => {
  const id = String(req.params.id);
  const job = appState.scanJobs.get(id);
  if (!job) {
    res.status(404).json({ error: "Scan job not found" });
    return;
  }
  res.json(job);
});


export default router;
