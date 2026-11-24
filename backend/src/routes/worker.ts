import express from "express";
import { appState, log } from "../state.js";
import { workerSnapshot, setWorkerScheduleHints, startWorkerScheduler, stopWorkerScheduler, runWorkerCycle } from "../worker.js";
import { persistSettings, clamp, appendLog } from "../settings.js";
import type { SubsonicConnection } from "../subsonic-client.js";
import { resolveConnection } from "../scan.js";

const router = express.Router();

router.get("/api/worker", (_req, res) => {
  res.json(workerSnapshot());
});

router.post("/api/worker/start", (req, res) => {
  let connection: SubsonicConnection;
  try {
    connection = resolveConnection(req.body);
  } catch (error) {
    res.status(400).json({ error: error instanceof Error ? error.message : "Invalid Subsonic connection" });
    return;
  }
  const requestedCount = Number(req.body?.weeklyPlaylistCount ?? appState.workerState.weeklyPlaylistCount);
  appState.workerConnection = connection;
  appState.workerState.weeklyPlaylistCount = clamp(requestedCount, 0, 8);
  appState.settings.weeklyPlaylistCount = appState.workerState.weeklyPlaylistCount;
  appState.settings.navidromeUrl = connection.baseUrl;
  appState.settings.navidromeUsername = connection.username;
  appState.settings.navidromePassword = connection.password;
  appState.workerState.running = true;
  appState.workerState.error = null;
  setWorkerScheduleHints();
  persistSettings();
  startWorkerScheduler();
  void runWorkerCycle("start").catch(() => null);
  appendLog({
    level: "info",
    scope: "system",
    message: "Worker started",
    meta: { weeklyPlaylistCount: appState.workerState.weeklyPlaylistCount, source: connection.baseUrl }
  });
  res.json(workerSnapshot());
});

router.post("/api/worker/stop", (_req, res) => {
  stopWorkerScheduler();
  persistSettings();
  appendLog({
    level: "info",
    scope: "system",
    message: "Worker stopped"
  });
  res.json(workerSnapshot());
});

export default router;
