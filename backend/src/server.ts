import cors from "cors";
import express from "express";
import fs from "node:fs";
import path from "node:path";
import { bootstrap as bootstrapScheduler } from "./scheduler.js";
import { port, dbFile, generatedArtworkDir, frontendDistDir } from "./config.js";
import { log } from "./state.js";
import { resumeWorkerSchedulerFromPersistence } from "./worker.js";
import workerRoutes from "./routes/worker.js";
import statsRoutes from "./routes/stats.js";
import settingsRoutes from "./routes/settings.js";
import tracksRoutes from "./routes/tracks.js";
import scanRoutes from "./routes/scan.js";
import playlistsRoutes from "./routes/playlists.js";

fs.mkdirSync(path.dirname(dbFile), { recursive: true });
fs.mkdirSync(generatedArtworkDir, { recursive: true });





const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));
app.use("/generated-artwork", express.static(generatedArtworkDir));
app.use(workerRoutes);
app.use(statsRoutes);
app.use(settingsRoutes);
app.use(tracksRoutes);
app.use(scanRoutes);
app.use(playlistsRoutes);





if (frontendDistDir) {
  app.use(express.static(frontendDistDir));
  app.use((req, res, next) => {
    if (req.method !== "GET" || req.path.startsWith("/api/")) {
      next();
      return;
    }
    res.sendFile(path.join(frontendDistDir!, "index.html"));
  });
}

bootstrapScheduler(dbFile);
log.info("scheduler bootstrapped");
resumeWorkerSchedulerFromPersistence();

app.listen(port, () => {
  log.info("server started", { port, dbFile, frontendDistDir: frontendDistDir ?? "none" });
});
