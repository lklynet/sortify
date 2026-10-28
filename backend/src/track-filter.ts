/**
 * Shared track filter semantics, expressed as a parameterized SQL WHERE clause.
 *
 * This mirrors the filter logic the frontend used to run client-side (and the
 * duplicated JS loops in playlist generation), so that filtering 100k+ track
 * libraries happens inside SQLite instead of shipping the whole library to the
 * browser. Any caller (filter browsing, saving a filter as playlist, refreshing
 * pinned filter playlists) MUST go through this builder so all paths agree.
 *
 * Notes on semantic equivalence with the legacy JS predicate:
 * - Tags: JS matched `tag.toLowerCase().includes(needle)` for at least one tag
 *   per needle. Tags are serialized as a JSON array in `tags_json`, so a
 *   substring over the raw JSON text is an equivalent match for realistic tag
 *   inputs (needles never contain JSON separators).
 * - Missing audio features / year behave like 0, exactly like the JS
 *   `(track.audioFeatures?.bpm ?? 0)` and `(track.year ?? 0)` defaults.
 */

function finiteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/** Escape LIKE wildcards so user-supplied needles match literally. */
function escapeLike(value: string): string {
  return value.replace(/([%_])/g, "\\$1");
}

export function buildTrackFilterWhere(filters: Record<string, unknown>): { where: string; params: unknown[] } {
  const clauses: string[] = [];
  const params: unknown[] = [];

  // Note: needles are intentionally NOT trimmed — the legacy client-side
  // predicate matched `tag.includes(needle)` verbatim (the UI trims input
  // separately), and substring semantics must stay identical.
  const tags = Array.isArray(filters.selectedTags)
    ? (filters.selectedTags as unknown[]).map((tag) => String(tag).toLowerCase()).filter(Boolean)
    : [];
  for (const needle of tags) {
    clauses.push("lower(tags_json) LIKE ? ESCAPE '\\'");
    params.push(`%${escapeLike(needle)}%`);
  }

  const bpmMin = finiteNumber(filters.bpmMin);
  if (bpmMin !== null) {
    clauses.push("COALESCE(json_extract(audio_features_json, '$.bpm'), 0) >= ?");
    params.push(bpmMin);
  }
  const bpmMax = finiteNumber(filters.bpmMax);
  if (bpmMax !== null) {
    clauses.push("COALESCE(json_extract(audio_features_json, '$.bpm'), 0) <= ?");
    params.push(bpmMax);
  }

  const keyQuery = typeof filters.keyQuery === "string" ? filters.keyQuery.trim().toLowerCase() : "";
  if (keyQuery) {
    clauses.push(
      "(lower(COALESCE(json_extract(audio_features_json, '$.key'), '')) LIKE ? ESCAPE '\\' " +
        "OR lower(COALESCE(json_extract(audio_features_json, '$.camelotKey'), '')) LIKE ? ESCAPE '\\')"
    );
    params.push(`%${escapeLike(keyQuery)}%`, `%${escapeLike(keyQuery)}%`);
  }

  if (filters.keyMode === "major" || filters.keyMode === "minor") {
    const keyExpr = "lower(COALESCE(json_extract(audio_features_json, '$.key'), ''))";
    if (filters.keyMode === "minor") {
      clauses.push(`${keyExpr} LIKE '%m%' AND ${keyExpr} NOT LIKE '%major%'`);
    } else {
      clauses.push(`${keyExpr} != '' AND NOT (${keyExpr} LIKE '%m%' AND ${keyExpr} NOT LIKE '%major%')`);
    }
  }

  const energyMin = finiteNumber(filters.energyMin);
  if (energyMin !== null) {
    clauses.push("COALESCE(json_extract(audio_features_json, '$.energy'), 0) * 100 >= ?");
    params.push(energyMin);
  }
  const energyMax = finiteNumber(filters.energyMax);
  if (energyMax !== null) {
    clauses.push("COALESCE(json_extract(audio_features_json, '$.energy'), 0) * 100 <= ?");
    params.push(energyMax);
  }

  const yearMin = finiteNumber(filters.yearMin);
  if (yearMin !== null) {
    clauses.push("COALESCE(year, 0) >= ?");
    params.push(yearMin);
  }
  const yearMax = finiteNumber(filters.yearMax);
  if (yearMax !== null) {
    clauses.push("COALESCE(year, 0) <= ?");
    params.push(yearMax);
  }

  return { where: clauses.length > 0 ? clauses.join(" AND ") : "1=1", params };
}

/**
 * Paths that can be synced to a Subsonic/Navidrome server.
 * Equivalent to the JS `extractSubsonicSongId(path)` check: either an explicit
 * `subsonic:<id>` path or a bare filename (no separators, ASCII only).
 */
export function syncablePathClause(): string {
  return "(path LIKE 'subsonic:%' OR path GLOB '[a-zA-Z0-9._-]*')";
}
