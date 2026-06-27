export type RankedTagSource = {
  source: "base" | "musicbrainz-recording" | "musicbrainz-artist" | "lastfm-track" | "lastfm-artist" | "lastfm-album" | "audio";
  weight: number;
  tags: string[];
};

const lowSignalTags = new Set(["decade", "low tempo", "mid tempo", "year"]);

function isDecadeTag(tag: string): boolean {
  return /^(?:\d{2}|\d{3}|\d{4})s$/.test(tag);
}

export function tagWeight(tag: string): number {
  if (isDecadeTag(tag) || tag === "decade") {
    return 0.12;
  }
  if (lowSignalTags.has(tag)) {
    return 0.45;
  }
  return 1;
}

export function isPrimaryClusterTag(tag: string): boolean {
  if (!tag) {
    return false;
  }
  if (isDecadeTag(tag)) {
    return false;
  }
  if (lowSignalTags.has(tag)) {
    return false;
  }
  return true;
}

export function normalizeTag(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/\s+and\s+/g, " & ")
    .replace(/\s+n\s+/g, " & ")
    .replace(/-/g, " ")
    .replace(/\s+/g, " ");
}

export function uniqueTags(tags: string[]): string[] {
  return [...new Set(
    tags
      .map(normalizeTag)
      .filter((tag) => {
        if (tag.length <= 1) return false;
        if (/^\d+$/.test(tag)) return false;
        return true;
      })
  )].slice(0, 40);
}

export function mergeRankedTagSources(sources: RankedTagSource[], limit = 40): string[] {
  const scoredTags = new Map<
    string,
    {
      score: number;
      sourceCount: number;
      bestRank: number;
      hasAudio: boolean;
      hasExternalMetadata: boolean;
      hasHeuristic: boolean;
    }
  >();
  for (const source of sources) {
    const tags = uniqueTags(source.tags);
    tags.forEach((tag, index) => {
      const entry = scoredTags.get(tag) ?? {
        score: 0,
        sourceCount: 0,
        bestRank: Number.POSITIVE_INFINITY,
        hasAudio: false,
        hasExternalMetadata: false,
        hasHeuristic: false
      };
      const rankWeight = Math.max(0.24, 1 - index * 0.08);
      entry.score += source.weight * rankWeight * tagWeight(tag);
      entry.sourceCount += 1;
      entry.bestRank = Math.min(entry.bestRank, index);
      if (source.source === "audio") {
        entry.hasAudio = true;
      } else if (source.source === "base") {
        entry.hasHeuristic = true;
      } else {
        entry.hasExternalMetadata = true;
      }
      scoredTags.set(tag, entry);
    });
  }
  return [...scoredTags.entries()]
    .map(([tag, entry]) => ({
      tag,
      score:
        entry.score +
        Math.max(0, entry.sourceCount - 1) * 0.38 +
        (entry.hasAudio && entry.hasExternalMetadata ? 0.18 : 0) +
        (entry.hasHeuristic && entry.hasExternalMetadata ? 0.08 : 0),
      bestRank: entry.bestRank,
      sourceCount: entry.sourceCount
    }))
    .sort((a, b) => b.score - a.score || b.sourceCount - a.sourceCount || a.bestRank - b.bestRank || a.tag.localeCompare(b.tag))
    .slice(0, limit)
    .map((entry) => entry.tag);
}

export function deriveBaseTags(
  title: string,
  artist: string,
  year: number | null,
  rawGenres: string[],
  bpm: number | null
): string[] {
  const tags = [...rawGenres];
  if (year) {
    tags.push(`${Math.floor(year / 10) * 10}s`, "decade");
  }
  if (bpm && Number.isFinite(bpm)) {
    if (bpm < 95) {
      tags.push("low tempo", "chill");
    } else if (bpm > 130) {
      tags.push("high energy", "dance");
    } else {
      tags.push("mid tempo");
    }
  }
  const text = `${title} ${artist}`.toLowerCase();
  if (/(acoustic|unplugged|folk)/.test(text)) {
    tags.push("acoustic");
  }
  if (/(live|session)/.test(text)) {
    tags.push("live");
  }
  if (/(remix|mix)/.test(text)) {
    tags.push("electronic");
  }
  return uniqueTags(tags);
}
