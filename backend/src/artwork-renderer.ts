import sharp from "sharp";
import { QuantizerCelebi, Score } from "@material/material-color-utilities";

export function sanitizeFilename(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80) || "playlist";
}

function escapeXml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function estimateTextWidth(text: string, fontSize: number) {
  return text.length * fontSize * 0.64;
}

function dedupeAdjacentWords(value: string): string {
  const words = value
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  const collapsed: string[] = [];
  for (const word of words) {
    const previous = collapsed[collapsed.length - 1];
    if (previous && previous.toLowerCase() === word.toLowerCase()) {
      continue;
    }
    collapsed.push(word);
  }
  return collapsed.join(" ").trim();
}

function wrapTitleWords(words: string[], maxWidth: number, fontSize: number): string[] {
  const wrapped: string[] = [];
  let currentLine = "";
  for (const word of words) {
    const candidate = currentLine ? `${currentLine} ${word}` : word;
    if (estimateTextWidth(candidate, fontSize) <= maxWidth) {
      currentLine = candidate;
    } else {
      if (currentLine) wrapped.push(currentLine);
      currentLine = word;
    }
  }
  if (currentLine) wrapped.push(currentLine);
  return wrapped.length ? wrapped : [words.join(" ")];
}

function layoutTitle(lines: string[]) {
  const safeLines = lines.length ? lines : ["Untitled", "Playlist"];
  const maxWidth = 1000;
  const maxHeight = 1080;
  const fontSize = 130;
  const lineHeight = Math.round(fontSize * 0.9);
  const wrapped = wrapTitleWords(safeLines, maxWidth, fontSize);
  const blockHeight = fontSize + (wrapped.length - 1) * lineHeight;
  if (blockHeight <= maxHeight) {
    return { fontSize, lineHeight, blockHeight, lines: wrapped };
  }
  const fallbackSize = 96;
  const fallbackLineHeight = Math.round(fallbackSize * 0.9);
  const fallbackWrapped = wrapTitleWords(safeLines, maxWidth, fallbackSize);
  const fallbackBlock = fallbackSize + (fallbackWrapped.length - 1) * fallbackLineHeight;
  return { fontSize: fallbackSize, lineHeight: fallbackLineHeight, blockHeight: fallbackBlock, lines: fallbackWrapped };
}

async function renderArtworkTitleOverlay(lines: string[], textColor: string) {
  const layout = layoutTitle(lines);
  const firstBaseline = (1200 - layout.blockHeight) / 2 + layout.fontSize;
  const leftInset = 72;
  const svg = `
    <svg width="1200" height="1200" viewBox="0 0 1200 1200" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <filter id="shadow" x="-4%" y="-4%" width="108%" height="108%">
          <feDropShadow dx="3" dy="4" stdDeviation="5" flood-color="#000000" flood-opacity="0.55"/>
        </filter>
      </defs>
      ${layout.lines
        .map(
          (line, index) =>
            `<text x="${leftInset}" y="${firstBaseline + index * layout.lineHeight}" text-anchor="start" font-family="Arial Black, Helvetica Neue, Arial, sans-serif" font-size="${layout.fontSize}" font-weight="900" letter-spacing="-1.8" fill="${textColor}" filter="url(#shadow)">${escapeXml(line)}</text>`
        )
        .join("")}
    </svg>
  `;
  return sharp(Buffer.from(svg)).png().toBuffer();
}

async function extractSourceColors(source: Buffer): Promise<{
  darkR: number; darkG: number; darkB: number;
  lightR: number; lightG: number; lightB: number;
}> {
  const { data } = await sharp(source)
    .resize(128, 128, { fit: "cover" })
    .removeAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });
  const pixels: number[] = [];
  for (let index = 0; index < data.length; index += 3) {
    pixels.push(((255 << 24) | (data[index] << 16) | (data[index + 1] << 8) | data[index + 2]) >>> 0);
  }
  const quantized = QuantizerCelebi.quantize(pixels, 128);
  const scored = Score.score(quantized, { desired: 8, filter: true });
  const candidates = scored.map((argb: number) => {
    const r = (argb >> 16) & 0xff;
    const g = (argb >> 8) & 0xff;
    const b = argb & 0xff;
    return { r, g, b, luminance: 0.299 * r + 0.587 * g + 0.114 * b };
  });
  if (candidates.length === 0) {
    return { darkR: 24, darkG: 24, darkB: 24, lightR: 230, lightG: 230, lightB: 230 };
  }
  if (candidates.length === 1) {
    const only = candidates[0];
    return {
      darkR: Math.round(only.r * 0.35),
      darkG: Math.round(only.g * 0.35),
      darkB: Math.round(only.b * 0.35),
      lightR: Math.round(only.r + (255 - only.r) * 0.45),
      lightG: Math.round(only.g + (255 - only.g) * 0.45),
      lightB: Math.round(only.b + (255 - only.b) * 0.45)
    };
  }
  candidates.sort((a, b) => a.luminance - b.luminance);
  const dark = candidates[0];
  const light = candidates[candidates.length - 1];
  return { darkR: dark.r, darkG: dark.g, darkB: dark.b, lightR: light.r, lightG: light.g, lightB: light.b };
}

async function fetchBuffer(url: string, init?: RequestInit): Promise<Buffer> {
  const response = await fetch(url, { ...init, signal: init?.signal ?? AbortSignal.timeout(20_000) });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return Buffer.from(await response.arrayBuffer());
}

export async function isUsableArtworkImage(imageUrl: string): Promise<boolean> {
  try {
    const source = await fetchBuffer(imageUrl, { signal: AbortSignal.timeout(8000) });
    const image = sharp(source, { failOn: "none" });
    const metadata = await image.metadata();
    if (!metadata.width || !metadata.height || metadata.width < 300 || metadata.height < 300) {
      return false;
    }
    const stats = await image.stats();
    const meanSpread =
      Math.abs(stats.channels[0]?.mean - stats.channels[1]?.mean)
      + Math.abs(stats.channels[1]?.mean - stats.channels[2]?.mean)
      + Math.abs(stats.channels[0]?.mean - stats.channels[2]?.mean);
    const luminanceRange =
      Math.max(stats.channels[0]?.max ?? 0, stats.channels[1]?.max ?? 0, stats.channels[2]?.max ?? 0)
      - Math.min(stats.channels[0]?.min ?? 255, stats.channels[1]?.min ?? 255, stats.channels[2]?.min ?? 255);
    const averageMean = ((stats.channels[0]?.mean ?? 0) + (stats.channels[1]?.mean ?? 0) + (stats.channels[2]?.mean ?? 0)) / 3;
    const averageStdev = ((stats.channels[0]?.stdev ?? 0) + (stats.channels[1]?.stdev ?? 0) + (stats.channels[2]?.stdev ?? 0)) / 3;
    if ((luminanceRange < 18 && meanSpread < 12) || (averageMean > 205 && averageStdev < 18)) {
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

export async function renderPlaylistArtwork(title: string, imageUrl: string) {
  const cleanTitle = dedupeAdjacentWords(title) || "Untitled Playlist";
  const source = await fetchBuffer(imageUrl);
  const sourceColors = await extractSourceColors(source);
  const base = sharp(source).resize(1200, 1200, { fit: "cover", position: "attention" }).ensureAlpha();
  const toneMap = await base
    .clone()
    .normalize()
    .linear(1.5, -36)
    .gamma(1.12)
    .raw()
    .toBuffer();
  const mappedPixels = Buffer.alloc(1200 * 1200 * 4);
  for (let index = 0; index < toneMap.length; index += 4) {
    const r = toneMap[index];
    const g = toneMap[index + 1];
    const b = toneMap[index + 2];
    const luminance = Math.max(0, Math.min(1, (0.299 * r + 0.587 * g + 0.114 * b) / 255));
    const mappedLuminance = Math.pow(luminance, 0.85);
    const offset = index;
    mappedPixels[offset] = Math.round(sourceColors.darkR + (sourceColors.lightR - sourceColors.darkR) * mappedLuminance);
    mappedPixels[offset + 1] = Math.round(sourceColors.darkG + (sourceColors.lightG - sourceColors.darkG) * mappedLuminance);
    mappedPixels[offset + 2] = Math.round(sourceColors.darkB + (sourceColors.lightB - sourceColors.darkB) * mappedLuminance);
    mappedPixels[offset + 3] = 255;
  }
  const detailLayer = await base
    .clone()
    .grayscale()
    .normalize()
    .linear(1.18, -12)
    .sharpen({ sigma: 1, m1: 1.2, m2: 2, x1: 2, y2: 10, y3: 16 })
    .png()
    .toBuffer();
  const mappedImage = await sharp(mappedPixels, {
    raw: { width: 1200, height: 1200, channels: 4 }
  })
    .png()
    .toBuffer();
  const midpointLuminance = (
    (sourceColors.darkR + sourceColors.lightR) / 2 * 0.299 +
    (sourceColors.darkG + sourceColors.lightG) / 2 * 0.587 +
    (sourceColors.darkB + sourceColors.lightB) / 2 * 0.114
  );
  const textColor = midpointLuminance > 110 ? "#111111" : "#ffffff";
  const titleOverlay = await renderArtworkTitleOverlay(cleanTitle.split(/\s+/).filter(Boolean), textColor);
  return sharp({
    create: { width: 1200, height: 1200, channels: 4, background: "#000000" }
  })
    .composite([
      { input: mappedImage, blend: "over" },
      { input: detailLayer, blend: "soft-light" },
      { input: titleOverlay, blend: "over" }
    ])
    .jpeg({ quality: 90, mozjpeg: true })
    .toBuffer();
}
