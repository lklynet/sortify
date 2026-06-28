import sharp from "sharp";
import { QuantizerCelebi, Score } from "@material/material-color-utilities";
import { createLogger } from "./logger.js";

const log = createLogger("artwork");

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
  return text.length * fontSize * 0.56;
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
  const maxWidth = 1200 - 72 * 2;
  const maxHeight = 1200 - 144;
  let fontSize = 132;
  let lineHeight: number;
  let wrapped: string[];
  let blockHeight: number;
  while (true) {
    lineHeight = Math.round(fontSize * 1.05);
    wrapped = wrapTitleWords(safeLines, maxWidth, fontSize);
    blockHeight = fontSize + (wrapped.length - 1) * lineHeight;
    if (blockHeight <= maxHeight) break;
    fontSize -= 2;
    if (fontSize <= 24) break;
  }
  return { fontSize, lineHeight, blockHeight, lines: wrapped };
}

async function renderArtworkTitleOverlay(lines: string[]) {
  const layout = layoutTitle(lines);
  const firstBaseline = (1200 - layout.blockHeight) / 2 + layout.fontSize;
  const leftInset = 72;
  const tspans = layout.lines
    .map((line, index) =>
      index === 0
        ? escapeXml(line)
        : `<tspan x="${leftInset}" dy="${layout.lineHeight}">${escapeXml(line)}</tspan>`
    )
    .join("");
  const svg = `
    <svg width="1200" height="1200" viewBox="0 0 1200 1200" xmlns="http://www.w3.org/2000/svg">
      <text x="${leftInset}" y="${firstBaseline}" font-family="Arial Black, Helvetica Neue, Arial, sans-serif" font-size="${layout.fontSize}" font-weight="900" letter-spacing="-1.8" fill="#ffffff">${tspans}</text>
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

export async function renderPlaylistArtwork(title: string, imageUrl: string) {
  const cleanTitle = dedupeAdjacentWords(title) || "Untitled Playlist";
  const source = await fetchBuffer(imageUrl);
  const sourceColors = await extractSourceColors(source);
  const base = sharp(source, { animated: false }).resize(1200, 1200, { fit: "cover", position: "attention" }).ensureAlpha();
  const toneMap = await base
    .clone()
    .grayscale()
    .normalize()
    .linear(1.5, -36)
    .gamma(1.12)
    .raw()
    .toBuffer();
  const mappedPixels = Buffer.alloc(1200 * 1200 * 3);
  for (let index = 0; index < toneMap.length; index++) {
    const luminance = Math.max(0, Math.min(1, toneMap[index] / 255));
    const mappedLuminance = 0.05 + luminance ** 1.38 * 0.32;
    const offset = index * 3;
    mappedPixels[offset] = Math.round(sourceColors.darkR + (sourceColors.lightR - sourceColors.darkR) * mappedLuminance);
    mappedPixels[offset + 1] = Math.round(sourceColors.darkG + (sourceColors.lightG - sourceColors.darkG) * mappedLuminance);
    mappedPixels[offset + 2] = Math.round(sourceColors.darkB + (sourceColors.lightB - sourceColors.darkB) * mappedLuminance);
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
    raw: { width: 1200, height: 1200, channels: 3 }
  })
    .png()
    .toBuffer();
  const darkWash = await sharp({
    create: { width: 1200, height: 1200, channels: 4, background: { r: 20, g: 12, b: 18, alpha: 0.18 } }
  })
    .png()
    .toBuffer();
  const titleOverlay = await renderArtworkTitleOverlay(cleanTitle.split(/\s+/).filter(Boolean));
  return sharp({
    create: { width: 1200, height: 1200, channels: 4, background: "#000000" }
  })
    .composite([
      { input: mappedImage, blend: "over" },
      { input: detailLayer, blend: "overlay" },
      { input: darkWash, blend: "multiply" },
      { input: titleOverlay, blend: "over" }
    ])
    .linear(0.96, -10)
    .jpeg({ quality: 90, mozjpeg: true })
    .toBuffer();
}
