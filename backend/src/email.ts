import nodemailer from "nodemailer";
import type { PlaylistCandidate } from "./playlist-generator.js";
import { createLogger } from "./logger.js";

const log = createLogger("email");

export type SmtpConfig = {
  host: string;
  port: number;
  secure: boolean;
  user: string;
  password: string;
  from: string;
  to: string;
  tlsInsecure: boolean;
};

export function isSmtpConfigured(config: Pick<SmtpConfig, "host" | "user" | "to">): boolean {
  return Boolean(config.host.trim() && config.user.trim() && config.to.trim());
}

export async function sendSmtpEmail(config: SmtpConfig, subject: string, html: string): Promise<void> {
  const transport = nodemailer.createTransport({
    host: config.host.trim(),
    port: config.port,
    secure: config.secure,
    auth: { user: config.user.trim(), pass: config.password },
    tls: { rejectUnauthorized: !config.tlsInsecure },
    connectionTimeout: 15_000,
    greetingTimeout: 15_000
  });
  try {
    await transport.sendMail({
      from: config.from.trim() || config.user.trim(),
      to: config.to,
      subject,
      html
    });
  } finally {
    await transport.close();
  }
}

function escapeHtml(value: string): string {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

export type WeeklyDigestInput = {
  candidates: PlaylistCandidate[];
  sourceTracks: number;
};

export type WeeklyDigest = {
  subject: string;
  html: string;
};

export function buildWeeklyDigest({ candidates, sourceTracks }: WeeklyDigestInput): WeeklyDigest {
  const now = new Date();
  const dateLabel = now.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric", timeZone: "UTC" });
  const totalTracks = candidates.reduce((sum, candidate) => sum + candidate.trackIds.length, 0);
  const playlistRows = candidates
    .map((candidate, index) => {
      const tags = candidate.signatureTags.slice(0, 4).map(escapeHtml).join(", ");
      return `
      <tr>
        <td class="num">${index + 1}</td>
        <td class="name">${escapeHtml(candidate.name)}</td>
        <td class="count">${candidate.trackIds.length} tracks</td>
        <td class="tags">${tags || "—"}</td>
      </tr>`;
    })
    .join("");
  const subject = `Sortify: ${candidates.length} playlist${candidates.length === 1 ? "" : "s"} refreshed (${dateLabel})`;
  const html = `
<!DOCTYPE html>
<html>
<body style="margin:0;padding:24px;background:#f4f4f5;color:#18181b;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
  <div style="max-width:640px;margin:0 auto;">
    <h1 style="font-size:20px;margin:0 0 4px;">Sortify playlist refresh</h1>
    <p style="margin:0 0 16px;color:#52525b;font-size:14px;">
      Your playlists were regenerated on ${escapeHtml(dateLabel)} from a library of ${sourceTracks} tracks.
    </p>
    <table style="width:100%;border-collapse:collapse;font-size:14px;">
      <thead>
        <tr style="border-bottom:2px solid #d4d4d8;text-align:left;color:#71717a;font-size:12px;text-transform:uppercase;">
          <th style="padding:8px 6px;width:24px;">#</th>
          <th style="padding:8px 6px;">Playlist</th>
          <th style="padding:8px 6px;width:90px;">Tracks</th>
          <th style="padding:8px 6px;">Signature tags</th>
        </tr>
      </thead>
      <tbody>${playlistRows}
      </tbody>
    </table>
    <p style="margin:16px 0 0;color:#71717a;font-size:13px;">
      ${candidates.length} playlist${candidates.length === 1 ? "" : "s"} · ${totalTracks} total tracks
    </p>
  </div>
</body>
</html>`;
  return { subject, html };
}

export async function sendWeeklyDigestEmail(config: SmtpConfig, input: WeeklyDigestInput): Promise<{ sent: boolean; error: string | null }> {
  if (!isSmtpConfigured(config)) {
    return { sent: false, error: "SMTP is not fully configured (host, user, and recipient are required)" };
  }
  const digest = buildWeeklyDigest(input);
  try {
    await sendSmtpEmail(config, digest.subject, digest.html);
    log.info("weekly digest email sent", { to: config.to, playlists: input.candidates.length });
    return { sent: true, error: null };
  } catch (error) {
    const message = error instanceof Error ? error.message : "SMTP send failed";
    log.error("weekly digest email failed", { to: config.to, error: message });
    return { sent: false, error: message };
  }
}
