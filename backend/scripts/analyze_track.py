#!/usr/bin/env python3
import argparse
import json
import math
import subprocess
import sys

import numpy as np


def clamp_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def load_audio(url: str, sample_seconds: int, sample_rate: int) -> np.ndarray:
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        url,
        "-vn",
        "-ac",
        "1",
        "-ar",
        str(sample_rate),
        "-t",
        str(sample_seconds),
        "-f",
        "s16le",
        "-"
    ]
    result = subprocess.run(command, capture_output=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="ignore").strip() or "ffmpeg decode failed")
    audio = np.frombuffer(result.stdout, dtype=np.int16).astype(np.float32)
    if audio.size < sample_rate * 5:
        raise RuntimeError("not enough audio decoded for analysis")
    return audio / 32768.0


def estimate_tempo(onset_envelope: np.ndarray, sample_rate: int, hop_length: int) -> tuple[float | None, float]:
    if onset_envelope.size < 16:
        return None, 0.0
    centered = onset_envelope - float(np.mean(onset_envelope))
    autocorr = np.correlate(centered, centered, mode="full")[centered.size - 1 :]
    min_bpm = 60.0
    max_bpm = 190.0
    min_lag = max(1, int(round((60.0 * sample_rate) / (max_bpm * hop_length))))
    max_lag = max(min_lag + 1, int(round((60.0 * sample_rate) / (min_bpm * hop_length))))
    window = autocorr[min_lag:max_lag]
    if window.size == 0:
        return None, 0.0
    best_index = int(np.argmax(window))
    best_lag = min_lag + best_index
    peak = float(window[best_index])
    reference = float(autocorr[0]) if autocorr[0] > 0 else 1.0
    confidence = clamp_unit(peak / reference)
    bpm = (60.0 * sample_rate) / (best_lag * hop_length)
    return bpm, confidence


def summarize(audio: np.ndarray, sample_rate: int) -> dict:
    frame_length = 2048
    hop_length = 512
    padded = np.pad(audio, (0, frame_length), mode="constant")
    frames = np.lib.stride_tricks.sliding_window_view(padded, frame_length)[::hop_length]
    window = np.hanning(frame_length).astype(np.float32)
    windowed = frames * window
    spectra = np.abs(np.fft.rfft(windowed, axis=1)) + 1e-9
    freqs = np.fft.rfftfreq(frame_length, 1.0 / sample_rate)

    rms = np.sqrt(np.mean(frames * frames, axis=1) + 1e-12)
    loudness_db = 20.0 * np.log10(float(np.percentile(rms, 75)) + 1e-9)
    loudness = clamp_unit((loudness_db + 45.0) / 39.0)
    energy = clamp_unit(float(np.percentile(rms, 85)) * 3.2)

    centroid = np.sum(spectra * freqs, axis=1) / np.sum(spectra, axis=1)
    brightness = clamp_unit((float(np.median(centroid)) - 350.0) / 2800.0)

    cumulative = np.cumsum(spectra, axis=1)
    thresholds = cumulative[:, -1:] * 0.85
    rolloff_indices = np.argmax(cumulative >= thresholds, axis=1)
    rolloff_freq = freqs[np.clip(rolloff_indices, 0, freqs.size - 1)]

    flatness = np.exp(np.mean(np.log(spectra), axis=1)) / np.mean(spectra, axis=1)
    flatness_value = clamp_unit(float(np.median(flatness)) * 3.0)

    zero_cross = np.mean(np.abs(np.diff(np.signbit(frames), axis=1)), axis=1)
    zcr_value = clamp_unit(float(np.median(zero_cross)) * 6.0)

    normalized = spectra / np.sum(spectra, axis=1, keepdims=True)
    flux = np.sqrt(np.sum(np.diff(normalized, axis=0) ** 2, axis=1))
    onset_envelope = np.maximum(0.0, flux - float(np.median(flux)))
    bpm, periodicity = estimate_tempo(onset_envelope, sample_rate, hop_length)
    rhythmic_density = clamp_unit(float(np.mean(onset_envelope)) * 12.0)

    tempo_norm = clamp_unit(((bpm or 110.0) - 70.0) / 90.0)
    danceability = clamp_unit((energy * 0.4) + (tempo_norm * 0.25) + (periodicity * 0.25) + (rhythmic_density * 0.1))
    acousticness = clamp_unit(1.0 - ((brightness * 0.28) + (flatness_value * 0.3) + (zcr_value * 0.22) + (energy * 0.2)))
    instrumentalness = clamp_unit((acousticness * 0.35) + ((1.0 - rhythmic_density) * 0.2) + ((1.0 - flatness_value) * 0.2) + ((1.0 - zcr_value) * 0.25))
    valence = clamp_unit((brightness * 0.35) + (energy * 0.35) + (danceability * 0.2) + ((1.0 - acousticness) * 0.1))

    mood_tags: list[str] = []
    if bpm is not None:
        if bpm < 95:
            mood_tags.extend(["low tempo", "laid-back"])
        elif bpm > 128:
            mood_tags.extend(["driving", "fast tempo"])
        else:
            mood_tags.append("mid tempo")
    if energy > 0.68:
        mood_tags.extend(["energetic", "high energy"])
    elif energy < 0.35:
        mood_tags.extend(["calm", "gentle"])
    if danceability > 0.66:
        mood_tags.extend(["danceable", "groove"])
    if acousticness > 0.62:
        mood_tags.extend(["organic", "acoustic"])
    elif acousticness < 0.32:
        mood_tags.extend(["electronic", "processed"])
    if brightness > 0.62:
        mood_tags.extend(["bright", "uplifting"])
    elif brightness < 0.32:
        mood_tags.extend(["warm", "dark"])
    if valence > 0.64:
        mood_tags.extend(["positive", "euphoric"])
    elif valence < 0.36:
        mood_tags.extend(["moody", "melancholic"])
    if rhythmic_density > 0.58:
        mood_tags.append("rhythmic")
    if instrumentalness > 0.62:
        mood_tags.append("textural")

    ordered_tags: list[str] = []
    seen: set[str] = set()
    for tag in mood_tags:
        normalized_tag = " ".join(tag.strip().lower().split())
        if normalized_tag and normalized_tag not in seen:
            seen.add(normalized_tag)
            ordered_tags.append(normalized_tag)

    vector = [
        tempo_norm,
        energy,
        danceability,
        valence,
        acousticness,
        instrumentalness,
        brightness,
        rhythmic_density
    ]

    return {
        "features": {
            "version": "audio-v1",
            "durationSampled": int(round(audio.size / sample_rate)),
            "bpm": None if bpm is None else round(float(bpm), 2),
            "energy": round(energy, 4),
            "danceability": round(danceability, 4),
            "valence": round(valence, 4),
            "acousticness": round(acousticness, 4),
            "instrumentalness": round(instrumentalness, 4),
            "brightness": round(brightness, 4),
            "rhythmicDensity": round(rhythmic_density, 4),
            "loudness": round(loudness, 4),
            "moodTags": ordered_tags[:12],
            "spectralRolloff": round(clamp_unit(float(np.median(rolloff_freq)) / 5000.0), 4),
            "periodicity": round(periodicity, 4)
        },
        "vector": [round(clamp_unit(value), 4) for value in vector]
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--sample-seconds", type=int, default=90)
    parser.add_argument("--sample-rate", type=int, default=22050)
    args = parser.parse_args()
    try:
        audio = load_audio(args.url, args.sample_seconds, args.sample_rate)
        payload = summarize(audio, args.sample_rate)
        json.dump(payload, sys.stdout)
        sys.stdout.write("\n")
        return 0
    except Exception as error:
        sys.stderr.write(str(error))
        sys.stderr.write("\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
