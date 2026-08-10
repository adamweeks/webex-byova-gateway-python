# ruff: noqa: UP006
"""Audio profiles and codec helpers for the direct voice-agent browser client."""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass
from typing import Dict, Iterable, List


@dataclass(frozen=True)
class AudioProfile:
    """One CES input/output codec profile exposed by the lab."""

    id: str
    label: str
    description: str
    input_encoding: str
    input_sample_rate_hertz: int
    output_encoding: str
    output_sample_rate_hertz: int

    def public_dict(self) -> Dict[str, object]:
        """Return the safe browser representation of this profile."""
        return {
            "id": self.id,
            "label": self.label,
            "description": self.description,
            "inputEncoding": self.input_encoding,
            "inputSampleRateHertz": self.input_sample_rate_hertz,
            "outputEncoding": self.output_encoding,
            "outputSampleRateHertz": self.output_sample_rate_hertz,
        }


AUDIO_PROFILES: Dict[str, AudioProfile] = {
    "native": AudioProfile(
        id="native",
        label="Native PCM",
        description="16 kHz microphone input and 24 kHz linear PCM output.",
        input_encoding="LINEAR16",
        input_sample_rate_hertz=16000,
        output_encoding="LINEAR16",
        output_sample_rate_hertz=24000,
    ),
    "wxcc": AudioProfile(
        id="wxcc",
        label="WxCC-like codec",
        description="8 kHz mu-law in both directions, without a WxCC call.",
        input_encoding="MULAW",
        input_sample_rate_hertz=8000,
        output_encoding="MULAW",
        output_sample_rate_hertz=8000,
    ),
    "lex_native": AudioProfile(
        id="lex_native",
        label="AWS Lex native PCM",
        description="16 kHz linear PCM in both directions.",
        input_encoding="LINEAR16",
        input_sample_rate_hertz=16000,
        output_encoding="LINEAR16",
        output_sample_rate_hertz=16000,
    ),
}


_MULAW_BIAS = 0x84
_MULAW_CLIP = 32635


def linear16_to_mulaw(pcm: bytes) -> bytes:
    """Encode little-endian signed 16-bit PCM as G.711 mu-law."""
    if len(pcm) % 2:
        pcm = pcm[:-1]
    encoded = bytearray()
    for (sample,) in struct.iter_unpack("<h", pcm):
        sign = 0x80 if sample < 0 else 0
        magnitude = min(abs(sample), _MULAW_CLIP) + _MULAW_BIAS
        exponent = max(0, min(7, magnitude.bit_length() - 8))
        mantissa = (magnitude >> (exponent + 3)) & 0x0F
        encoded.append((~(sign | (exponent << 4) | mantissa)) & 0xFF)
    return bytes(encoded)


def mulaw_to_linear16(encoded: bytes) -> bytes:
    """Decode G.711 mu-law as little-endian signed 16-bit PCM."""
    samples: List[int] = []
    for value in encoded:
        value = (~value) & 0xFF
        sign = value & 0x80
        exponent = (value >> 4) & 0x07
        mantissa = value & 0x0F
        magnitude = ((mantissa << 3) + _MULAW_BIAS) << exponent
        sample = magnitude - _MULAW_BIAS
        samples.append(-sample if sign else sample)
    return b"".join(struct.pack("<h", sample) for sample in samples)


def pcm16_rms(pcm: bytes) -> float:
    """Return RMS amplitude for little-endian signed 16-bit PCM."""
    if len(pcm) < 2:
        return 0.0
    samples: Iterable[int] = (
        sample[0] for sample in struct.iter_unpack("<h", pcm[: len(pcm) // 2 * 2])
    )
    total = 0
    count = 0
    for sample in samples:
        total += sample * sample
        count += 1
    return math.sqrt(total / count) if count else 0.0


def silence_chunks(profile: AudioProfile, duration_ms: int) -> List[bytes]:
    """Build 100 ms CES audio chunks for a requested silence duration."""
    chunks: List[bytes] = []
    remaining_ms = max(0, duration_ms)
    bytes_per_sample = 2 if profile.input_encoding == "LINEAR16" else 1
    silence_byte = b"\x00" if profile.input_encoding == "LINEAR16" else b"\xff"
    while remaining_ms:
        chunk_ms = min(100, remaining_ms)
        sample_count = profile.input_sample_rate_hertz * chunk_ms // 1000
        chunks.append(silence_byte * sample_count * bytes_per_sample)
        remaining_ms -= chunk_ms
    return chunks


def ces_input_audio(profile: AudioProfile, pcm16: bytes) -> bytes:
    """Convert browser PCM16 into the selected CES input encoding."""
    if profile.input_encoding == "MULAW":
        return linear16_to_mulaw(pcm16)
    return pcm16[: len(pcm16) // 2 * 2]


def browser_output_audio(profile: AudioProfile, ces_audio: bytes) -> bytes:
    """Convert selected CES output into browser-playable PCM16."""
    if profile.output_encoding == "MULAW":
        return mulaw_to_linear16(ces_audio)
    return ces_audio[: len(ces_audio) // 2 * 2]
