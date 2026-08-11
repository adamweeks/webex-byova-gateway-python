# ruff: noqa: UP006
"""Audio profiles and codec helpers for the direct voice-agent browser client."""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass
from typing import Dict, Iterable, List

from src.utils.telephony_audio import (
    G711MulawOutputConverter,
    linear16_to_mulaw,
    mulaw_to_linear16,
)


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
    transport_encoding: str | None = None
    transport_sample_rate_hertz: int | None = None

    @property
    def browser_encoding(self) -> str:
        """Return the codec produced after optional local transcoding."""
        return self.transport_encoding or self.output_encoding

    @property
    def browser_sample_rate_hertz(self) -> int:
        """Return the sample rate produced after optional local transcoding."""
        return self.transport_sample_rate_hertz or self.output_sample_rate_hertz

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
            "transportEncoding": self.browser_encoding,
            "transportSampleRateHertz": self.browser_sample_rate_hertz,
            "transcoded": bool(self.transport_encoding),
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
        label="GECX direct mu-law",
        description=(
            "GECX returns 8 kHz mu-law directly; no connector output "
            "conversion."
        ),
        input_encoding="MULAW",
        input_sample_rate_hertz=8000,
        output_encoding="MULAW",
        output_sample_rate_hertz=8000,
    ),
    "connector_mulaw": AudioProfile(
        id="connector_mulaw",
        label="Connector mu-law from GECX PCM",
        description=(
            "GECX 24 kHz linear PCM output, locally filtered and encoded once "
            "as 8 kHz mu-law."
        ),
        input_encoding="MULAW",
        input_sample_rate_hertz=8000,
        output_encoding="LINEAR16",
        output_sample_rate_hertz=24000,
        transport_encoding="MULAW",
        transport_sample_rate_hertz=8000,
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


@dataclass(frozen=True)
class ConvertedOutputAudio:
    """One CES output frame after its selected transport conversion."""

    pcm16: bytes
    transport_audio: bytes
    transport_encoding: str
    sample_rate_hertz: int


class OutputAudioConverter:
    """Convert provider output into the codec that the transport would carry."""

    def __init__(self, profile: AudioProfile) -> None:
        self.profile = profile
        self._mulaw_converter: G711MulawOutputConverter | None = None
        if profile.browser_encoding == "MULAW":
            self._mulaw_converter = G711MulawOutputConverter(
                profile.output_encoding,
                profile.output_sample_rate_hertz,
            )

    def process(self, ces_audio: bytes) -> ConvertedOutputAudio:
        """Return transport bytes plus browser-playable PCM for one CES frame."""
        if self.profile.browser_encoding == "MULAW":
            assert self._mulaw_converter is not None
            transport_audio = self._mulaw_converter.process(ces_audio)
            browser_pcm = mulaw_to_linear16(transport_audio)
        elif (
            self.profile.browser_encoding == "LINEAR16"
            and self.profile.output_encoding == "LINEAR16"
            and self.profile.browser_sample_rate_hertz
            == self.profile.output_sample_rate_hertz
        ):
            transport_audio = ces_audio[: len(ces_audio) // 2 * 2]
            browser_pcm = transport_audio
        else:
            raise ValueError(
                "Unsupported CES-to-transport conversion: "
                f"{self.profile.output_encoding}/{self.profile.output_sample_rate_hertz} "
                f"to {self.profile.browser_encoding}/"
                f"{self.profile.browser_sample_rate_hertz}"
            )

        return ConvertedOutputAudio(
            pcm16=browser_pcm,
            transport_audio=transport_audio,
            transport_encoding=self.profile.browser_encoding,
            sample_rate_hertz=self.profile.browser_sample_rate_hertz,
        )


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
    return OutputAudioConverter(profile).process(ces_audio).pcm16
