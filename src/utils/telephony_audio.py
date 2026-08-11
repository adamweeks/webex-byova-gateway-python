"""Streaming audio conversion helpers for 8 kHz G.711 telephony output."""

from __future__ import annotations

import struct

import numpy as np

_MULAW_BIAS = 0x84
_MULAW_CLIP = 32635


def linear16_to_mulaw(pcm: bytes) -> bytes:
    """Encode little-endian signed 16-bit PCM as G.711 mu-law."""
    even_pcm = pcm[: len(pcm) // 2 * 2]
    encoded = bytearray()
    for (sample,) in struct.iter_unpack("<h", even_pcm):
        sign = 0x80 if sample < 0 else 0
        magnitude = min(abs(sample), _MULAW_CLIP) + _MULAW_BIAS
        exponent = max(0, min(7, magnitude.bit_length() - 8))
        mantissa = (magnitude >> (exponent + 3)) & 0x0F
        encoded.append((~(sign | (exponent << 4) | mantissa)) & 0xFF)
    return bytes(encoded)


def mulaw_to_linear16(encoded: bytes) -> bytes:
    """Decode G.711 mu-law as little-endian signed 16-bit PCM."""
    samples: list[int] = []
    for value in encoded:
        value = (~value) & 0xFF
        sign = value & 0x80
        exponent = (value >> 4) & 0x07
        mantissa = value & 0x0F
        magnitude = ((mantissa << 3) + _MULAW_BIAS) << exponent
        sample = magnitude - _MULAW_BIAS
        samples.append(-sample if sign else sample)
    return b"".join(struct.pack("<h", sample) for sample in samples)


class Linear16Resampler:
    """Stateful anti-aliased integer-ratio PCM16 downsampler."""

    def __init__(self, input_rate_hertz: int, output_rate_hertz: int) -> None:
        if input_rate_hertz <= output_rate_hertz:
            raise ValueError("Linear16Resampler only supports downsampling")
        if input_rate_hertz % output_rate_hertz:
            raise ValueError("Linear16Resampler requires an integer sample-rate ratio")
        self.input_rate_hertz = input_rate_hertz
        self.output_rate_hertz = output_rate_hertz
        self._ratio = input_rate_hertz // output_rate_hertz

        # A 127-tap Kaiser-windowed sinc stays within about 1.2 dB through the
        # 3.4 kHz telephony passband while rejecting content at the 4 kHz output
        # Nyquist limit by more than 40 dB.
        tap_count = 127
        cutoff_cycles_per_sample = 0.45 / self._ratio
        positions = np.arange(tap_count, dtype=np.float64) - (tap_count - 1) / 2
        taps = (
            2
            * cutoff_cycles_per_sample
            * np.sinc(2 * cutoff_cycles_per_sample * positions)
            * np.kaiser(tap_count, 8.6)
        )
        self._taps = taps / np.sum(taps)
        self.reset()

    def reset(self) -> None:
        """Clear FIR history and decimation phase for a new output turn."""
        self._processed_samples = 0
        self._history = np.zeros(len(self._taps) - 1, dtype=np.float64)

    def process(self, pcm16: bytes) -> bytes:
        """Downsample one PCM16 frame while retaining streaming state."""
        even_pcm = pcm16[: len(pcm16) // 2 * 2]
        if not even_pcm:
            return b""
        samples = np.frombuffer(even_pcm, dtype="<i2").astype(np.float64)
        extended = np.concatenate((self._history, samples))
        filtered = np.convolve(extended, self._taps, mode="valid")
        first_output = (-self._processed_samples) % self._ratio
        downsampled = filtered[first_output :: self._ratio]
        self._history = extended[-len(self._history) :]
        self._processed_samples += len(samples)
        clipped = np.clip(np.rint(downsampled), -32768, 32767).astype("<i2")
        return clipped.tobytes()


class G711MulawOutputConverter:
    """Convert streamed provider audio into WxCC-compatible 8 kHz mu-law."""

    def __init__(self, source_encoding: str, source_rate_hertz: int) -> None:
        normalized = (
            source_encoding.upper()
            .replace("AUDIO_ENCODING_", "")
            .replace("-", "_")
        )
        if normalized in {"MULAW", "ULAW", "LINEAR_16_MULAW"}:
            self.source_encoding = "MULAW"
        elif normalized in {"LINEAR16", "LINEAR_16"}:
            self.source_encoding = "LINEAR16"
        else:
            raise ValueError(f"Unsupported provider output encoding: {source_encoding}")
        self.source_rate_hertz = source_rate_hertz
        self._resampler: Linear16Resampler | None = None
        if source_rate_hertz != 8000:
            if self.source_encoding != "LINEAR16":
                raise ValueError("Only LINEAR16 provider output can be resampled")
            self._resampler = Linear16Resampler(source_rate_hertz, 8000)

    def reset(self) -> None:
        """Reset streaming resampler state for a new provider output turn."""
        if self._resampler is not None:
            self._resampler.reset()

    def process(self, provider_audio: bytes) -> bytes:
        """Return one streamed 8 kHz G.711 mu-law transport frame."""
        if self.source_encoding == "MULAW":
            return provider_audio
        linear16 = provider_audio[: len(provider_audio) // 2 * 2]
        if self._resampler is not None:
            linear16 = self._resampler.process(linear16)
        return linear16_to_mulaw(linear16)
