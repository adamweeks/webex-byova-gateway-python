class PcmCaptureProcessor extends AudioWorkletProcessor {
  constructor(options) {
    super();
    const requested = options.processorOptions?.frameSamples || Math.round(sampleRate * 0.02);
    this.frameSamples = Math.max(128, requested);
    this.buffer = new Float32Array(this.frameSamples);
    this.offset = 0;
    this.active = true;
    this.port.onmessage = (event) => {
      if (event.data?.type === "stop") this.active = false;
    };
  }

  process(inputs) {
    if (!this.active) return false;
    const channel = inputs[0]?.[0];
    if (!channel) return true;
    let sourceOffset = 0;
    while (sourceOffset < channel.length) {
      const count = Math.min(channel.length - sourceOffset, this.frameSamples - this.offset);
      this.buffer.set(channel.subarray(sourceOffset, sourceOffset + count), this.offset);
      this.offset += count;
      sourceOffset += count;
      if (this.offset === this.frameSamples) {
        const frame = this.buffer;
        this.port.postMessage(frame, [frame.buffer]);
        this.buffer = new Float32Array(this.frameSamples);
        this.offset = 0;
      }
    }
    return true;
  }
}

registerProcessor("pcm-capture", PcmCaptureProcessor);
