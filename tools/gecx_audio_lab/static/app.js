const elements = {
  connectionBadge: document.querySelector("#connectionBadge"),
  connectionLabel: document.querySelector("#connectionLabel"),
  targetSelect: document.querySelector("#targetSelect"),
  profileSelect: document.querySelector("#profileSelect"),
  endpointingSelect: document.querySelector("#endpointingSelect"),
  initialText: document.querySelector("#initialText"),
  projectValue: document.querySelector("#projectValue"),
  appValue: document.querySelector("#appValue"),
  modeValue: document.querySelector("#modeValue"),
  runtimeValue: document.querySelector("#runtimeValue"),
  providerRoute: document.querySelector("#providerRoute"),
  outputScopeLabel: document.querySelector("#outputScopeLabel"),
  turnCopy: document.querySelector("#turnCopy"),
  startButton: document.querySelector("#startButton"),
  endButton: document.querySelector("#endButton"),
  talkButton: document.querySelector("#talkButton"),
  talkLabel: document.querySelector("#talkLabel"),
  talkHint: document.querySelector("#talkHint"),
  textForm: document.querySelector("#textForm"),
  textInput: document.querySelector("#textInput"),
  sendTextButton: document.querySelector("#sendTextButton"),
  latencyMetric: document.querySelector("#latencyMetric"),
  durationMetric: document.querySelector("#durationMetric"),
  anomalyMetric: document.querySelector("#anomalyMetric"),
  rmsMetric: document.querySelector("#rmsMetric"),
  metricAlert: document.querySelector(".metric-alert"),
  playbackModeSelect: document.querySelector("#playbackModeSelect"),
  stopPlaybackButton: document.querySelector("#stopPlaybackButton"),
  downloadButton: document.querySelector("#downloadButton"),
  clearButton: document.querySelector("#clearButton"),
  transcript: document.querySelector("#transcript"),
  eventLog: document.querySelector("#eventLog"),
  scopeCanvas: document.querySelector("#scopeCanvas"),
};

const LOW_ENERGY_RMS_LIMIT = 300;

const state = {
  config: null,
  socket: null,
  socketPromise: null,
  sessionActive: false,
  turnActive: false,
  continuousMic: false,
  recording: false,
  microphoneMuted: false,
  capture: null,
  pendingAudioMetadata: null,
  playbackContext: null,
  nextPlaybackAt: 0,
  activeSources: new Set(),
  pcmChunks: [],
  wavSampleRate: null,
  rawDurationMs: 0,
  anomalyCount: 0,
};

class StreamingResampler {
  constructor(inputRate, outputRate) {
    this.ratio = inputRate / outputRate;
    this.position = 0;
    this.previous = null;
  }

  process(input) {
    if (this.ratio === 1) return input;
    const source = new Float32Array(input.length + (this.previous === null ? 0 : 1));
    let offset = 0;
    if (this.previous !== null) {
      source[0] = this.previous;
      offset = 1;
    }
    source.set(input, offset);
    if (source.length < 2) {
      this.previous = source[0] ?? this.previous;
      return new Float32Array();
    }
    const output = [];
    let cursor = this.position;
    while (cursor < source.length - 1) {
      const base = Math.floor(cursor);
      const fraction = cursor - base;
      output.push(source[base] + (source[base + 1] - source[base]) * fraction);
      cursor += this.ratio;
    }
    this.previous = source[source.length - 1];
    this.position = cursor - (source.length - 1);
    return Float32Array.from(output);
  }
}

class PcmCapture {
  constructor(targetRate, onFrame) {
    this.targetRate = targetRate;
    this.onFrame = onFrame;
    this.stream = null;
    this.context = null;
    this.source = null;
    this.node = null;
    this.silentGain = null;
  }

  async start() {
    this.stream = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount: 1,
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
      },
    });
    this.context = new AudioContext({ sampleRate: this.targetRate, latencyHint: "interactive" });
    await this.context.audioWorklet.addModule("/static/pcm-capture-worklet.js");
    await this.context.resume();
    const resampler = new StreamingResampler(this.context.sampleRate, this.targetRate);
    this.node = new AudioWorkletNode(this.context, "pcm-capture", {
      processorOptions: { frameSamples: Math.round(this.context.sampleRate * 0.02) },
    });
    this.node.port.onmessage = (event) => {
      const input = event.data instanceof Float32Array ? event.data : new Float32Array(event.data);
      const resampled = resampler.process(input);
      if (!resampled.length) return;
      scope.updateInput(resampled);
      this.onFrame(floatToPcm16(resampled));
    };
    this.source = this.context.createMediaStreamSource(this.stream);
    this.silentGain = this.context.createGain();
    this.silentGain.gain.value = 0;
    this.source.connect(this.node);
    this.node.connect(this.silentGain).connect(this.context.destination);
    return this.context.sampleRate;
  }

  async stop() {
    this.node?.port.postMessage({ type: "stop" });
    this.source?.disconnect();
    this.node?.disconnect();
    this.silentGain?.disconnect();
    this.stream?.getTracks().forEach((track) => track.stop());
    if (this.context && this.context.state !== "closed") await this.context.close();
  }
}

class SignalScope {
  constructor(canvas) {
    this.canvas = canvas;
    this.context = canvas.getContext("2d");
    this.input = new Float32Array(256);
    this.output = new Float32Array(256);
    this.draw = this.draw.bind(this);
    requestAnimationFrame(this.draw);
  }

  updateInput(samples) { this.input = samples; }
  updateOutput(samples) { this.output = samples; }

  draw() {
    const ratio = window.devicePixelRatio || 1;
    const width = Math.max(1, Math.round(this.canvas.clientWidth * ratio));
    const height = Math.max(1, Math.round(this.canvas.clientHeight * ratio));
    if (this.canvas.width !== width || this.canvas.height !== height) {
      this.canvas.width = width;
      this.canvas.height = height;
    }
    this.context.clearRect(0, 0, width, height);
    this.drawSignal(this.input, "#ffb342", height * 0.36, height * 0.22);
    this.drawSignal(this.output, "#cbff3f", height * 0.67, height * 0.22);
    requestAnimationFrame(this.draw);
  }

  drawSignal(samples, color, center, amplitude) {
    if (!samples?.length) return;
    const { width } = this.canvas;
    this.context.beginPath();
    this.context.strokeStyle = color;
    this.context.lineWidth = Math.max(1, window.devicePixelRatio || 1);
    this.context.shadowColor = color;
    this.context.shadowBlur = 7;
    const step = Math.max(1, Math.floor(samples.length / Math.max(1, width)));
    let point = 0;
    for (let index = 0; index < samples.length; index += step) {
      const x = (point / Math.ceil(samples.length / step)) * width;
      const y = center - Math.max(-1, Math.min(1, samples[index])) * amplitude;
      if (point === 0) this.context.moveTo(x, y);
      else this.context.lineTo(x, y);
      point += 1;
    }
    this.context.stroke();
    this.context.shadowBlur = 0;
  }
}

const scope = new SignalScope(elements.scopeCanvas);

function floatToPcm16(samples) {
  const buffer = new ArrayBuffer(samples.length * 2);
  const view = new DataView(buffer);
  samples.forEach((sample, index) => {
    const clipped = Math.max(-1, Math.min(1, sample));
    view.setInt16(index * 2, clipped < 0 ? clipped * 0x8000 : clipped * 0x7fff, true);
  });
  return buffer;
}

function pcm16ToFloat(buffer) {
  const view = new DataView(buffer);
  const samples = new Float32Array(Math.floor(buffer.byteLength / 2));
  for (let index = 0; index < samples.length; index += 1) {
    const value = view.getInt16(index * 2, true);
    samples[index] = value < 0 ? value / 0x8000 : value / 0x7fff;
  }
  return samples;
}

function setConnection(stateName, label) {
  elements.connectionBadge.dataset.state = stateName;
  elements.connectionLabel.textContent = label;
}

function selectedTarget() {
  return state.config?.targets.find((target) => target.id === elements.targetSelect.value);
}

function selectedProfile() {
  return state.config?.profiles.find((profile) => profile.id === elements.profileSelect.value);
}

function renderTarget() {
  const target = selectedTarget();
  if (!target) return;
  elements.projectValue.textContent = target.providerLabel;
  elements.appValue.textContent = target.botName || target.applicationId;
  elements.modeValue.textContent = target.interactionLabel;
  elements.runtimeValue.textContent = target.credentialMode;
  elements.providerRoute.textContent = target.provider === "aws_lex" ? "AWS LEX" : "GECX BIDI";
  elements.outputScopeLabel.textContent = target.provider === "aws_lex" ? "LEX" : "GECX";
  const supported = new Set(target.supportedProfileIds);
  [...elements.profileSelect.options].forEach((option) => {
    option.hidden = !supported.has(option.value);
    option.disabled = !supported.has(option.value);
  });
  if (!supported.has(elements.profileSelect.value)) {
    elements.profileSelect.value = target.defaultProfileId;
  }
  elements.initialText.value = target.initialText || "";
  elements.turnCopy.innerHTML = target.interactionMode === "continuous"
    ? "<p>Open once to keep call audio streaming across agent responses.</p><p>Use <strong>Mute microphone</strong> without ending the provider session.</p>"
    : "<p>This matches the gateway connector: audio is buffered for one utterance.</p><p>Click <strong>Finish turn</strong> to submit it to AWS Lex.</p>";
  resetTalkButton();
}

function lockSetup(locked) {
  elements.targetSelect.disabled = locked;
  elements.profileSelect.disabled = locked;
  elements.endpointingSelect.disabled = locked;
  elements.initialText.disabled = locked;
  elements.startButton.disabled = locked;
  elements.endButton.disabled = !locked;
  elements.talkButton.disabled = !locked;
  elements.sendTextButton.disabled = !locked;
}

function setTurnActive(active) {
  state.turnActive = active;
  elements.talkButton.disabled = !state.sessionActive || (active && !state.recording);
  elements.sendTextButton.disabled = !state.sessionActive || active;
}

async function stopCapture(sendCommit = false) {
  const capture = state.capture;
  if (!capture && !state.recording) return;
  state.capture = null;
  state.recording = false;
  state.continuousMic = false;
  state.microphoneMuted = false;
  await capture?.stop();
  if (sendCommit && state.socket?.readyState === WebSocket.OPEN) {
    state.socket.send(JSON.stringify({ type: "commit" }));
  }
  resetTalkButton();
  setTurnActive(state.turnActive);
}

function logEvent(message, level = "info") {
  const item = document.createElement("li");
  const timestamp = document.createElement("time");
  timestamp.textContent = new Date().toLocaleTimeString([], { hour12: false });
  const copy = document.createElement("span");
  copy.textContent = message;
  if (level !== "info") copy.className = level;
  item.append(timestamp, copy);
  elements.eventLog.prepend(item);
  while (elements.eventLog.children.length > 160) elements.eventLog.lastElementChild.remove();
}

function addTranscript(speaker, text) {
  elements.transcript.querySelector(".empty-state")?.remove();
  const line = document.createElement("p");
  line.className = speaker === "CALLER" ? "caller" : "agent";
  const label = document.createElement("span");
  label.className = "speaker";
  label.textContent = `${speaker}  `;
  line.append(label, document.createTextNode(text));
  elements.transcript.append(line);
  elements.transcript.scrollTop = elements.transcript.scrollHeight;
}

async function openSocket() {
  if (state.socket?.readyState === WebSocket.OPEN) return state.socket;
  if (state.socketPromise) return state.socketPromise;
  setConnection("connecting", "opening socket");
  state.socketPromise = new Promise((resolve, reject) => {
    const protocol = location.protocol === "https:" ? "wss" : "ws";
    const socket = new WebSocket(`${protocol}://${location.host}/ws`);
    socket.binaryType = "arraybuffer";
    socket.onopen = () => {
      state.socket = socket;
      state.socketPromise = null;
      setConnection("connected", "lab ready");
      logEvent("Local WebSocket bridge ready");
      resolve(socket);
    };
    socket.onerror = () => {
      state.socketPromise = null;
      setConnection("error", "socket error");
      reject(new Error("Could not open the local WebSocket"));
    };
    socket.onclose = () => {
      void stopCapture(false);
      state.socket = null;
      state.sessionActive = false;
      state.turnActive = false;
      state.continuousMic = false;
      state.microphoneMuted = false;
      state.recording = false;
      lockSetup(false);
      setConnection("offline", "offline");
      logEvent("Local WebSocket closed", "warn");
    };
    socket.onmessage = handleSocketMessage;
  });
  return state.socketPromise;
}

async function handleSocketMessage(event) {
  if (typeof event.data !== "string") {
    if (!state.pendingAudioMetadata) {
      logEvent("Received audio without metadata", "error");
      return;
    }
    const metadata = state.pendingAudioMetadata;
    state.pendingAudioMetadata = null;
    await receiveAudio(event.data, metadata);
    return;
  }
  const message = JSON.parse(event.data);
  if (message.type === "audio") {
    state.pendingAudioMetadata = message;
    return;
  }
  switch (message.type) {
    case "socket_ready":
      break;
    case "session_started":
      state.sessionActive = true;
      lockSetup(true);
      setTurnActive(Boolean(message.initialTurnPending));
      setConnection("connected", `${selectedTarget()?.provider === "aws_lex" ? "AWS Lex" : "GECX"} live`);
      logEvent(`Direct session ${message.sessionId.slice(0, 8)} started`);
      if (selectedProfile()) {
        const profile = selectedProfile();
        const providerCodec = `${profile.outputEncoding}/${profile.outputSampleRateHertz.toLocaleString()} Hz`;
        const transportCodec = `${profile.transportEncoding}/${profile.transportSampleRateHertz.toLocaleString()} Hz`;
        logEvent(
          profile.transcoded
            ? `${profile.label}: provider ${providerCodec} → connector ${transportCodec}`
            : `${profile.label}: provider ${providerCodec} passed through as ${transportCodec}`,
        );
      }
      if (message.initialTurnPending) {
        elements.talkHint.textContent = "opening turn / waiting for provider";
      } else {
        resetTalkButton();
      }
      break;
    case "session_closed":
      await stopCapture(false);
      state.sessionActive = false;
      state.turnActive = false;
      state.continuousMic = false;
      state.microphoneMuted = false;
      state.recording = false;
      lockSetup(false);
      setConnection("connected", "lab ready");
      resetTalkButton();
      logEvent("Provider session closed");
      break;
    case "turn_started":
      state.continuousMic = Boolean(message.continuous);
      setTurnActive(true);
      logEvent(`Turn ${message.turnId}: ${message.continuous ? "continuous microphone" : "microphone turn"} opened`);
      await beginMicrophoneCapture();
      break;
    case "turn_committed":
      setTurnActive(true);
      logEvent(`Turn ${message.turnId}: input committed${message.endpointingSilenceMs ? ` with ${message.endpointingSilenceMs}ms tail` : ""}`);
      break;
    case "turn_completed":
      if (message.cancelled) {
        await stopCapture(false);
        setTurnActive(false);
        resetTalkButton();
        logEvent(`Turn ${message.turnId}: microphone input canceled`, "warn");
      } else if (message.continuous && state.recording) {
        state.continuousMic = true;
        setTurnActive(true);
        elements.talkHint.textContent = state.microphoneMuted
          ? "call open / microphone muted"
          : "microphone live / listening";
        logEvent(`Turn ${message.turnId}: provider output complete; microphone remains open`);
      } else {
        await stopCapture(false);
        setTurnActive(false);
        resetTalkButton();
        logEvent(`Turn ${message.turnId ?? "autonomous"}: provider output complete`);
      }
      break;
    case "transcript":
      addTranscript("CALLER", message.text);
      break;
    case "agent_text":
      addTranscript(selectedTarget()?.provider === "aws_lex" ? "AWS LEX" : "GECX", message.text);
      break;
    case "interruption":
      logEvent("Provider interruption signal", "warn");
      break;
    case "end_session":
      logEvent(`Provider ended the session (${message.source})`, "warn");
      break;
    case "error":
      if (["gecx", "aws_lex"].includes(message.source)) {
        setConnection("error", "provider error");
        await stopCapture(false);
        state.sessionActive = false;
        state.turnActive = false;
        state.continuousMic = false;
        state.microphoneMuted = false;
        lockSetup(false);
        resetTalkButton();
      } else if (message.source === "browser") {
        setConnection("error", "input error");
        await stopCapture(false);
        setTurnActive(false);
        resetTalkButton();
      }
      logEvent(message.message, "error");
      break;
    default:
      logEvent(`Event: ${message.type}`);
  }
}

async function ensurePlaybackContext() {
  if (!state.playbackContext || state.playbackContext.state === "closed") {
    state.playbackContext = new AudioContext({ latencyHint: "interactive" });
  }
  if (state.playbackContext.state === "suspended") await state.playbackContext.resume();
  return state.playbackContext;
}

async function receiveAudio(buffer, metadata) {
  const samples = pcm16ToFloat(buffer);
  scope.updateOutput(samples);
  state.pcmChunks.push(new Uint8Array(buffer.slice(0)));
  state.wavSampleRate = metadata.sampleRateHertz;
  state.rawDurationMs += metadata.encodedDurationMs;
  elements.durationMetric.textContent = `${(state.rawDurationMs / 1000).toFixed(1)}s`;
  elements.rmsMetric.textContent = Math.round(metadata.rms).toLocaleString();
  elements.downloadButton.disabled = false;
  if (metadata.commitToFirstAudioMs !== null) {
    elements.latencyMetric.textContent = `${(metadata.commitToFirstAudioMs / 1000).toFixed(2)}s`;
    logEvent(`First audio after ${metadata.commitToFirstAudioMs.toFixed(0)}ms`);
  }
  if (metadata.anomalouslyLong) {
    state.anomalyCount += 1;
    elements.anomalyMetric.textContent = String(state.anomalyCount);
    elements.metricAlert.classList.add("has-alert");
    logEvent(`Long raw frame: ${(metadata.encodedDurationMs / 1000).toFixed(2)}s / ${metadata.rawBytes.toLocaleString()} bytes`, "warn");
  }

  const bypassLowEnergyFrame = elements.playbackModeSelect.value === "bypass"
    && metadata.anomalouslyLong
    && metadata.rms < LOW_ENERGY_RMS_LIMIT;
  if (bypassLowEnergyFrame) {
    elements.talkHint.textContent = state.recording
      ? (state.microphoneMuted
        ? "quiet anomaly bypassed / microphone muted"
        : "quiet anomaly bypassed / microphone live")
      : "quiet anomaly bypassed / waiting";
    logEvent(
      `Playback bypassed ${(metadata.encodedDurationMs / 1000).toFixed(2)}s near-silent frame (RMS ${metadata.rms.toFixed(1)}); raw evidence retained`,
      "warn",
    );
    return;
  }

  const context = await ensurePlaybackContext();
  const audioBuffer = context.createBuffer(1, samples.length, metadata.sampleRateHertz);
  audioBuffer.copyToChannel(samples, 0);
  const source = context.createBufferSource();
  source.buffer = audioBuffer;
  source.connect(context.destination);
  const startsAt = Math.max(context.currentTime + 0.018, state.nextPlaybackAt);
  state.nextPlaybackAt = startsAt + audioBuffer.duration;
  state.activeSources.add(source);
  elements.stopPlaybackButton.disabled = false;
  elements.talkHint.textContent = "Provider audio received";
  source.onended = () => {
    state.activeSources.delete(source);
    if (!state.activeSources.size) {
      elements.stopPlaybackButton.disabled = true;
      elements.talkHint.textContent = state.recording
        ? (state.microphoneMuted ? "call open / microphone muted" : "microphone live / listening")
        : (state.sessionActive ? "open microphone to continue" : "session required");
    }
  };
  source.start(startsAt);
}

async function startSession() {
  try {
    const socket = await openSocket();
    await ensurePlaybackContext();
    clearEvidence();
    lockSetup(true);
    elements.talkButton.disabled = true;
    elements.sendTextButton.disabled = true;
    setConnection("connecting", "provider connecting");
    socket.send(JSON.stringify({
      type: "start",
      targetId: elements.targetSelect.value,
      profileId: elements.profileSelect.value,
      endpointingSilenceMs: Number(elements.endpointingSelect.value),
      initialText: elements.initialText.value,
    }));
  } catch (error) {
    lockSetup(false);
    setConnection("error", "start failed");
    logEvent(error.message, "error");
  }
}

async function endSession() {
  if (state.recording) await stopCapture(false);
  if (state.socket?.readyState === WebSocket.OPEN) {
    state.socket.send(JSON.stringify({ type: "stop" }));
  }
}

function startTalking() {
  if (!state.sessionActive || state.turnActive || state.recording) return;
  setTurnActive(true);
  elements.talkHint.textContent = "opening microphone turn";
  state.socket.send(JSON.stringify({
    type: "talk_start",
    continuous: selectedTarget()?.interactionMode === "continuous",
  }));
}

async function beginMicrophoneCapture() {
  if (!state.sessionActive || state.recording) return;
  const profile = selectedProfile();
  if (!profile) return;
  try {
    state.capture = new PcmCapture(profile.inputSampleRateHertz, (pcm) => {
      if (state.socket?.readyState !== WebSocket.OPEN) return;
      state.socket.send(state.microphoneMuted ? new ArrayBuffer(pcm.byteLength) : pcm);
    });
    const actualRate = await state.capture.start();
    state.recording = true;
    state.continuousMic = selectedTarget()?.interactionMode === "continuous";
    state.microphoneMuted = false;
    setTurnActive(true);
    elements.talkButton.classList.add("live");
    elements.talkLabel.textContent = state.continuousMic ? "Mute microphone" : "Finish turn";
    elements.talkHint.textContent = `${state.continuousMic ? "call open" : "recording turn"} / ${actualRate.toLocaleString()} Hz capture`;
    logEvent(`${state.continuousMic ? "Continuous microphone" : "Microphone turn"} active; streaming ${profile.inputEncoding} at ${profile.inputSampleRateHertz.toLocaleString()} Hz`);
  } catch (error) {
    state.socket?.send(JSON.stringify({ type: "commit" }));
    setTurnActive(false);
    resetTalkButton();
    logEvent(`Microphone unavailable: ${error.message}`, "error");
  }
}

function toggleMicrophoneMute() {
  if (!state.recording) return;
  state.microphoneMuted = !state.microphoneMuted;
  elements.talkButton.classList.toggle("muted", state.microphoneMuted);
  elements.talkLabel.textContent = state.microphoneMuted
    ? "Unmute microphone"
    : "Mute microphone";
  elements.talkHint.textContent = state.microphoneMuted
    ? "call open / microphone muted"
    : "microphone live / listening";
  logEvent(state.microphoneMuted
    ? "Microphone muted; silent call audio continues"
    : "Microphone unmuted; caller audio is live");
}

function resetTalkButton() {
  elements.talkButton.classList.remove("live");
  elements.talkButton.classList.remove("muted");
  const continuous = selectedTarget()?.interactionMode === "continuous";
  elements.talkLabel.textContent = continuous ? "Open microphone" : "Start talking";
  elements.talkHint.textContent = state.sessionActive
    ? (continuous ? "click to open call audio" : "click to record one turn")
    : "session required";
}

function stopPlayback() {
  state.activeSources.forEach((source) => {
    try { source.stop(); } catch (_) { /* already stopped */ }
  });
  state.activeSources.clear();
  if (state.playbackContext) state.nextPlaybackAt = state.playbackContext.currentTime;
  elements.stopPlaybackButton.disabled = true;
  logEvent("Playback queue stopped");
}

function clearEvidence() {
  stopPlayback();
  state.pcmChunks = [];
  state.wavSampleRate = null;
  state.rawDurationMs = 0;
  state.anomalyCount = 0;
  elements.latencyMetric.textContent = "—";
  elements.durationMetric.textContent = "0.0s";
  elements.anomalyMetric.textContent = "0";
  elements.rmsMetric.textContent = "—";
  elements.metricAlert.classList.remove("has-alert");
  elements.downloadButton.disabled = true;
  elements.transcript.innerHTML = '<p class="empty-state">Recognition and agent text will appear here.</p>';
  elements.eventLog.replaceChildren();
}

function downloadWav() {
  if (!state.pcmChunks.length || !state.wavSampleRate) return;
  const totalBytes = state.pcmChunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  const buffer = new ArrayBuffer(44 + totalBytes);
  const view = new DataView(buffer);
  const writeAscii = (offset, text) => [...text].forEach((character, index) => view.setUint8(offset + index, character.charCodeAt(0)));
  writeAscii(0, "RIFF");
  view.setUint32(4, 36 + totalBytes, true);
  writeAscii(8, "WAVE");
  writeAscii(12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, 1, true);
  view.setUint32(24, state.wavSampleRate, true);
  view.setUint32(28, state.wavSampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  writeAscii(36, "data");
  view.setUint32(40, totalBytes, true);
  const output = new Uint8Array(buffer, 44);
  let offset = 0;
  state.pcmChunks.forEach((chunk) => {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(new Blob([buffer], { type: "audio/wav" }));
  link.download = `voice-agent-direct-${elements.targetSelect.value}-${elements.profileSelect.value}-${new Date().toISOString().replaceAll(":", "-")}.wav`;
  link.click();
  setTimeout(() => URL.revokeObjectURL(link.href), 1000);
}

elements.targetSelect.addEventListener("change", renderTarget);
elements.startButton.addEventListener("click", startSession);
elements.endButton.addEventListener("click", endSession);
elements.talkButton.addEventListener("click", () => {
  if (!state.recording) {
    startTalking();
  } else if (state.continuousMic) {
    toggleMicrophoneMute();
  } else {
    void stopCapture(true);
  }
});
elements.textForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = elements.textInput.value.trim();
  if (!text || !state.sessionActive || state.turnActive) return;
  setTurnActive(true);
  state.socket.send(JSON.stringify({ type: "text", text }));
  addTranscript("TEXT", text);
  elements.textInput.value = "";
});
elements.stopPlaybackButton.addEventListener("click", stopPlayback);
elements.downloadButton.addEventListener("click", downloadWav);
elements.clearButton.addEventListener("click", clearEvidence);

async function initialize() {
  try {
    const response = await fetch("/api/config");
    if (!response.ok) throw new Error(`Config request failed: ${response.status}`);
    state.config = await response.json();
    state.config.targets.forEach((target) => {
      const option = document.createElement("option");
      option.value = target.id;
      option.textContent = target.label;
      option.selected = target.id === state.config.defaultTargetId;
      elements.targetSelect.append(option);
    });
    state.config.profiles.forEach((profile) => {
      const option = document.createElement("option");
      option.value = profile.id;
      option.textContent = `${profile.label} — ${profile.description}`;
      option.selected = profile.id === state.config.defaultProfileId;
      elements.profileSelect.append(option);
    });
    renderTarget();
    lockSetup(false);
    await openSocket();
  } catch (error) {
    setConnection("error", "startup error");
    logEvent(error.message, "error");
  }
}

initialize();
