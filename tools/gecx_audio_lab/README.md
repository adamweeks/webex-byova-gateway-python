# GECX Audio Lab compatibility entry point

The lab now supports both GECX and AWS Lex and is documented as the
[Voice Agent Audio Lab](../voice_agent_lab/README.md).

The earlier command remains compatible:

```bash
python -m tools.gecx_audio_lab --gateway-config config/config.yaml
```

Prefer the provider-neutral entry point for new usage:

```bash
python -m tools.voice_agent_lab --gateway-config config/config.yaml
```

Existing gitignored GECX overlay files remain accepted with `--config`.
