# Kafka Connect Image Optimization

The baseline and optimized images were built on the same Docker engine with `--no-cache`.

| Metric | Value |
|---|---:|
| Baseline size | 1549476393 bytes (1477.7 MiB) |
| Optimized size | 1540697383 bytes (1469.32 MiB) |
| Reduction | 8779010 bytes (8.37 MiB) |
| Reduction percentage | 0.57% |

The optimized image installs the S3 connector in a builder stage and copies only the plugin directory into the pinned Kafka Connect runtime stage.

See `kafka_connect_plugin_smoke.json` and `kafka_connect_bronze_sink_response.json` for runtime proof.
