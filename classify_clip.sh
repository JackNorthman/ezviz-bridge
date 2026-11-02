#!/usr/bin/env bash
# Classify every frame of a video clip with the bridge's YOLO model.
# Usage: ./classify_clip.sh <clip.mp4> [fps=2] [conf=0.35]
# Typical source: a clip downloaded with the EZVIZ app or EZVIZ Studio. Needs ffmpeg on the host
# (frame extraction of a long clip can use a lot of RAM: keep clips short on small machines).
set -euo pipefail
cd "$(dirname "$0")"
CLIP="$(readlink -f "$1")"; FPS="${2:-2}"; CONF="${3:-0.35}"
NAME="$(basename "${CLIP%.*}")"
WORK="$PWD/data/clips/$NAME"; FR="$WORK/frames"; OUT="$WORK/out"
mkdir -p "$FR" "$OUT"; rm -f "$FR"/f_*.jpg
echo "== extracting frames at ${FPS} fps from $CLIP"
ffmpeg -hide_banner -loglevel error -y -i "$CLIP" -vf "fps=$FPS" -q:v 3 "$FR/f_%06d.jpg"
echo "== $(ls "$FR" | wc -l) frames → detector"
docker run --rm -v "$FR:/frames:ro" -v "$OUT:/out" -v "$PWD/models:/models:ro" \
  ezviz-bridge:local python /app/clip_frames.py /frames /out "$FPS" "$CONF"
echo "== annotated best frames in $OUT"
