#!/usr/bin/env python3
"""Run the detector over a directory of extracted video frames.

Usage (inside the bridge image): python /app/clip_frames.py <frames_dir> <out_dir> [fps] [conf]
Frames are expected as <frames_dir>/f_000001.jpg … (ffmpeg -vf fps=N). Prints a timeline of
what was seen per frame, a per-label summary, and saves the best annotated frame per label.
"""
import json, os, sys, pathlib
from PIL import Image
from detector import Detector, annotate

frames = pathlib.Path(sys.argv[1]); out = pathlib.Path(sys.argv[2]); out.mkdir(parents=True, exist_ok=True)
fps = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0
conf = float(sys.argv[4]) if len(sys.argv) > 4 else 0.35
det = Detector(os.environ.get("MODEL_PATH", "/models/yolo11n.onnx"), conf=conf)
files = sorted(frames.glob("f_*.jpg"))
best: dict[str, tuple[float, int, list]] = {}
timeline = []
for i, f in enumerate(files):
    im = Image.open(f)
    dets = det.detect(im)
    t = i / fps
    labels = {d["label"]: d["conf"] for d in dets}
    timeline.append((t, labels))
    for d in dets:
        if d["conf"] > best.get(d["label"], (0, 0, []))[0]:
            best[d["label"]] = (d["conf"], i, dets)
    interesting = {k: v for k, v in labels.items() if k in ("person", "cat", "dog", "bird")}
    if interesting:
        print(f"  t={t:6.1f}s  " + "  ".join(f"{k}:{v:.2f}" for k, v in sorted(interesting.items(), key=lambda kv: -kv[1])))
print(f"\n  {len(files)} frames at {fps:g} fps ({len(files)/fps:.1f} s), conf>={conf}")
if not best:
    print("  nothing detected in any frame"); sys.exit(0)
for label, (c, i, dets) in sorted(best.items(), key=lambda kv: -kv[1][0]):
    seen = [t for t, l in timeline if label in l]
    fn = out / f"best_{label}_{c:.2f}.jpg"
    annotate(Image.open(files[i]), dets).save(fn, "JPEG", quality=88)
    print(f"  {label:10s} best {c:.2f} at t={i/fps:.1f}s, seen in {len(seen)} frame(s) {seen[0]:.1f}–{seen[-1]:.1f}s  -> {fn.name}")
(out / "timeline.json").write_text(json.dumps([{"t": t, "labels": l} for t, l in timeline], indent=1))
