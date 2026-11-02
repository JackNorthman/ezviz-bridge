"""Tiny YOLO11 (ONNX, CPU) object detector for alarm snapshots.

Model: yolo11n exported with `ultralytics` (imgsz=640, opset 12, static shape).
Output tensor is (1, 84, 8400): 4 box coords (cx, cy, w, h in letterboxed pixels)
followed by 80 COCO class scores, no separate objectness.
"""
from __future__ import annotations

import numpy as np
import onnxruntime as ort
from PIL import Image, ImageDraw

COCO = (
    "person,bicycle,car,motorcycle,airplane,bus,train,truck,boat,traffic light,"
    "fire hydrant,stop sign,parking meter,bench,bird,cat,dog,horse,sheep,cow,"
    "elephant,bear,zebra,giraffe,backpack,umbrella,handbag,tie,suitcase,frisbee,"
    "skis,snowboard,sports ball,kite,baseball bat,baseball glove,skateboard,"
    "surfboard,tennis racket,bottle,wine glass,cup,fork,knife,spoon,bowl,banana,"
    "apple,sandwich,orange,broccoli,carrot,hot dog,pizza,donut,cake,chair,couch,"
    "potted plant,bed,dining table,toilet,tv,laptop,mouse,remote,keyboard,"
    "cell phone,microwave,oven,toaster,sink,refrigerator,book,clock,vase,"
    "scissors,teddy bear,hair drier,toothbrush"
).split(",")
assert len(COCO) == 80


def _iou(box: np.ndarray, boxes: np.ndarray) -> np.ndarray:
    x1 = np.maximum(box[0], boxes[:, 0])
    y1 = np.maximum(box[1], boxes[:, 1])
    x2 = np.minimum(box[2], boxes[:, 2])
    y2 = np.minimum(box[3], boxes[:, 3])
    inter = np.clip(x2 - x1, 0, None) * np.clip(y2 - y1, 0, None)
    a = (box[2] - box[0]) * (box[3] - box[1])
    b = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
    return inter / np.maximum(a + b - inter, 1e-9)


class Detector:
    def __init__(self, model_path: str, conf: float = 0.4, iou: float = 0.5,
                 size: int = 640, threads: int = 4) -> None:
        so = ort.SessionOptions()
        so.intra_op_num_threads = threads
        so.log_severity_level = 3
        self.sess = ort.InferenceSession(model_path, so, providers=["CPUExecutionProvider"])
        self.inp = self.sess.get_inputs()[0].name
        self.conf, self.iou, self.size = conf, iou, size

    def _letterbox(self, im: Image.Image):
        w, h = im.size
        r = min(self.size / w, self.size / h)
        nw, nh = max(1, round(w * r)), max(1, round(h * r))
        canvas = Image.new("RGB", (self.size, self.size), (114, 114, 114))
        dx, dy = (self.size - nw) // 2, (self.size - nh) // 2
        canvas.paste(im.resize((nw, nh), Image.BILINEAR), (dx, dy))
        arr = (np.asarray(canvas, dtype=np.float32) / 255.0).transpose(2, 0, 1)[None]
        return np.ascontiguousarray(arr), r, dx, dy

    def detect(self, im: Image.Image) -> list[dict]:
        """Return [{label, conf, box:[x1,y1,x2,y2]}] in original-image pixels, best first."""
        im = im.convert("RGB")
        arr, r, dx, dy = self._letterbox(im)
        out = self.sess.run(None, {self.inp: arr})[0]
        pred = out[0]
        if pred.shape[0] == 84:          # (84, 8400) -> (8400, 84)
            pred = pred.T
        boxes, scores = pred[:, :4], pred[:, 4:]
        cls = scores.argmax(1)
        conf = scores[np.arange(len(cls)), cls]
        keep = conf >= self.conf
        boxes, cls, conf = boxes[keep], cls[keep], conf[keep]
        if not len(boxes):
            return []
        xyxy = np.stack([
            (boxes[:, 0] - boxes[:, 2] / 2 - dx) / r,
            (boxes[:, 1] - boxes[:, 3] / 2 - dy) / r,
            (boxes[:, 0] + boxes[:, 2] / 2 - dx) / r,
            (boxes[:, 1] + boxes[:, 3] / 2 - dy) / r,
        ], 1)
        W, H = im.size
        xyxy[:, [0, 2]] = xyxy[:, [0, 2]].clip(0, W)
        xyxy[:, [1, 3]] = xyxy[:, [1, 3]].clip(0, H)
        dets: list[dict] = []
        for c in np.unique(cls):
            idx = np.where(cls == c)[0]
            order = idx[np.argsort(-conf[idx])]
            while order.size:
                i = order[0]
                dets.append({"label": COCO[int(c)], "conf": round(float(conf[i]), 3),
                             "box": [int(v) for v in xyxy[i]]})
                rest = order[1:]
                if not rest.size:
                    break
                order = rest[_iou(xyxy[i], xyxy[rest]) < self.iou]
        dets.sort(key=lambda d: -d["conf"])
        return dets


def annotate(im: Image.Image, dets: list[dict]) -> Image.Image:
    im = im.convert("RGB").copy()
    dr = ImageDraw.Draw(im)
    for d in dets:
        x1, y1, x2, y2 = d["box"]
        colour = {"person": (255, 80, 80), "cat": (80, 220, 120), "dog": (80, 160, 255)}.get(d["label"], (255, 200, 0))
        dr.rectangle([x1, y1, x2, y2], outline=colour, width=3)
        text = f'{d["label"]} {d["conf"]:.2f}'
        tw = 7 * len(text) + 6
        dr.rectangle([x1, max(0, y1 - 14), x1 + tw, y1], fill=colour)
        dr.text((x1 + 3, max(0, y1 - 13)), text, fill=(0, 0, 0))
    return im
