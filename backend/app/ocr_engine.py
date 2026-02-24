import easyocr
import cv2
import math
import pytesseract
import os

os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")

try:
    from paddleocr import PaddleOCR
except Exception:
    PaddleOCR = None

_reader = None
_paddle_reader = None

def get_reader():
    global _reader
    if _reader is None:
        _reader = easyocr.Reader(['en'], gpu=False)
    return _reader


def get_paddle_reader():
    global _paddle_reader
    if PaddleOCR is None:
        raise RuntimeError("paddleocr_not_installed")
    if _paddle_reader is None:
        _paddle_reader = PaddleOCR(
            lang="en",
            ocr_version="PP-OCRv5",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            text_detection_model_name="PP-OCRv5_mobile_det",
            text_recognition_model_name="PP-OCRv5_mobile_rec",
            text_det_limit_side_len=1024,
        )
    return _paddle_reader


def ocr_image(
    image_path,
    backend="easyocr",
    max_width=850,
    max_pixels=1_800_000,
    chunk_height=620,
    chunk_overlap=60,
):
    if backend == "paddleocr":
        return _ocr_image_paddle(
            image_path=image_path,
            max_width=max_width,
            max_pixels=max_pixels,
        )
    if backend == "tesseract":
        return _ocr_image_tesseract(
            image_path=image_path,
            max_width=max_width,
            max_pixels=max_pixels,
        )
    return _ocr_image_easyocr(
        image_path=image_path,
        max_width=max_width,
        max_pixels=max_pixels,
        chunk_height=chunk_height,
        chunk_overlap=chunk_overlap,
    )


def _ocr_image_easyocr(
    image_path,
    max_width=850,
    max_pixels=1_800_000,
    chunk_height=620,
    chunk_overlap=60,
):
    reader = get_reader()
    img = cv2.imread(image_path)
    if img is None:
        return []

    h, w = img.shape[:2]
    if max(h, w) > max_width:
        scale = max_width / float(max(h, w))
        img = cv2.resize(img, None, fx=scale, fy=scale)
        h, w = img.shape[:2]

    pixels = h * w
    if pixels > max_pixels:
        scale = math.sqrt(max_pixels / float(pixels))
        img = cv2.resize(img, None, fx=scale, fy=scale)

    ocr_items = []
    h, _ = img.shape[:2]
    step = max(1, chunk_height - chunk_overlap)
    idx = 1

    for top in range(0, h, step):
        bottom = min(h, top + chunk_height)
        chunk = img[top:bottom, :]
        if chunk.size == 0:
            continue

        chunk_results = reader.readtext(
            chunk,
            batch_size=1,
            mag_ratio=0.9,
            canvas_size=1024,
            decoder="greedy",
        )

        for (bbox, text, conf) in chunk_results:
            shifted_bbox = [[float(pt[0]), float(pt[1] + top)] for pt in bbox]
            if _is_overlap_duplicate(ocr_items, shifted_bbox, text):
                continue

            ocr_items.append({
                "id": idx,
                "text": text,
                "confidence": float(conf),
                "bbox": shifted_bbox,
            })
            idx += 1

        if bottom >= h:
            break

    return ocr_items


def _ocr_image_tesseract(
    image_path,
    max_width=1000,
    max_pixels=2_000_000,
):
    img = cv2.imread(image_path)
    if img is None:
        return []

    h, w = img.shape[:2]
    if max(h, w) > max_width:
        scale = max_width / float(max(h, w))
        img = cv2.resize(img, None, fx=scale, fy=scale)
        h, w = img.shape[:2]

    pixels = h * w
    if pixels > max_pixels:
        scale = math.sqrt(max_pixels / float(pixels))
        img = cv2.resize(img, None, fx=scale, fy=scale)

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.equalizeHist(gray)

    config = (
        "--oem 1 --psm 11 "
        "-c preserve_interword_spaces=1 "
        "-c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.,/-()$"
    )
    data = pytesseract.image_to_data(
        gray,
        output_type=pytesseract.Output.DICT,
        config=config,
        lang="eng",
    )

    items = []
    n = len(data.get("text", []))
    idx = 1
    for i in range(n):
        text = (data["text"][i] or "").strip()
        if not text:
            continue
        conf_raw = data.get("conf", ["-1"])[i]
        try:
            conf = float(conf_raw)
        except Exception:
            conf = -1.0
        if conf < 0:
            continue

        x = float(data["left"][i])
        y = float(data["top"][i])
        w = float(data["width"][i])
        h = float(data["height"][i])
        if w <= 0 or h <= 0:
            continue

        bbox = [
            [x, y],
            [x + w, y],
            [x + w, y + h],
            [x, y + h],
        ]
        if _is_overlap_duplicate(items, bbox, text):
            continue

        items.append({
            "id": idx,
            "text": text,
            "confidence": conf / 100.0,
            "bbox": bbox,
        })
        idx += 1

    return items


def _ocr_image_paddle(
    image_path,
    max_width=1100,
    max_pixels=2_300_000,
):
    reader = get_paddle_reader()
    img = cv2.imread(image_path)
    if img is None:
        return []

    h, w = img.shape[:2]
    if max(h, w) > max_width:
        scale = max_width / float(max(h, w))
        img = cv2.resize(img, None, fx=scale, fy=scale)
        h, w = img.shape[:2]

    pixels = h * w
    if pixels > max_pixels:
        scale = math.sqrt(max_pixels / float(pixels))
        img = cv2.resize(img, None, fx=scale, fy=scale)
        h, w = img.shape[:2]

    # Paddle can segfault on very thin strips after its own internal resize.
    # Pad narrow dimensions with whitespace to keep aspect ratio bounded.
    orig_h, orig_w = img.shape[:2]
    pad_top = 0
    pad_bottom = 0
    pad_left = 0
    pad_right = 0
    max_ratio = 3.0
    if orig_w / max(float(orig_h), 1.0) > max_ratio:
        target_h = int(math.ceil(orig_w / max_ratio))
        pad_total = max(0, target_h - orig_h)
        pad_top = pad_total // 2
        pad_bottom = pad_total - pad_top
    elif orig_h / max(float(orig_w), 1.0) > max_ratio:
        target_w = int(math.ceil(orig_h / max_ratio))
        pad_total = max(0, target_w - orig_w)
        pad_left = pad_total // 2
        pad_right = pad_total - pad_left

    if pad_top or pad_bottom or pad_left or pad_right:
        img = cv2.copyMakeBorder(
            img,
            pad_top,
            pad_bottom,
            pad_left,
            pad_right,
            cv2.BORDER_CONSTANT,
            value=(255, 255, 255),
        )

    results = reader.ocr(img) or []
    items = []
    idx = 1
    for page in results:
        if not page:
            continue
        # PaddleOCR v3 returns per-page dicts with rec_texts/rec_polys arrays.
        if isinstance(page, dict):
            texts = page.get("rec_texts") or []
            scores = page.get("rec_scores") or []
            polys = page.get("rec_polys") or page.get("dt_polys") or []
            for i, text_raw in enumerate(texts):
                text = (text_raw or "").strip()
                if not text:
                    continue
                bbox = polys[i] if i < len(polys) else None
                if bbox is None:
                    continue
                conf = float(scores[i]) if i < len(scores) else 0.0
                norm_bbox = []
                for pt in bbox:
                    x = float(pt[0]) - float(pad_left)
                    y = float(pt[1]) - float(pad_top)
                    x = max(0.0, min(float(orig_w), x))
                    y = max(0.0, min(float(orig_h), y))
                    norm_bbox.append([x, y])
                if len(norm_bbox) != 4 or _is_overlap_duplicate(items, norm_bbox, text):
                    continue
                items.append({
                    "id": idx,
                    "text": text,
                    "confidence": conf,
                    "bbox": norm_bbox,
                })
                idx += 1
            continue

        # Legacy tuple/list format.
        for entry in page:
            if not entry or len(entry) < 2:
                continue
            bbox = entry[0]
            rec = entry[1]
            if not bbox or not rec:
                continue
            text = (rec[0] or "").strip()
            conf = float(rec[1]) if len(rec) > 1 else 0.0
            if not text:
                continue
            norm_bbox = []
            for pt in bbox:
                x = float(pt[0]) - float(pad_left)
                y = float(pt[1]) - float(pad_top)
                x = max(0.0, min(float(orig_w), x))
                y = max(0.0, min(float(orig_h), y))
                norm_bbox.append([x, y])
            if _is_overlap_duplicate(items, norm_bbox, text):
                continue
            items.append({
                "id": idx,
                "text": text,
                "confidence": conf,
                "bbox": norm_bbox,
            })
            idx += 1
    return items


def _is_overlap_duplicate(existing_items, bbox, text):
    if not existing_items:
        return False

    cx = sum(pt[0] for pt in bbox) / 4.0
    cy = sum(pt[1] for pt in bbox) / 4.0
    normalized_text = (text or "").strip().lower()

    for item in existing_items[-120:]:
        item_bbox = item.get("bbox") or []
        if len(item_bbox) != 4:
            continue

        item_cx = sum(pt[0] for pt in item_bbox) / 4.0
        item_cy = sum(pt[1] for pt in item_bbox) / 4.0
        if abs(item_cy - cy) <= 18 and abs(item_cx - cx) <= 30:
            if (item.get("text") or "").strip().lower() == normalized_text:
                return True

    return False
