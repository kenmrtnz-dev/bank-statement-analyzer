import cv2

def clean_page(image_path):
    img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)

    # 1️⃣ Denoise
    img = cv2.GaussianBlur(img, (3, 3), 0)

    # 2️⃣ Adaptive threshold (great for passbooks)
    thresh = cv2.adaptiveThreshold(
        img,
        255,
        cv2.ADAPTIVE_THRESH_MEAN_C,
        cv2.THRESH_BINARY,
        31,
        15
    )

    # 3️⃣ Morphological opening (remove noise)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    cleaned = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel)

    return cleaned
