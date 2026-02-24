# Modules

Active feature modules:
- `jobs/`: API routes and filesystem-oriented job lifecycle (`upload -> parse -> summary/export`).
- `ocr/`: PDF parsing pipeline (`text` and `ocr`), image cleaning, OCR task runner.

Design rules:
- Keep endpoints in `*/router.py`.
- Keep business logic in `*/service.py`.
- Keep persistence/file I/O in `*/repository.py`.
- Keep OCR-specific logic inside `ocr/*`.

The previous multi-role workflow modules are now legacy and not part of the active request flow.
