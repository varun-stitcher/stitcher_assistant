"""File/system dev tools. Register on the mcp server."""

from __future__ import annotations

import datetime
import os
import pathlib
import platform

from fastmcp import FastMCP


def register(mcp: FastMCP) -> None:
    @mcp.tool
    def ping() -> str:
        """Liveness check for the tool bridge."""
        return "pong"

    @mcp.tool
    def now_utc() -> str:
        """Current UTC timestamp (ISO-8601)."""
        return datetime.datetime.now(datetime.UTC).isoformat()

    @mcp.tool
    def system_info() -> dict:
        """Non-secret host info: platform, python version, cwd, user."""
        return {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cwd": str(pathlib.Path.cwd()),
            "user": os.environ.get("USER", ""),
        }

    @mcp.tool
    def list_directory(path: str = ".") -> list[str]:
        """List the top-level entries of a directory (names only)."""
        try:
            return sorted(p.name for p in pathlib.Path(path).iterdir())
        except Exception as e:  # noqa: BLE001
            return [f"ERR: {e}"]

    @mcp.tool
    def read_text_file(path: str, max_chars: int = 4000) -> str:
        """Read a UTF-8 text file, truncated to max_chars."""
        p = pathlib.Path(path)
        if not p.is_file():
            return f"ERR: no such file: {path}"
        try:
            return p.read_text(encoding="utf-8")[:max_chars]
        except Exception as e:  # noqa: BLE001
            return f"ERR: {e}"

    @mcp.tool
    def read_pdf(path: str, max_chars: int = 6000) -> dict:
        """Extract the text content of a PDF file (all pages, layout-preserving).

        Useful to inspect an invoice before running ``normalize_to_focus`` on it.
        Returns per-page extracted text (truncated to ``max_chars`` total) plus
        the absolute path and page count.
        """
        import fitz  # PyMuPDF, available in this venv

        p = pathlib.Path(path)
        if not p.is_file():
            return {"ok": False, "error": f"no such file: {path}"}
        try:
            doc = fitz.open(p)
            pages = []
            chars = 0
            for i, page in enumerate(doc):
                text = page.get_text("text")
                if chars + len(text) > max_chars:
                    text = text[: max(0, max_chars - chars)]
                pages.append({"page": i + 1, "text": text})
                chars += len(text)
                if chars >= max_chars:
                    break
            return {
                "ok": True,
                "path": str(p.resolve()),
                "pages": len(doc),
                "extracted_text": pages,
                "total_chars": chars,
            }
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}
