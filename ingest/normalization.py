# ingest/normalization.py
import re, unicodedata
from typing import Optional

def _strip_accents(text: str) -> str:
    if text is None: return ""
    return "".join(ch for ch in unicodedata.normalize("NFD", text) if unicodedata.category(ch) != "Mn")

def canonical(text: Optional[str]) -> str:
    if not text: return ""
    s = _strip_accents(str(text)).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def slugify(text: Optional[str], sep: str = "-") -> str:
    s = canonical(text)
    s = re.sub(r"[^a-z0-9]+", sep, s)
    s = re.sub(fr"{re.escape(sep)}+", sep, s).strip(sep)
    return s
