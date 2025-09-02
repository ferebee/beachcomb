# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
hashing.py — v0.1

File hashing utilities for the beachcomb tool.
"""
import hashlib
import re
from pathlib import Path
from typing import Optional

from utils import run, which

def fast_hash(data: bytes) -> str:
    return hashlib.blake2b(data, digest_size=16).hexdigest()

def full_hash_openssl(path: Path, algo: str = "sha256") -> Optional[str]:
    if which("openssl"):
        rc, out, err = run(["openssl", "dgst", f"-{algo}", str(path)], timeout=3600)
        if rc == 0:
            m = re.search(r"= ([0-9a-fA-F]+)", out)
            if m:
                return m.group(1).lower()
    return None

def full_hash_b3sum(path: Path) -> Optional[str]:
    if which("b3sum"):
        rc, out, err = run(["b3sum", str(path)], timeout=3600)
        if rc == 0:
            return out.strip().split()[0].lower()
    return None
