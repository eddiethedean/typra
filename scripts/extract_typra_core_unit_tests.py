#!/usr/bin/env python3
"""Extract #[cfg(test)] mod blocks from typra-core sources into tests/unit/*.rs for include!."""
from __future__ import annotations

import re
from pathlib import Path


def skip_rust_string_or_raw(text: str, i: int) -> int | None:
    """If position i starts a string or raw string, return index after its closing quote."""
    if i >= len(text):
        return None
    # Raw string: r###" ... "###
    if text[i] == "r":
        j = i + 1
        hashes = 0
        while j < len(text) and text[j] == "#":
            hashes += 1
            j += 1
        if j < len(text) and text[j] == '"':
            j += 1
            end_pat = '"' + ("#" * hashes)
            k = text.find(end_pat, j)
            if k == -1:
                return None
            return k + len(end_pat)
    # Normal string
    if text[i] == '"':
        j = i + 1
        esc = False
        while j < len(text):
            c = text[j]
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                return j + 1
            j += 1
        return None
    return None


def ident_continue(c: str) -> bool:
    return c.isalnum() or c == "_"


def skip_apostrophe_region(text: str, i: int) -> int | None:
    """Skip lifetime ('a, 'static) or char literal ('x', '\\n')."""
    if i >= len(text) or text[i] != "'":
        return None
    j = i + 1
    if j >= len(text):
        return None
    if text[j] == "\\":
        j += 1
        if j < len(text) and text[j] == "x":
            j += 1
            while j < len(text) and text[j] in "0123456789abcdefABCDEF":
                j += 1
            return j + 1 if j < len(text) and text[j] == "'" else None
        j += 1
        return j + 1 if j < len(text) and text[j] == "'" else None
    if text[j].isalpha() or text[j] == "_":
        start_ident = j
        while j < len(text) and ident_continue(text[j]):
            j += 1
        # Char literal `'x'` has closing quote immediately after one codepoint (ASCII: one byte).
        if j < len(text) and text[j] == "'" and (j - start_ident) == 1:
            return j + 1
        # Lifetime / label
        return j
    # Unicode char literal — scan to next '
    while j < len(text) and text[j] != "'":
        j += 1
    return j + 1 if j < len(text) else None


def extract_test_modules(
    src: str,
) -> list[tuple[str, str, int, int]]:
    """Each tuple: (mod_name, inner_body, cfg_start_idx, end_after_brace_exclusive)."""
    results: list[tuple[str, str, int, int]] = []
    i = 0
    while True:
        idx = src.find("#[cfg(test)]", i)
        if idx == -1:
            break
        m = re.match(r"#\[cfg\(test\)\]\s*mod\s+(\w+)\s*\{", src[idx:])
        if not m:
            i = idx + 1
            continue
        mod_name = m.group(1)
        brace_open = idx + m.end() - 1
        depth = 0
        j = brace_open
        len_src = len(src)
        while j < len_src:
            c = src[j]
            # comments (only outside strings — we never enter comments from inside strings here)
            if c == "/" and j + 1 < len_src:
                if src[j + 1] == "/":
                    j += 2
                    while j < len_src and src[j] != "\n":
                        j += 1
                    continue
                if src[j + 1] == "*":
                    j += 2
                    while j + 1 < len_src and not (src[j] == "*" and src[j + 1] == "/"):
                        j += 1
                    j += 2 if j + 1 < len_src else 1
                    continue
            if j + 1 < len_src and src[j] == "b" and src[j + 1] == '"':
                ns = skip_rust_string_or_raw(src, j + 1)
                if ns is not None:
                    j = ns
                    continue
            if j + 2 < len_src and src[j : j + 2] == 'br':
                ns = skip_rust_string_or_raw(src, j + 1)
                if ns is not None:
                    j = ns
                    continue
            ns = skip_rust_string_or_raw(src, j)
            if ns is not None:
                j = ns
                continue
            ap = skip_apostrophe_region(src, j)
            if ap is not None:
                j = ap
                continue
            if c == "{":
                depth += 1
                j += 1
                continue
            if c == "}":
                depth -= 1
                j += 1
                if depth == 0:
                    inner = src[brace_open + 1 : j - 1]
                    results.append((mod_name, inner, idx, j))
                    i = j
                    break
                continue
            j += 1
        else:
            raise RuntimeError(f"Unterminated cfg(test) mod {mod_name}")
    return results


def unit_filename(src_rel: Path, mod_name: str) -> str:
    """src_rel is relative to typra-core/, e.g. Path('src/checkpoint.rs')."""
    stem = str(src_rel.with_suffix("")).replace("/", "_").replace("\\", "_")
    if mod_name == "tests":
        return f"{stem}_tests.rs"
    return f"{stem}_{mod_name}.rs"


def stub_for(rel: Path, mod_name: str) -> str:
    name = unit_filename(rel, mod_name)
    return (
        f"#[cfg(test)]\nmod {mod_name} {{\n    include!(concat!(\n        env!(\"CARGO_MANIFEST_DIR\"),\n        \"/tests/unit/{name}\"\n    ));\n}}\n"
    )


def main() -> None:
    core = Path("crates/typra-core")
    src_root = core / "src"
    unit_dir = core / "tests" / "unit"
    unit_dir.mkdir(parents=True, exist_ok=True)

    for path in sorted(src_root.rglob("*.rs")):
        text = path.read_text()
        try:
            modules = extract_test_modules(text)
        except RuntimeError as e:
            raise RuntimeError(f"{path}: {e}") from e
        if not modules:
            continue
        rel = path.relative_to(core)
        for mod_name, body, _, _ in modules:
            out = unit_dir / unit_filename(rel, mod_name)
            out.write_text(body.lstrip("\n"))
            print(f"Wrote {out} ({len(body)} bytes) from {rel} mod {mod_name}")

        # Replace cfg(test) blocks with include stubs (last span first).
        new_text = text
        for mod_name, _body, cfg_idx, end_j in sorted(
            modules, key=lambda t: t[2], reverse=True
        ):
            replacement = stub_for(rel, mod_name)
            new_text = new_text[:cfg_idx] + replacement + new_text[end_j:]
        path.write_text(new_text)
        print(f"Rewrote {rel}")


if __name__ == "__main__":
    main()
