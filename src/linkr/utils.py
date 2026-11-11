from __future__ import annotations

import re


def topic_to_regex(pattern: str) -> re.Pattern[str]:
    """
    Convert an event topic pattern into a regular expression.

    ``*`` matches exactly one segment, ``#`` matches zero or more segments.
    """
    parts = pattern.split(".")

    # Consecutive # are semantically equivalent to a single #.
    normalized: list[str] = []
    for part in parts:
        if part == "#" and normalized and normalized[-1] == "#":
            continue
        normalized.append(part)

    parts = normalized

    # # alone matches zero or more words.
    if parts == ["#"]:
        return re.compile(r"(?:[^.]+(?:\.[^.]+)*)?")

    regex = ["^"]

    for i, part in enumerate(parts):
        if part == "#":
            if i == 0:
                # # at the beginning: zero or more "word." prefixes.
                regex.append(r"(?:[^.]+\.)*")
            else:
                # # in the middle or at the end: zero or more ".word" suffixes.
                regex.append(r"(?:\.[^.]+)*")
        else:
            # Normally every non-first segment needs a dot.
            #
            # Exception: #.foo. The leading # already consumes the dots after
            # the words it consumes, so "foo" must be placed directly after it.
            if i > 0 and not (parts[i - 1] == "#" and i - 1 == 0):
                regex.append(r"\.")

            if part == "*":
                regex.append(r"[^.]+")
            else:
                regex.append(re.escape(part))

    regex.append("$")

    return re.compile("".join(regex))
