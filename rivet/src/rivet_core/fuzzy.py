"""Shared fuzzy matching utilities for Rivet.

Provides a single subsequence-based fuzzy match with scoring, used by
catalog search, completions, and the command palette.
"""

from __future__ import annotations


def fuzzy_match(query: str, candidate: str) -> tuple[float, list[int]] | None:
    """Subsequence fuzzy match with scoring.

    All query chars must appear in candidate in order (case-insensitive).

    Returns ``(score, match_positions)`` or ``None`` if no match.
    Lower score = better match.

    Scoring heuristics:
      - Exact prefix match bonus (strong)
      - Word boundary bonus (after ``_``, ``.``, ``-``, ``/``)
      - Consecutive character bonus
      - Scatter penalty (gaps between matched positions)
      - Length penalty (shorter candidates score better)
    """
    if not query:
        return (0.0, [])

    q = query.lower()
    c = candidate.lower()
    positions: list[int] = []
    ci = 0

    for ch in q:
        found = c.find(ch, ci)
        if found == -1:
            return None
        positions.append(found)
        ci = found + 1

    # Scoring components
    score = 0.0

    # Contiguous substring bonus (strongest): if the query appears as a
    # literal substring anywhere in the candidate, this is almost certainly
    # a relevant match.  Award a large bonus and use the substring position
    # for match_positions so the scatter penalty doesn't apply.
    substr_idx = c.find(q)
    if substr_idx != -1:
        positions = list(range(substr_idx, substr_idx + len(q)))
        score -= 20.0

    # Exact prefix match bonus (strong)
    if c.startswith(q):
        score -= 10.0

    # Word boundary bonus: count matches at word boundaries (after _, ., or start)
    for pos in positions:
        if pos == 0 or candidate[pos - 1] in ("_", ".", "-", "/"):
            score -= 2.0

    # Consecutive bonus: count consecutive position pairs
    for i in range(1, len(positions)):
        if positions[i] == positions[i - 1] + 1:
            score -= 1.5

    # Scatter penalty: total gap between matched positions
    if len(positions) > 1:
        score += (positions[-1] - positions[0] - len(positions) + 1) * 0.5

    # Length penalty: shorter candidates are better
    score += len(candidate) * 0.1

    return (score, positions)
