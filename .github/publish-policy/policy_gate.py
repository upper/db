#!/usr/bin/env python3
"""Base-context publish-policy gate: changed-files-vs-policy check (ci-cd#148).

This is the code the ``pull_request_target`` workflow
(``templates/github-publish-policy-gate.yml``) runs **from the base branch
context**. It decides whether a pull request may merge into the public
snapshot repo *without ever checking out or executing PR-head content*:

* the policy (``publish-policy.json``) and this code both come from the **base**
  checkout — never the PR head — so a PR cannot rewrite the gate that judges it;
* the PR is consumed strictly as **data**: the list of changed file paths,
  obtained from the GitHub API (``gh api .../pulls/<n>/files``), never by
  fetching or running head content.

A PR is **denied** when it touches:

* a **blocked path** (matches a ``blocked_paths`` glob from the base policy) —
  blocked for everyone; or
* the **policy/gate machinery** (``publish-policy.json`` itself or anything
  under ``.github/workflows/``) — blocked for everyone **except** the
  authorized sync bot's own snapshot PRs.

The **authorized sync-bot update path**: gate/policy changes are legitimate only
when the PR is authored by the sync-bot account and its attempt branch is bound
to the PR base. New attempts use ``sync/<base>/<hash>``; the legacy
``sync/<hash>`` form is accepted only on the repository's verified default
branch so the one-time policy upgrade cannot deadlock. The
gate itself being a **required status check under a branch ruleset** (see
``policy_gate_ruleset.sh``) means renaming/removing the workflow in a PR cannot
substitute a passing check — the required check simply never reports.

Stdlib only (no third-party imports), so the single file can be shipped into a
public repo and run under the base checkout. The decision core
(:func:`evaluate`) is a pure function — (base policy + changed-files list +
PR author/branch) in, allow/deny out — exercised exhaustively by the tests; the
CLI is the thin wrapper the workflow invokes.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional

# The shipped policy file (repo-root-relative). Changing it is a policy change.
POLICY_FILENAME = "publish-policy.json"

# Any PR change under these prefixes is gate/policy machinery (bot-only). The
# gate's OWN base-side executable lives under GATE_DIR_PREFIX — the workflow runs
# ``<GATE_DIR_PREFIX>policy_gate.py`` — so a non-bot edit there would let an
# attacker-controlled copy become the trusted code future pull_request_target
# runs execute. GATE_SCRIPT_PATH is the single source of truth kept in lockstep
# with the workflow's run: line.
WORKFLOWS_PREFIX = ".github/workflows/"
GATE_DIR_PREFIX = ".github/publish-policy/"
GATE_SCRIPT_PATH = GATE_DIR_PREFIX + "policy_gate.py"

# Attempt refs are either the legacy default-only ``sync/<hash>`` form or the
# base-bound ``sync/<public-branch>/<hash>`` form. The final slash separates the
# opaque tree hash, so public branch names may themselves contain slashes.
LEGACY_SYNC_BRANCH_RE = re.compile(r"^sync/[0-9a-f]{7,64}$")
MAPPED_SYNC_BRANCH_RE = re.compile(r"^sync/(?P<base>.+)/(?P<key>[0-9a-f]{7,64})$")

# Exit codes for the CLI (the workflow branches on these; the required check
# reports failure to the ruleset on non-zero).
EXIT_ALLOW = 0
EXIT_DENY = 1
EXIT_INFRA = 2

# The only policy schema version this gate understands. A policy declaring any
# other version is rejected (fail closed) rather than interpreted optimistically.
EXPECTED_SCHEMA_VERSION = 2

# A well-formed source hash: ``sha256:`` + 64 lowercase hex. The gate rejects a
# policy whose hash does not match this shape (fail closed on a malformed hash).
_SOURCE_HASH_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PUBLIC_BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")


class UnsupportedPattern(ValueError):
    """A ``.gitattributes`` pattern outside the enforceable subset.

    Raised by :func:`validate_pattern` / :func:`_translate_glob`. Callers **fail
    closed** on it — the generator refuses to emit an unenforceable glob, and the
    gate refuses to load a policy containing one — rather than shipping a
    blocked-path rule the matcher cannot faithfully enforce.
    """


# ---------------------------------------------------------------------------
# Path matching — a BOUNDED, real-git-verified .gitattributes glob subset.
#
# Every ADMITTED pattern shape is provably equivalent to git's `git archive`
# behavior (a real-git equivalence test per shape); everything else FAILS
# GENERATION CLOSED (:class:`UnsupportedPattern`). This closes the grammar
# surface so no future "pattern X slips through the gate" can exist.
#
# Admitted subset:
#   * literal path segments;
#   * ``*`` (within a segment) and ``**`` (spans separators);
#   * ``?`` (one char within a segment);
#   * a simple bracket set ``[abc]`` / ``[a-z]`` with ``!``/``^`` negation and a
#     literal ``]`` first member — but NOT POSIX classes, nested brackets, or
#     escapes;
#   * a leading ``/`` anchor and a trailing ``/`` directory marker.
#
# Rejected (fail closed): POSIX classes ``[[:...:]]`` / collating / equivalence
# classes, any bracket we cannot fully evaluate, backslash escapes (git
# wildmatch's ``\`` beyond the C-quote decoding the generator already did), and
# negation ``!``.
#
# Incoming CHANGED-FILE names are the API's canonical git paths and are matched
# byte-for-byte — NEVER host-path-normalized: a backslash is valid filename
# DATA, not a separator, and edge whitespace is significant. Only the PATTERN
# carries git's slash semantics.
# ---------------------------------------------------------------------------
def path_matches(path: str, pattern: str) -> bool:
    """Does the exact repo-relative git ``path`` match ``.gitattributes``-style
    ``pattern``? ``path`` is used byte-for-byte (no normalization); the pattern
    grammar is the bounded subset above. Unsupported syntax raises
    :class:`UnsupportedPattern` (callers fail closed)."""

    if not path or not pattern:
        return False

    core = pattern.strip("/")
    if not core:
        return False
    # git treats a pattern with any slash (leading or internal) as anchored to
    # the repo root; a slashless pattern matches a segment at any depth.
    anchored = pattern.startswith("/") or ("/" in core)

    if not anchored:
        # Slashless: match any single path segment. A matched segment also blocks
        # everything beneath it, since every descendant path contains it.
        seg_re = _compile_glob(core, subpath=False)
        return any(seg_re.match(seg) for seg in path.split("/"))

    # Anchored: matches the path itself OR any subpath under it (directory rule).
    return bool(_compile_glob(core, subpath=True).match(path))


def _compile_glob(core: str, subpath: bool) -> "re.Pattern[str]":
    """Compile a glob ``core`` (surrounding ``/`` already stripped) to a regex.

    ``subpath=True`` appends ``(?:/.*)?`` so a directory pattern also matches
    files underneath it. DOTALL so ``.`` spans an embedded newline (a blocked
    filename containing ``\\n`` must not evade the match). Raises
    :class:`UnsupportedPattern` on any out-of-subset shape."""

    body = _translate_glob(core)
    suffix = r"(?:/.*)?" if subpath else ""
    try:
        return re.compile("^" + body + suffix + "$", re.DOTALL)
    except re.error as exc:  # pragma: no cover - _translate_glob shields this
        raise UnsupportedPattern(f"uncompilable pattern {core!r}: {exc}") from exc


def _translate_glob(pat: str) -> str:
    out: list[str] = []
    i = 0
    n = len(pat)
    while i < n:
        c = pat[i]
        if c == "\\":
            # git wildmatch uses \ to escape the next char; we do not implement
            # it (the generator already C-quote-decoded), so refuse rather than
            # guess. (A literal-backslash filename is still matchable via `?`.)
            raise UnsupportedPattern(
                f"backslash escapes are not supported in pattern: {pat!r}"
            )
        if c == "*":
            if i + 1 < n and pat[i + 1] == "*":
                out.append(".*")  # ** spans separators
                i += 2
                if i < n and pat[i] == "/":  # swallow the slash in a/**/b
                    i += 1
                continue
            out.append("[^/]*")
            i += 1
            continue
        if c == "?":
            out.append("[^/]")
            i += 1
            continue
        if c == "[":
            token, i = _read_bracket(pat, i)
            out.append(token)
            continue
        out.append(re.escape(c))
        i += 1
    return "".join(out)


def _read_bracket(pat: str, i: int) -> "tuple[str, int]":
    """Read a simple ``[...]`` set starting at ``pat[i]``; return (regex,
    next-index).

    Admits a leading ``!``/``^`` negation and a literal ``]`` first member.
    **Rejects** (``UnsupportedPattern``) any inner ``[`` or ``\\`` — that flags a
    POSIX class ``[[:...:]]``, a collating/equivalence class, a nested bracket,
    or an escape, none of which this bounded subset evaluates. So a pattern like
    ``file[[:digit:]].txt`` fails generation closed instead of under-matching."""

    j = i + 1
    neg = False
    if j < len(pat) and pat[j] in "!^":
        neg = True
        j += 1
    if j < len(pat) and pat[j] == "]":  # literal ] as first member
        j += 1
    while j < len(pat) and pat[j] != "]":
        if pat[j] in "[\\":
            raise UnsupportedPattern(
                f"unsupported bracket construct (POSIX class / nested / escape) "
                f"in pattern: {pat!r}"
            )
        j += 1
    if j >= len(pat):
        raise UnsupportedPattern(f"unmatched '[' in pattern: {pat!r}")
    inner = pat[i + 1 + (1 if neg else 0): j]
    if not inner:
        raise UnsupportedPattern(f"empty bracket class in pattern: {pat!r}")
    return "[" + ("^" if neg else "") + inner + "]", j + 1


def validate_pattern(pattern: str) -> None:
    """Raise :class:`UnsupportedPattern` if ``pattern`` is outside the bounded
    subset — so the generator fails closed instead of emitting an unenforceable
    glob, and the gate fails closed instead of loading one.

    Rejected: empty patterns, negation (``!`` un-ignore), and (via the shared
    :func:`_translate_glob`) POSIX/nested brackets, backslash escapes, and
    unmatched brackets."""

    if not pattern:
        raise UnsupportedPattern("empty pattern")
    if pattern.lstrip("/").startswith("!"):
        raise UnsupportedPattern(f"negation is not supported: {pattern!r}")
    core = pattern.strip("/")
    if not core:
        raise UnsupportedPattern(f"pattern has no path component: {pattern!r}")
    _translate_glob(core)  # the single grammar gate: raises on any out-of-subset shape


def matches_any(path: str, patterns: Iterable[str]) -> Optional[str]:
    """Return the first pattern that matches ``path``, or ``None``."""

    for pat in patterns:
        if path_matches(path, pat):
            return pat
    return None


# ---------------------------------------------------------------------------
# Policy loading (strict / fail-closed)
# ---------------------------------------------------------------------------
def load_policy(path: str) -> dict:
    """Load and **strictly validate** ``publish-policy.json`` from the base
    checkout. Any structural problem is a fail-closed error (never a silent
    allow): unknown ``schema_version``, a missing/non-list ``blocked_paths``, a
    blocked-path glob outside the enforceable subset, or a ``source_hash`` that
    is not ``sha256:<64 hex>``."""

    with open(path, "r", encoding="utf-8") as handle:
        policy = json.load(handle)
    if not isinstance(policy, dict):
        raise ValueError("publish-policy.json must be a JSON object")

    version = policy.get("schema_version")
    if version != EXPECTED_SCHEMA_VERSION:
        raise ValueError(
            f"publish-policy.json schema_version {version!r} is not the expected "
            f"{EXPECTED_SCHEMA_VERSION}"
        )

    bp = policy.get("blocked_paths")
    if not isinstance(bp, list) or not all(isinstance(p, str) for p in bp):
        raise ValueError("publish-policy.json 'blocked_paths' must be a list of strings")
    for pat in bp:
        validate_pattern(pat)  # UnsupportedPattern (a ValueError) -> fail closed

    branches = policy.get("published_branches")
    if (not isinstance(branches, list) or not branches
            or not all(isinstance(branch, str) and branch for branch in branches)
            or branches != sorted(set(branches))):
        raise ValueError(
            "publish-policy.json 'published_branches' must be a non-empty, "
            "sorted list of unique strings"
        )
    for branch in branches:
        if (not _PUBLIC_BRANCH_RE.fullmatch(branch) or '..' in branch
                or branch.endswith(('/', '.')) or '//' in branch
                or '@{' in branch or branch == '@'
                or any(component.startswith('.') or component.endswith('.lock')
                       for component in branch.split('/'))):
            raise ValueError(
                f"publish-policy.json contains unsafe published branch {branch!r}"
            )

    sh = policy.get("source_hash")
    if not isinstance(sh, str) or not _SOURCE_HASH_RE.match(sh):
        raise ValueError("publish-policy.json 'source_hash' must be 'sha256:<64 hex>'")

    return policy


def blocked_paths(policy: dict) -> list:
    return [p for p in policy.get("blocked_paths", []) if isinstance(p, str)]


def published_branches(policy: dict) -> list:
    return [p for p in policy.get("published_branches", []) if isinstance(p, str)]


# ---------------------------------------------------------------------------
# Authorization + decision
# ---------------------------------------------------------------------------
def is_policy_change(path: str) -> bool:
    """True iff the changed path is gate/policy machinery, which only the
    authorized sync bot may modify: ``publish-policy.json``, anything under
    ``.github/workflows/``, or anything under ``.github/publish-policy/`` — the
    gate's own base-side executable, whose post-merge copy the next
    ``pull_request_target`` run trusts and executes.

    ``path`` is the API's exact git path — compared byte-for-byte, not
    host-normalized (these are canonical git paths already)."""

    p = path
    return (
        p == POLICY_FILENAME
        or p.startswith(WORKFLOWS_PREFIX)
        or p.startswith(GATE_DIR_PREFIX)
    )


def is_authorized_sync_bot(
    pr_author: Optional[str],
    pr_head_ref: Optional[str],
    sync_bot_login: Optional[str],
    pr_base_ref: Optional[str] = None,
    allowed_bases: Iterable[str] = (),
    verified_default_branch: Optional[str] = None,
) -> bool:
    """The authorized maintainer update path: PR authored by the configured
    sync-bot account **and** on an attempt branch bound to the PR base.

    Both conditions are required — a bot pushing to a non-attempt branch, or
    anyone else pushing to a ``sync/<hash>`` branch, is **not** authorized. When
    no ``sync_bot_login`` is configured, no PR is ever authorized (fail closed:
    the exemption is opt-in)."""

    if not sync_bot_login:
        return False
    if not pr_author or pr_author.lower() != sync_bot_login.lower():
        return False
    if not pr_head_ref or not pr_base_ref or pr_base_ref not in set(allowed_bases):
        return False
    mapped = MAPPED_SYNC_BRANCH_RE.fullmatch(pr_head_ref)
    if mapped:
        return mapped.group("base") == pr_base_ref
    if LEGACY_SYNC_BRANCH_RE.fullmatch(pr_head_ref):
        return bool(verified_default_branch and pr_base_ref == verified_default_branch)
    return False


@dataclass
class Violation:
    path: str
    reason: str  # "blocked-path" | "policy-change"
    pattern: Optional[str] = None


@dataclass
class Decision:
    allowed: bool
    authorized: bool
    violations: list = field(default_factory=list)

    def summary(self) -> str:
        if self.allowed:
            who = "authorized sync-bot update" if self.authorized else "no protected paths touched"
            return f"ALLOW — {who}"
        lines = [f"DENY — {len(self.violations)} disallowed change(s):"]
        for v in self.violations:
            if v.reason == "blocked-path":
                lines.append(f"  - {v.path}: blocked path (matches {v.pattern!r})")
            else:
                lines.append(f"  - {v.path}: policy/gate change not from the authorized sync bot")
        return "\n".join(lines)


def evaluate(
    policy: dict,
    changed_files: Iterable[str],
    *,
    pr_author: Optional[str] = None,
    pr_head_ref: Optional[str] = None,
    pr_base_ref: Optional[str] = None,
    sync_bot_login: Optional[str] = None,
    verified_default_branch: Optional[str] = None,
) -> Decision:
    """Pure decision core: (base policy + changed files + PR author/branch) ->
    allow/deny.

    * A **blocked path** is denied for everyone (the sync bot's snapshots are
      export-ignore-filtered, so they never carry blocked content anyway).
    * A **policy/gate change** (``publish-policy.json`` or
      ``.github/workflows/``) is denied unless the PR is the authorized sync
      bot's own snapshot (author + base-bound namespaced attempt branch, or the
      verified-default-only legacy ``sync/<hash>`` form).

    Findings are collected (not short-circuited) so the PR author sees every
    problem at once.
    """

    authorized = is_authorized_sync_bot(
        pr_author, pr_head_ref, sync_bot_login,
        pr_base_ref=pr_base_ref,
        allowed_bases=published_branches(policy),
        verified_default_branch=verified_default_branch,
    )
    globs = blocked_paths(policy)
    violations: list = []
    for raw in changed_files:
        # Use the API's exact git path — byte-for-byte, no host normalization:
        # a backslash is filename DATA (not a separator) and edge whitespace is
        # significant. Corrupting it here could hide a blocked path.
        path = raw
        if not path:
            continue
        pat = matches_any(path, globs)
        if pat is not None:
            violations.append(Violation(path=path, reason="blocked-path", pattern=pat))
            continue
        if is_policy_change(path) and not authorized:
            violations.append(Violation(path=path, reason="policy-change"))
    return Decision(allowed=not violations, authorized=authorized, violations=violations)


# ---------------------------------------------------------------------------
# Changed-files listing (PR-as-DATA, via the API — never a head checkout)
# ---------------------------------------------------------------------------
CommandRunner = Callable[..., "subprocess.CompletedProcess"]


def _run(argv: list) -> "subprocess.CompletedProcess":
    return subprocess.run(argv, capture_output=True, text=True)


# GitHub caps the changed-files listing at 3,000 files even with pagination. A
# PR above this can hide a blocked path outside the returned set, so the gate
# must fail closed (deny) rather than allow on an incomplete listing.
FILE_LISTING_CAP = 3000


class PRTooLarge(RuntimeError):
    """The PR touches more files than the API listing can return, so the changed
    set cannot be enumerated completely. The gate denies (fail closed) rather
    than judge a PR it cannot fully see."""


def _iter_json_values(text: str):
    """Yield every top-level JSON value in ``text``. ``gh api --paginate`` over
    an array endpoint normally returns one combined array, but tolerate several
    concatenated arrays/objects too. Raises ``ValueError`` on malformed JSON."""

    decoder = json.JSONDecoder()
    idx = 0
    n = len(text)
    while idx < n:
        while idx < n and text[idx].isspace():
            idx += 1
        if idx >= n:
            break
        value, end = decoder.raw_decode(text, idx)
        yield value
        idx = end


def changed_files_count(repo: str, pr_number: int, runner: CommandRunner = _run) -> int:
    """The PR's total ``changed_files`` count, from the PR object (authoritative,
    independent of the 3,000-file listing cap)."""

    res = runner(["gh", "api", f"repos/{repo}/pulls/{pr_number}",
                  "--jq", ".changed_files"])
    if res.returncode != 0:
        raise RuntimeError(f"gh api pulls/{pr_number} failed: {res.stderr.strip()}")
    try:
        return int((res.stdout or "").strip())
    except ValueError as exc:
        raise RuntimeError(f"could not read changed_files count: {exc}") from exc


def repository_default_branch(repo: str, runner: CommandRunner = _run) -> str:
    """Resolve the verified public default from the trusted repository API."""

    res = runner(["gh", "api", f"repos/{repo}", "--jq", ".default_branch"])
    if res.returncode != 0 or not (res.stdout or "").strip():
        raise RuntimeError(f"cannot resolve repository default branch: {res.stderr.strip()}")
    return res.stdout.strip()


def list_changed_files(
    repo: str,
    pr_number: int,
    runner: CommandRunner = _run,
) -> list:
    """List a PR's changed file paths **as data** via the GitHub API, losslessly
    and completely.

    Reads the authoritative ``changed_files`` count from the PR object, then the
    paginated ``pulls/<n>/files`` **JSON**. The gate fails closed
    (:class:`PRTooLarge`, → CLI DENY) whenever the returned records cannot be
    trusted to be the full set: above :data:`FILE_LISTING_CAP` (the API's hard
    listing limit — a specific message), or — more generally — whenever the
    number of returned file records does not equal the declared ``changed_files``
    count. Never allow a PR whose full change set cannot be enumerated.

    Filenames come straight from each object's ``filename`` / ``previous_filename``
    field (JSON-decoded, never ``--jq`` line output or ``splitlines()``/``strip()``),
    so a name containing a newline or significant edge whitespace is preserved
    exactly. Never fetches, checks out, or executes PR-head content."""

    count = changed_files_count(repo, pr_number, runner=runner)
    if count > FILE_LISTING_CAP:
        raise PRTooLarge(
            f"PR touches {count} files, above the {FILE_LISTING_CAP}-file listing "
            "cap; cannot enumerate the full change set to gate it."
        )

    res = runner(["gh", "api", "--paginate", f"repos/{repo}/pulls/{pr_number}/files"])
    if res.returncode != 0:
        raise RuntimeError(f"gh api pulls/{pr_number}/files failed: {res.stderr.strip()}")
    try:
        objs = [o for value in _iter_json_values(res.stdout or "") for o in value]
    except (ValueError, TypeError) as exc:
        raise RuntimeError(f"could not parse pulls/{pr_number}/files JSON: {exc}") from exc

    # Completeness: one record per changed file. If the returned record count
    # does not match the authoritative changed_files, the listing is incomplete
    # (or otherwise untrustworthy) at ANY size — fail closed rather than gate a
    # partial view. This subsumes the >cap case but catches sub-cap gaps too.
    if len(objs) != count:
        raise PRTooLarge(
            f"changed-files listing returned {len(objs)} record(s) but the PR "
            f"declares {count} changed file(s); the set is incomplete — refusing "
            "to gate a partial view."
        )

    files: list = []
    for obj in objs:
        if not isinstance(obj, dict):
            raise RuntimeError("pulls/files element is not a JSON object")
        fn = obj.get("filename")
        if isinstance(fn, str) and fn:
            files.append(fn)
        prev = obj.get("previous_filename")
        if isinstance(prev, str) and prev:  # rename: also see the old (maybe blocked) path
            files.append(prev)
    return files


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: Optional[list] = None, runner: CommandRunner = _run) -> int:
    parser = argparse.ArgumentParser(
        prog="policy_gate.py",
        description="Base-context publish-policy gate (changed-files vs policy).",
    )
    parser.add_argument("--policy", required=True,
                        help="path to publish-policy.json from the BASE checkout")
    parser.add_argument("--repo", required=True, help="owner/name of the GitHub repo")
    parser.add_argument("--pr", required=True, type=int, help="pull request number")
    parser.add_argument("--pr-author", default="", help="PR author login (from the event payload)")
    parser.add_argument("--pr-head-ref", default="", help="PR head branch ref (from the event payload)")
    parser.add_argument("--base-ref", required=True,
                        help="PR base branch ref (from the event payload)")
    parser.add_argument("--sync-bot-login", default="",
                        help="authorized sync-bot account login (enables the maintainer update path)")
    parser.add_argument("--changed-files-file", default="",
                        help="optional: read newline-separated changed paths from this file "
                             "instead of calling the API (testing/offline)")
    args = parser.parse_args(argv)

    try:
        policy = load_policy(args.policy)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"policy-gate: infra error: cannot load policy: {exc}", file=sys.stderr)
        return EXIT_INFRA

    try:
        verified_default = repository_default_branch(args.repo, runner=runner)
    except RuntimeError as exc:
        print(f"policy-gate: infra error: {exc}", file=sys.stderr)
        return EXIT_INFRA

    try:
        if args.changed_files_file:
            with open(args.changed_files_file, "r", encoding="utf-8") as handle:
                changed = [ln.rstrip("\n") for ln in handle if ln.rstrip("\n")]
        else:
            changed = list_changed_files(args.repo, args.pr, runner=runner)
    except PRTooLarge as exc:
        # Fail CLOSED, not infra: a PR we cannot fully enumerate must be denied,
        # not allowed through on an incomplete listing.
        print(f"policy-gate: DENY — {exc}", file=sys.stderr)
        return EXIT_DENY
    except (OSError, RuntimeError) as exc:
        print(f"policy-gate: infra error: cannot list changed files: {exc}", file=sys.stderr)
        return EXIT_INFRA

    decision = evaluate(
        policy,
        changed,
        pr_author=args.pr_author or None,
        pr_head_ref=args.pr_head_ref or None,
        pr_base_ref=args.base_ref,
        sync_bot_login=args.sync_bot_login or None,
        verified_default_branch=verified_default,
    )
    print(decision.summary())
    return EXIT_ALLOW if decision.allowed else EXIT_DENY


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
