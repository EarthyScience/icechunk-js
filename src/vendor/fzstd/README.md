# Vendored `fzstd`

This directory contains a vendored copy of [`fzstd`](https://github.com/101arrowz/fzstd)
(MIT, © 2020 Arjun Barrett) used for zstd decompression — both the outer
metadata-file decompression and the dictionary-compressed virtual chunk
locations in manifests.

## Why it's vendored (not an npm dependency)

- **Released `fzstd` (0.1.1) has no dictionary support.** Its `decompress()`
  takes no dictionary argument, and icechunk's `compressed_location` fields are
  zstd frames compressed against a per-manifest dictionary — undecodable
  without it. Dictionary support exists only in the unmerged
  [fzstd#18](https://github.com/101arrowz/fzstd/pull/18).
- **The PR branch can't be cleanly `npm install`ed** — its `package.json`
  `main`/`module` point at build outputs that aren't committed, and there's no
  `prepare` hook, so a git/`github:` install resolves to missing files.
- **`zstdify` (the other dictionary-capable JS zstd lib) silently corrupts
  data** — it returned wrong bytes (same length, no error) for ~32% of real
  icechunk metadata fixtures, at both 1.4.0 and `main` (the PR#3 "fix" does not
  address it). `fzstd` is byte-exact vs the reference `zstd` CLI on all of them.

So we vendor the dictionary-capable source and decompress with it everywhere.

## Upstream source

| | |
|---|---|
| Repo | `101arrowz/fzstd` |
| Branch | `handlerug:push-qxkytkvukoqs` (= the head of fzstd#18) |
| Commit | `819999b` (dictionary support + the aliasing fix, see below) |
| File | `src/index.ts` → `index.ts` here (with a header prepended) |
| License | `LICENSE` (copied verbatim) |

`index.ts` is `@ts-nocheck`'d and excluded from Prettier (`.prettierignore`) to
stay byte-identical to upstream.

## Local modifications

**None.** This copy is byte-identical to the branch.

A dictionary/output-buffer aliasing bug in the original PR (silent corruption
when the dictionary-content length equals the decompressed size) was found here
and fixed upstream via [handlerug/fzstd#1](https://github.com/handlerug/fzstd/pull/1),
now merged into the branch — so there is no longer a local patch to re-apply.
Regression coverage lives in
`tests/format/flatbuffers/manifest-dictionary.test.ts` ("decodes a frame whose
output aliases the dictionary buffer").

> Note: the size-bound (`MAX_DECOMPRESSED_LOCATION_SIZE`) and UTF-8 strictness
> guards are **not** here — they live in the consumer
> (`src/format/flatbuffers/manifest-parser.ts`), since bounding untrusted input
> is the caller's job, not the decoder's.

## Re-vendoring (updating the copy)

```sh
# Fetch the upstream source for the pinned branch/commit:
gh api "repos/handlerug/fzstd/contents/src/index.ts?ref=push-qxkytkvukoqs" \
  --jq '.content' | base64 -d > /tmp/fzstd-index.ts
```

Then:
1. Re-prepend the provenance header (top of the current `index.ts`), updating
   the pinned commit.
2. Re-run the suite: `npm test` (the aliasing + dictionary tests must pass).

## When this can be deleted

Once fzstd#18 **and** the aliasing fix land upstream and a release is published
to npm: delete this directory, add `fzstd@<version>` to `dependencies`, and
repoint the four imports (`src/reader/session.ts`,
`src/format/flatbuffers/{repo,manifest}-parser.ts`, and the two parser tests).

## Related

- Consumer: `src/format/flatbuffers/manifest-parser.ts` (dictionary decode,
  decode priority matching the Rust `ref_to_payload`, size/UTF-8 guards).
- Test fixtures: `tests/fixtures/dictionary/` (generated with the `zstd` CLI;
  generation commands documented in the test file headers).
