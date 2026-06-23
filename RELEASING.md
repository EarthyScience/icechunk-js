# Releasing

Publishing to npm runs in CI (`.github/workflows/publish.yaml`).

1. `npm version <patch|minor|major>` — bumps `package.json`, commits, tags `vX.Y.Z`.
2. `git push --follow-tags`
3. `gh release create vX.Y.Z --generate-notes` — triggers the publish (the tag alone does not).
4. CI installs, tests, builds, and publishes. Verify with `npm view icechunk-js version`.

Manual fallback: `npm ci && npm run build && npm publish --access public`.
