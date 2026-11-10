# Merry Meeple artifact access: design

**Date:** 2026-11-10
**Branch:** `feat/merry-meeple-artifact-access` (from `main`), one PR, conventional commits
**Related:** MM-website spec `docs/superpowers/specs/2026-11-09-game-neighbours-design.md`
(phase 2) and its architecture page `docs/architecture/game-recommendations.html`.

## Why

The Merry Meeple's games page (cpdelehanty/MM-website) ranks game neighbours in the
browser from two bgg-viewer artifacts: the **vectors** and the **catalog**. Live, a
Cloudflare Pages Function in that repo signs short-lived GCS URLs, and the browser downloads
the files straight from `bgg-data-warehouse-bgg-viewer-artifacts`. For that, the café needs:

1. an identity that can read those two artifacts, and nothing else;
2. a key for it, held only as a Cloudflare secret;
3. its origins on the bucket's CORS list, or browsers refuse the bytes.

This PR is the GCP side of that. The function itself is MM-website's.

## Changes (`terraform/merry_meeple.tf`, plus one line in `storage.tf`)

1. **Service account** `merry-meeple-artifacts`
   ("Merry Meeple site: reads vectors + catalog artifacts"). No other roles anywhere.
2. **Conditioned read grant.** `roles/storage.objectViewer` on the artifacts bucket, with an
   IAM condition limiting it to objects whose names start `vectors-` or `catalog-`:
   ```
   resource.name.startsWith("projects/_/buckets/<bucket>/objects/vectors-") ||
   resource.name.startsWith("projects/_/buckets/<bucket>/objects/catalog-")
   ```
   That covers the data files (`vectors-<hash>.arrow.gz`, `catalog-<hash>.arrow.gz`, named by
   bgg-viewer `scripts/lib/publish-artifact.ts`) and their pointers (`vectors-current.json`,
   `catalog-current.json`), and excludes thumbnails and coordinates. Listing the bucket is
   denied (the condition is false at bucket level); the function never lists. The bucket
   already has uniform bucket-level access, which conditions require.
3. **CORS:** add to the existing `cors.origin` list on `bgg_viewer_artifacts`:
   `https://themerrymeeple.com`, `https://www.themerrymeeple.com`,
   `http://localhost:3000`, `http://localhost:8788` (the café's local server and
   `wrangler pages dev`'s default). Existing origins unchanged.
4. **Output** `merry_meeple_artifacts_service_account` (the email), for the runbook.

**No key in terraform.** The key is created by hand in the Cloud Console after apply, as
the CI key (`GCP_SA_KEY_BGG_DW`) was, so the private key never sits in terraform state.
A comment on the service account points at the runbook.

## Runbook: `docs/runbooks/merry-meeple-artifact-key.md`

The key lives outside terraform, so the one thing that can be forgotten is rotating it.
The runbook covers: what the key is for and can reach; where it lives (Cloudflare Pages
secret `GCS_SA_KEY` on project `the-merry-meeple`, production; and the café's local
`.dev.vars`); creating it; handing it over (a password-manager share or Signal, never plain
email or chat); **rotation** step by step (create new → café updates secret + redeploys →
verify → delete old); emergency revocation; a key-age check in the console; and a record of
when the current key was made. Rotation interval: **12 months**, with a calendar reminder
Phil sets by hand (bot-opened GitHub issues don't reach him).

## Verification

- PR's `terraform plan` (prod) shows exactly: 1 service account created, 1 bucket IAM member
  created, the bucket updated in place with only the four CORS origins added; nothing else.
- After merge + apply: create the key, then from Phil's machine sign a URL for
  `vectors-current.json` with it and fetch it (200), and sign one for a `thumbnails-*` object
  (403). Recorded in the runbook as the post-rotation check too.

## Non-goals

The Cloudflare function and the switch in MM-website; preview-site origins
(`*.pages.dev`); putting the key in terraform; any change to bgg-viewer's own access.
