# Runbook: The Merry Meeple's artifact key

> **Rotate every 12 months.** Nothing automated will remind you: the key is outside
> terraform on purpose. Keep a calendar reminder for the date in "Current key" below.

## What this key is

| | |
|---|---|
| Service account | `merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com` (terraform output `merry_meeple_artifacts_service_account`) |
| Defined in | `terraform/merry_meeple.tf` |
| Can read | Only objects named `vectors-*` and `catalog-*` in `bgg-data-warehouse-bgg-viewer-artifacts`: the vectors and catalog artifacts and their `*-current.json` pointers. Nothing else in the bucket or the project. Cannot list, write or delete. |
| Used by | The Merry Meeple website (cpdelehanty/MM-website): a Cloudflare Pages Function signs short-lived download URLs with it. |
| Lives in | Cloudflare Pages project `the-merry-meeple`, **production** environment, secret **`GCS_SA_KEY`** (the whole JSON key file as the value). For local testing, the café's gitignored `.dev.vars` may hold a copy. |
| Owner | Phil creates, rotates and revokes it. The café stores it. |

If this key leaks, the worst case is someone downloading the vectors and catalog artifacts.
Still rotate it, and revoke it immediately if it shows up anywhere public.

## Current key

| Key ID | Created | Rotate by | Handed to |
|---|---|---|---|
| _(fill in when created)_ | | | |

Update this table, in a PR, every time the key changes.

## Create a key (first time, or as step 1 of a rotation)

1. Cloud Console → IAM & Admin → Service Accounts → `merry-meeple-artifacts` → **Keys** →
   **Add key** → **Create new key** → JSON. A file downloads.
2. Note the key ID (shown in the Keys list) and today's date in "Current key" above.
3. Set a calendar reminder for 12 months out: "Rotate Merry Meeple artifact key: see
   bgg-data-warehouse docs/runbooks/merry-meeple-artifact-key.md".

## Hand it over

Send the JSON file through a password-manager share or Signal. **Never** plain email, chat,
a GitHub issue/PR, or a commit. Delete your downloaded copy once the café confirms it's stored.

The café then:
1. Cloudflare dashboard → Workers & Pages → `the-merry-meeple` → Settings → Variables and
   Secrets → Production → add/replace secret `GCS_SA_KEY` with the whole JSON as the value.
   (Or `npx wrangler pages secret put GCS_SA_KEY --project-name=the-merry-meeple`.)
2. Redeploy (`./deploy.sh`): Pages Functions read secrets at deploy time.
3. Check: open a game page on themerrymeeple.com; the "You May Also Like" row appears.

## Rotate (every 12 months, or whenever in doubt)

Old and new keys both work until the old one is deleted, so there is no outage.

1. **Create** a new key (above). Don't touch the old one yet.
2. **Hand it over**; the café replaces `GCS_SA_KEY` and redeploys.
3. **Verify** the live site (game page shows the row; "I Know a Game I Like" returns results).
4. **Delete the old key**: Service Accounts → `merry-meeple-artifacts` → Keys → delete the
   old key ID. If the site breaks now, the café is still on the old key: redo step 2.
5. **Record** the new key in "Current key" and move the calendar reminder.

## Revoke in an emergency

Service Accounts → `merry-meeple-artifacts` → Keys → delete the key. The café's
recommendations go to "unavailable" (the rest of their site is unaffected) until a new key
is created and handed over. To cut access entirely, disable the service account instead.

## Check a key works

With the key file, sign a URL for `vectors-current.json` and fetch it: expect **200**. Sign
one for any `thumbnails-*` object: expect **403**. Do this after creating or rotating.

## How old is the current key?

Cloud Console → Service Accounts → `merry-meeple-artifacts` → Keys shows each key's creation
date. Anything older than 12 months is overdue.
