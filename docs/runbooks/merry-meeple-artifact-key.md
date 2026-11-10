# Runbook: The Merry Meeple's artifact key

> **There is no rotation schedule.** The key doesn't expire. Replace it **when it stops
> working**, or if you think it has leaked. Whoever does this won't remember how: every step
> below is a command to copy. Run them in Git Bash, in order; each ends with a check that
> must pass before the next.
>
> The key is outside terraform on purpose, so the private key never sits in terraform state.

## When to do this

- **It stopped working.** The café's site says recommendations are "unavailable", or
  `curl -s https://themerrymeeple.com/api/artifact/vectors` returns
  `{"error":"artifact unavailable"}` (HTTP 503). Run [Diagnose](#diagnose-it-stopped-working)
  first: it's often a missing secret on their side, not the key.
- **It may have leaked** (posted somewhere, laptop lost, someone who had it shouldn't).
  Do [Emergency: revoke now](#emergency-revoke-now), then steps 0–9.
- **First setup:** steps 0–6 and 9.

## What this key is

| | |
|---|---|
| Service account | `merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com` (terraform output `merry_meeple_artifacts_service_account`) |
| Defined in | `terraform/merry_meeple.tf` |
| Can read | Only objects named `vectors-*` and `catalog-*` in `bgg-data-warehouse-bgg-viewer-artifacts`: the vectors and catalog artifacts and their `*-current.json` pointers. Nothing else in the bucket or project. Cannot list, write or delete. |
| Used by | The Merry Meeple website (cpdelehanty/MM-website): `functions/api/artifact/[name].js` signs short-lived download URLs with it. |
| Lives in | Cloudflare Pages project **`the-merry-meeple`**, production secret **`GCS_SA_KEY`** (the whole JSON key). Also in `MM-website/.dev.vars` (gitignored) on any machine used for local testing. |
| Who | Phil creates, rotates and revokes. The café stores it in Cloudflare and redeploys. |

If it leaks, the worst case is someone downloading the vectors and catalog. Still rotate it,
and revoke it at once if it appears anywhere public.

## Current key

| Key ID | Created | Handed to |
|---|---|---|
| _(fill in)_ | | |

Update this table in a PR every time the key changes.

---

## 0. Set these first (every time)

```bash
SA=merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com
PROJECT=bgg-data-warehouse
TODAY=$(date +%Y-%m-%d)
KEY_FILE="C:/Users/philh/merry-meeple-key-$TODAY.json"   # outside every repo
MM=/c/Users/philh/projects/MM-website                    # your MM-website checkout
```

Check you are authenticated as yourself: `gcloud auth list` shows your account as active.

## 1. Note the key(s) that exist now

```bash
gcloud iam service-accounts keys list --iam-account="$SA" --project="$PROJECT" --managed-by=user
```

Write down the current KEY_ID: it is the **old** key you will delete in step 8.
(First time ever: the list is empty; skip step 8.)

## 2. Create the new key

```bash
gcloud iam service-accounts keys create "$KEY_FILE" --iam-account="$SA" --project="$PROJECT"
```

It prints `created key [<NEW_KEY_ID>]`. Check:

```bash
gcloud iam service-accounts keys list --iam-account="$SA" --project="$PROJECT" --managed-by=user
```

shows the new KEY_ID with today's date (and the old one, still valid).

## 3. Put it in your local `.dev.vars` and test locally

`.dev.vars` needs the JSON **on one line, in single quotes** (so the `\n` in the private key
survives). This writes it:

```bash
cd "$MM"
python -c "import json,sys; k=json.load(open(sys.argv[1])); print(\"GCS_SA_KEY='\" + json.dumps(k, separators=(',', ':')) + \"'\")" "$KEY_FILE" > .dev.vars
git check-ignore .dev.vars    # must print: .dev.vars   (if it prints nothing, STOP: delete .dev.vars)
```

Start the site with its function (leave this running in its own terminal):

```bash
cd "$MM" && npx wrangler pages dev . --port 8788
```

In another terminal, check the function hands out working links:

```bash
curl -s http://localhost:8788/api/artifact/vectors   # expect JSON with "url", "hash", "builtAt"
URL=$(curl -s http://localhost:8788/api/artifact/vectors | python -c "import json,sys; print(json.load(sys.stdin)['url'])")
curl -s -o /dev/null -w "%{http_code}\n" "$URL"        # expect 200
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8788/api/artifact/catalog   # expect 200
```

A `503` from the function means the key in `.dev.vars` is wrong (re-run step 3's python
line). A `403` on `$URL` means the key's permissions are wrong (check `terraform/merry_meeple.tf`
was applied). Stop wrangler with Ctrl-C when done.

## 4. Hand the key to the café

Send `$KEY_FILE` through a **password-manager share or Signal**. Never email, chat, a GitHub
issue/PR, or a commit. Tell them it's a rotation and to do step 5 today.

## 5. The café: replace the secret and redeploy

The café runs, in their MM-website checkout, with the key file saved as `key.json`:

```bash
npx wrangler pages secret put GCS_SA_KEY --project-name=the-merry-meeple < key.json
./deploy.sh          # secrets reach the live site only with a new deployment
rm key.json
```

(The Cloudflare dashboard works too: Workers & Pages → `the-merry-meeple` → Settings →
Variables and Secrets → Production → edit `GCS_SA_KEY`, paste the whole file, then redeploy.)

## 6. Verify the live site uses the new key

```bash
curl -s https://themerrymeeple.com/api/artifact/vectors   # expect JSON with "url"
URL=$(curl -s https://themerrymeeple.com/api/artifact/vectors | python -c "import json,sys; print(json.load(sys.stdin)['url'])")
curl -s -o /dev/null -w "%{http_code}\n" "$URL"            # expect 200
echo "$URL" | grep -o 'X-Goog-Credential=[^%]*'            # the service account, sanity check
```

The live check alone can't tell old key from new (both work until step 8); step 8 is what
proves the café switched. **Do not start step 8 until the café confirms step 5 is done.**

## 7. (Optional) wait a day

If you want zero risk, leave both keys valid for 24h and re-run step 6 tomorrow.

## 8. Delete the old key

```bash
gcloud iam service-accounts keys delete <OLD_KEY_ID> --iam-account="$SA" --project="$PROJECT"
```

Then **immediately re-run step 6.** Still `200`: the café is on the new key, done.
`503` or "unavailable" on the site: the café is still on the old (now deleted) key. Ask them
to redo step 5 with the new key; the site recovers as soon as they redeploy.

## 9. Clean up and record

```bash
rm "$KEY_FILE"     # your copy; .dev.vars keeps the one-line version for local testing
gcloud iam service-accounts keys list --iam-account="$SA" --project="$PROJECT" --managed-by=user   # exactly one key
```

- Update [Current key](#current-key) (new KEY_ID, `$TODAY`, who received it) in a PR.

---

## Emergency: revoke now

Key leaked, laptop lost, anything unexpected:

```bash
gcloud iam service-accounts keys delete <KEY_ID> --iam-account="$SA" --project="$PROJECT"
# or cut all access, every key, at once:
gcloud iam service-accounts disable "$SA" --project="$PROJECT"
```

The café's recommendations show "unavailable" (the rest of their site is fine) until you do
steps 2–6 with a new key (and `gcloud iam service-accounts enable "$SA" --project="$PROJECT"`
if you disabled it).

## Diagnose: it stopped working

Set the variables from [step 0](#0-set-these-first-every-time), then:

```bash
gcloud iam service-accounts describe "$SA" --project="$PROJECT" --format="value(disabled)"
gcloud iam service-accounts keys list --iam-account="$SA" --project="$PROJECT" --managed-by=user
```

- **Account disabled (`True`):** someone disabled it, or Google did after detecting the key
  in public. Treat as a leak: `gcloud iam service-accounts enable "$SA" --project="$PROJECT"`,
  delete every listed key (`keys delete <KEY_ID> ...`), then steps 0–9.
- **No keys listed, or the KEY_ID in [Current key](#current-key) is missing or shows
  `DISABLED`:** the key is gone. Steps 0–9 (skip 8).
- **Key listed and enabled:** the key is fine; the problem is on the café's side. Ask them
  to check the `GCS_SA_KEY` secret exists on the `the-merry-meeple` production environment,
  and to redeploy (`./deploy.sh`). If it still fails, run step 3 locally with a fresh key to
  tell a key problem from a site problem.
