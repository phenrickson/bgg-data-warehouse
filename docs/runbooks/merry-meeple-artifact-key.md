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
  Do [Emergency: revoke now](#emergency-revoke-now), then steps 1–9.
- **First setup:** steps 0–3, 4–6 with the café, and 9.

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

## 0. Check you're signed in

```bash
gcloud auth list
```

Your own account is marked active. Every command below is complete as written: copy it,
run it. Only `<OLD_KEY_ID>` / `<KEY_ID>` need filling in, from a `keys list` output.

## 1. Note the key(s) that exist now

```bash
gcloud iam service-accounts keys list --iam-account=merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse --managed-by=user
```

Write down the current KEY_ID: it is the **old** key you will delete in step 8.
(First time ever: the list is empty; skip step 8.)

## 2. Create the new key

If `C:/Users/philh/merry-meeple-key.json` already exists from a previous time, delete it first (`rm /c/Users/philh/merry-meeple-key.json`).

```bash
gcloud iam service-accounts keys create "C:/Users/philh/merry-meeple-key.json" --iam-account=merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse
```

It prints `created key [<NEW_KEY_ID>] of type [json] as [...]`. The file is written straight to
`C:/Users/philh/merry-meeple-key.json`, nothing to download. Check:

```bash
gcloud iam service-accounts keys list --iam-account=merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse --managed-by=user
```

shows the new KEY_ID with today's date (and the old one, still valid).

## 3. Put it in your local `.dev.vars` and test locally

`.dev.vars` needs the JSON **on one line, in single quotes** (so the `
` in the private key
survives). This writes it:

```bash
cd /c/Users/philh/projects/MM-website
python -c "import json; k=json.load(open('C:/Users/philh/merry-meeple-key.json')); print(\"GCS_SA_KEY='\" + json.dumps(k, separators=(',', ':')) + \"'\")" > .dev.vars
git check-ignore .dev.vars
```

The last line must print `.dev.vars`. If it prints nothing, **stop** and delete `.dev.vars`.

Start the site with its function (it holds this terminal):

```bash
cd /c/Users/philh/projects/MM-website && npx wrangler pages dev . --port 8788
```

In a second terminal:

```bash
curl -s http://localhost:8788/api/artifact/vectors
```

Expect JSON with `"url"`, `"hash"`, `"builtAt"`. Then check the link it hands out downloads
(expect `200`), for both artifacts:

```bash
curl -s http://localhost:8788/api/artifact/vectors | python -c "import json,sys; print(json.load(sys.stdin)['url'])" | xargs curl -s -o /dev/null -w "%{http_code}\n"
curl -s http://localhost:8788/api/artifact/catalog | python -c "import json,sys; print(json.load(sys.stdin)['url'])" | xargs curl -s -o /dev/null -w "%{http_code}\n"
```

Finally open http://localhost:8788/games/ in a browser: "I Know a Game I Like" returns
results, and a game page shows "You May Also Like". (Only a browser checks CORS; `curl` can't.)

- `{"error":"artifact unavailable"}` from the first `curl`: `.dev.vars` is wrong; redo the
  `python` line.
- `403` from the download check: the key's permissions are wrong; check
  `terraform/merry_meeple.tf` is applied.
- Page says "unavailable" but both `curl`s pass: open the browser Console (F12). A CORS error
  means the address isn't on the bucket's list in `terraform/storage.tf` (use exactly
  `http://localhost:8788`, not `127.0.0.1`). Right after a CORS change, allow a few minutes.

Stop wrangler with Ctrl-C when done.

## 4. Hand the key to the café

Send `C:/Users/philh/merry-meeple-key.json` through a **password-manager share or Signal**. Never email, chat, a GitHub
issue/PR, or a commit. Tell them it's a new key and to do step 5 today.

## 5. The café: replace the secret and redeploy

The café saves the file as `key.json` in their MM-website checkout and runs:

```bash
npx wrangler pages secret put GCS_SA_KEY --project-name=the-merry-meeple < key.json
./deploy.sh
rm key.json
```

Secrets reach the live site only with a new deployment, hence `./deploy.sh`.
(The Cloudflare dashboard works too: Workers & Pages → `the-merry-meeple` → Settings →
Variables and Secrets → Production → edit `GCS_SA_KEY`, paste the whole file, then redeploy.)

## 6. Verify the live site

```bash
curl -s https://themerrymeeple.com/api/artifact/vectors
curl -s https://themerrymeeple.com/api/artifact/vectors | python -c "import json,sys; print(json.load(sys.stdin)['url'])" | xargs curl -s -o /dev/null -w "%{http_code}\n"
```

Expect JSON with `"url"`, then `200`. This alone can't tell the old key from the new (both
work until step 8). **Do not start step 8 until the café confirms step 5 is done.**

## 7. (Optional) wait a day

If you want zero risk, leave both keys valid for 24h and re-run step 6 tomorrow.

## 8. Delete the old key

```bash
gcloud iam service-accounts keys delete <OLD_KEY_ID> --iam-account=merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse
```

Then **immediately re-run step 6.** Still `200`: the café is on the new key, done.
`artifact unavailable` / "unavailable" on the site: the café is still on the old (now
deleted) key. Ask them to redo step 5 with the new key; it recovers as soon as they redeploy.

## 9. Clean up and record

```bash
rm /c/Users/philh/merry-meeple-key.json
gcloud iam service-accounts keys list --iam-account=merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse --managed-by=user
```

The `rm` deletes your copy (`.dev.vars` keeps the one-line version for local testing). The
list must show exactly one key. Then update [Current key](#current-key) (KEY_ID, date, who
received it) in a PR.

---

## Emergency: revoke now

Key leaked, laptop lost, anything unexpected. Delete the key:

```bash
gcloud iam service-accounts keys delete <KEY_ID> --iam-account=merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse
```

or cut all access, every key, at once:

```bash
gcloud iam service-accounts disable merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse
```

The café's recommendations show "unavailable" (the rest of their site is fine) until you do
steps 2–6 with a new key, after re-enabling the account if you disabled it:

```bash
gcloud iam service-accounts enable merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse
```

## Diagnose: it stopped working

```bash
gcloud iam service-accounts describe merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse --format="value(disabled)"
gcloud iam service-accounts keys list --iam-account=merry-meeple-artifacts@bgg-data-warehouse.iam.gserviceaccount.com --project=bgg-data-warehouse --managed-by=user
```

- **First line prints `True` (account disabled):** someone disabled it, or Google did after
  detecting the key in public. Treat as a leak: re-enable it (command above), delete every
  listed key, then steps 1–9.
- **No keys listed, or the KEY_ID in [Current key](#current-key) is missing or `DISABLED`:**
  the key is gone. Steps 1–9 (skip 8).
- **Key listed and enabled:** the key is fine; the problem is on the café's side. Ask them to
  check the `GCS_SA_KEY` secret exists in the `the-merry-meeple` production environment, and
  to redeploy (`./deploy.sh`). If it still fails, run step 3 locally to tell a key problem
  from a site problem.
