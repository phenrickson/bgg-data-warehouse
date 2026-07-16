# Cloud Run Service Auth Pattern (reusable)

## Problem

Cloud Run services in this ecosystem are deployed `--allow-unauthenticated` with
`roles/run.invoker` granted to **`allUsers`** — confirmed live for all five
`bgg-predictive-models` services (`bgg-model-scoring`, `bgg-embeddings-service`,
`bgg-collection-scoring`, `bgg-text-embeddings-service`, `bgg-streamlit-prod`). Anyone
who discovers a `.run.app` URL can invoke them: pull proprietary model outputs, burn
compute, and open a public dashboard. We need a gating pattern that (a) lets an
authorized human access a service easily, (b) makes *granting* access a simple,
revocable operation, and (c) applies unchanged to every service so we can extend it to
the predictive-models services.

## Pattern

**Gate — Cloud Run IAM.** Deploy every gated service `--no-allow-unauthenticated` and
ensure `allUsers` is **not** in its IAM policy. The service is then invocable only by
identities holding `roles/run.invoker` on it.

**Grant surface — one invoker Google Group, bound via Terraform.** Create a single
group, e.g. `bgg-api-invokers@googlegroups.com` (or a Cloud Identity group). Grant it
`roles/run.invoker` on each gated service through **Terraform** — an *authoritative*
`google_cloud_run_v2_service_iam_binding` on the `run.invoker` role listing the group
(and, day one, your own `user:`). Authoritative binding ⇒ Terraform **guarantees no
`allUsers`** and corrects drift on every apply. The binding is applied by the
`terraform.yml` Actions workflow (PR = plan, merge = apply) — **never `gcloud
add-iam-policy-binding` from the terminal.**

Thereafter "provide access" is a **group-membership change** (Workspace admin), never an
IAM/Terraform edit:

- Authorized **person** → add `user:someone@example.com` to the group.
- **Service** caller (a new front-end's runtime SA; dash-viewer; later, predictive-models
  callers) → add `serviceAccount:…@….iam.gserviceaccount.com` to the group. (Google
  Groups accept service accounts as members.)
- **Revoke** → remove the member.

The group is what makes "I can provide access" a one-step operation without touching
infra code; Terraform pins *the group* as the sole invoker principal once.

**Human access (easy).** Either:

- Ad-hoc: `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" <url>/health`
- Local dev (nicest): `gcloud run services proxy bgg-warehouse-api --region us-central1`
  then hit `http://localhost:8080` — the proxy injects your identity, no header needed.

**Service-to-service access.** The caller mints a Google-signed **ID token** whose
audience is the target service URL and attaches it as `Authorization: Bearer`. Reusable
helper (copy into each caller repo; ~15 lines, no new heavy deps — `google-auth` is
already present):

```python
import google.auth.transport.requests
import google.oauth2.id_token

def id_token_headers(audience_url: str) -> dict:
    """Bearer header for calling an authenticated Cloud Run service.

    Works off ADC: the Cloud Run metadata server in prod, a service-account context
    locally. `audience_url` is the target service's base URL.
    """
    req = google.auth.transport.requests.Request()
    token = google.oauth2.id_token.fetch_id_token(req, audience_url)
    return {"Authorization": f"Bearer {token}"}
```

The callee needs **no application code** to verify the token — Cloud Run IAM validates
it at the edge before the request reaches the app.

## Applying it here (warehouse read API)

The API is **consumer-agnostic** — it exists to serve any front-end (a new front-end is
planned; dash-viewer is merely the first existing consumer). The group is what keeps it
that way: a new consumer gets access by being added to the group, and the API knows
nothing about who calls it.

1. Deploy `bgg-warehouse-api` `--no-allow-unauthenticated` via the Cloud Build **Actions**
   workflow (not the terminal).
2. Terraform (applied by `terraform.yml`) authoritatively binds `run.invoker` to the
   invoker group **and, day one, your own `user:`** — so the API is usable immediately
   without coupling to any front-end. Each consumer (a new front-end's SA, dash-viewer's
   SA, another service) is then added to the **group** as it comes online — no
   Terraform/API change per consumer.
3. Any consumer builds its authenticated calls with `id_token_headers(WAREHOUSE_API_URL)`.

## Extending to the predictive-models services (follow-up)

The same three moves per service, one service at a time to avoid breakage:

1. Identify every current caller of the service (scoring pipeline, Streamlit, dash-viewer)
   and confirm each can mint an ID token (runs as a service account).
2. Add each caller's identity to the invoker group; add an authoritative Terraform
   `run.invoker` binding for the group on that service (applied via `terraform.yml`).
3. Redeploy the service `--no-allow-unauthenticated` via its Actions workflow (the
   authoritative binding removes `allUsers`), and update each caller to attach
   `id_token_headers(url)`.

**Special case — `bgg-streamlit-prod`:** it's a *browser-facing* dashboard, so
ID-token/`run.invoker` gating doesn't apply (browsers don't send ID tokens). That one
needs **Identity-Aware Proxy (IAP)** or in-app login, and is a separate decision from
the machine-to-machine services.

## What this doesn't change

- No change to how services authenticate to BigQuery/GCS (still ADC / runtime SA).
- The pattern gates *inbound* invocation only.
- Remediating the existing `allUsers` exposure is tracked as its own security follow-up
  (see the plan) — this spec defines the target pattern, it does not itself flip the
  live services.
