"""Reader for the pipeline monitor.

How fresh each downstream table is, how much of the games it should cover it actually
covers, and which model versions are live. One query for the tables, one for models.
See docs/superpowers/specs/2026-10-02-pipeline-monitor-design.md.
"""

from dataclasses import dataclass
from typing import Any, Optional

from google.cloud import bigquery

from src.warehouse.bq import dataset, get_client

# The daily scoring run's default year range (bgg-predictive-models
# .github/workflows/run-scoring-service.yml). bgg_predictions only covers these years.
SCORING_YEARS = (2025, 2030)


@dataclass(frozen=True)
class TableSpec:
    name: str
    dataset: str
    table: str
    ts_column: str
    universe: str | None = None  # a CTE in _UNIVERSES; None = no coverage
    where: str | None = None
    count_users: bool = False


TABLES = [
    TableSpec("raw.thing_ids", "raw", "thing_ids", "load_timestamp"),
    TableSpec("raw.fetched_responses", "raw", "fetched_responses", "fetch_timestamp",
              universe="boardgame_ids", where="t.fetch_status = 'success'"),
    TableSpec("analytics.games_features", "analytics", "games_features", "load_timestamp"),
    TableSpec("predictions.bgg_description_embeddings", "predictions",
              "bgg_description_embeddings", "created_ts", universe="dated_games"),
    TableSpec("predictions.bgg_complexity_predictions", "predictions",
              "bgg_complexity_predictions", "score_ts", universe="dated_games"),
    TableSpec("predictions.bgg_predictions", "predictions", "bgg_predictions", "score_ts",
              universe="scoring_games"),
    TableSpec("predictions.bgg_game_embeddings", "predictions", "bgg_game_embeddings",
              "created_ts", universe="dated_games"),
    TableSpec("predictions.bgg_game_coordinates", "predictions", "bgg_game_coordinates",
              "created_ts", universe="dated_games"),
    TableSpec("predictions.user_collection_predictions", "predictions",
              "user_collection_predictions", "score_ts", count_users=True),
]


def _universes() -> str:
    features = f"`{dataset('analytics')}.games_features`"
    return f"""
        -- The ML services only embed/score games with a year_published
        -- (bgg-predictive-models services/*/main.py), so that is their universe.
        dated_games AS (
          SELECT DISTINCT game_id FROM {features} WHERE year_published IS NOT NULL
        ),
        scoring_games AS (
          SELECT DISTINCT game_id FROM {features}
          WHERE year_published BETWEEN @year_start AND @year_end
        ),
        boardgame_ids AS (
          SELECT DISTINCT game_id FROM `{dataset('raw')}.thing_ids` WHERE type = 'boardgame'
        )"""


def _select(i: int, t: TableSpec) -> str:
    src = f"`{dataset(t.dataset)}.{t.table}`"
    where = f"WHERE {t.where}" if t.where else ""
    users = "COUNT(DISTINCT t.username)" if t.count_users else "CAST(NULL AS INT64)"
    if t.universe:
        covered = "COUNT(DISTINCT u.game_id)"
        universe = f"(SELECT COUNT(*) FROM {t.universe})"
        join = f"LEFT JOIN {t.universe} u ON u.game_id = t.game_id"
    else:
        covered = universe = "CAST(NULL AS INT64)"
        join = ""
    return f"""
        SELECT {i} AS ord, '{t.name}' AS table_name, MAX(t.{t.ts_column}) AS last_updated,
               COUNT(DISTINCT t.game_id) AS games, {covered} AS covered,
               {universe} AS universe, {users} AS users
        FROM {src} t {join} {where}"""


def fetch_table_status(client: Optional[bigquery.Client] = None) -> list[dict[str, Any]]:
    """One row per monitored table, in ``TABLES`` order."""
    client = client or get_client()
    sql = f"WITH {_universes()}\n" + "\nUNION ALL\n".join(
        _select(i, t) for i, t in enumerate(TABLES)
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("year_start", "INT64", SCORING_YEARS[0]),
            bigquery.ScalarQueryParameter("year_end", "INT64", SCORING_YEARS[1]),
        ]
    )
    rows = sorted(
        (dict(r) for r in client.query(sql, job_config=job_config).result()),
        key=lambda r: r["ord"],
    )
    return [
        {
            "table": r["table_name"],
            "last_updated": r["last_updated"],
            "games": r["games"],
            "covered": r["covered"],
            "universe": r["universe"],
            "users": r["users"],
        }
        for r in rows
    ]


def fetch_deployed_models(client: Optional[bigquery.Client] = None) -> list[dict[str, Any]]:
    """Every model version still serving games, from the ``monitoring.deployed_models``
    table (a dozen rows; Dataform rebuilds it on each pass from the serving tables)."""
    client = client or get_client()
    sql = f"""
        SELECT model_category, model_type, model_name, model_version, experiment,
               algorithm, games_count, last_updated
        FROM `{dataset('monitoring')}.deployed_models`
        ORDER BY model_category, model_type, last_updated DESC
    """
    return [dict(r) for r in client.query(sql).result()]
