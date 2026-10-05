"""Refuse SQL that could touch anything outside the scratch dataset.

The proof's safety comes from code, not permissions (user credentials can write
anywhere), so every .sql file goes through check_sql before it is sent.
"""

import re

SCRATCH = "bgg-data-warehouse.scratch_parsing"
FORBIDDEN = ("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "DROP", "ALTER")
CREATE_TARGET = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\s+)?(?:TABLE|VIEW|FUNCTION)\s+(?:IF\s+NOT\s+EXISTS\s+)?(`[^`]+`|[\w.\-]+)",
    re.IGNORECASE,
)


def _strip_comments_and_strings(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"", "''", sql)


def check_sql(sql: str) -> None:
    code = _strip_comments_and_strings(sql)
    for word in FORBIDDEN:
        if re.search(rf"\b{word}\b", code, re.IGNORECASE):
            raise ValueError(f"{word} is not allowed in proof SQL")
    for target in CREATE_TARGET.findall(code):
        name = target.strip("`")
        if not name.startswith(SCRATCH + "."):
            raise ValueError(f"CREATE target {name} is outside scratch_parsing")
