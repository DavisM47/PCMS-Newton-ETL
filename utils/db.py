import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import re

def create_sql_engine(config):
    conn = config["connection"]
    params = quote_plus(
        f"DRIVER={{{conn['driver']}}};"
        f"SERVER={conn['server']};"
        f"DATABASE={conn['database']};"
        f"Trusted_Connection={conn['trusted_connection']};"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine

def validate_sql_engine(engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        print("SQL connection ready")
        return True

    except Exception as e:
        print("SQL connection failed:", e)
        return False

def sql_server_busy(engine):

    sql = """
    SELECT COUNT(*) AS running
    FROM sys.dm_exec_requests
    WHERE status = 'running'
    """

    result = pd.read_sql(sql, engine)

    return result["running"][0] > 5

def apply_unit_filter(sql: str, engine, config, key) -> str:
    """
    Apply unit_seqno filter to SQL using WHERE or AND automatically,
    by detecting whether a WHERE clause already exists in the query.
    """
    units = config.get("filters", {}).get("units", [])

    if not units:
        return sql

    unit_clause = ", ".join("'" + str(u).replace("'", "''") + "'" for u in units)
    df = pd.read_sql(f"SELECT unit_seqno FROM dbo.unit WHERE unit_id IN ({unit_clause})", engine)

    if df.empty:
        return sql

    seqnos = df["unit_seqno"].astype(int).tolist()
    seqno_clause = ", ".join(str(x) for x in seqnos)

    # Detect whether the SQL already contains a WHERE clause
    has_where = bool(re.search(r"\bWHERE\b", sql, re.IGNORECASE))
    join_keyword = "AND" if has_where else "WHERE"

    if key == "areas" or key == "units":
        return f"{sql} {join_keyword} dbo.unit.unit_seqno IN ({seqno_clause})"
    else:
        return f"{sql} {join_keyword} dbo.equip.unit_seqno IN ({seqno_clause})"

def get_sql_columns(sql: str) -> list[str]:
    # Normalize whitespace
    sql = " ".join(sql.split())

    # Extract SELECT ... FROM portion
    match = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.IGNORECASE)
    if not match:
        return []

    select_part = match.group(1)

    # Remove DISTINCT
    select_part = re.sub(r"^DISTINCT\s+", "", select_part, flags=re.IGNORECASE)

    columns = []

    for col in select_part.split(","):
        col = col.strip()

        # Handle aliases
        alias_match = re.search(r"\s+AS\s+(.+)$", col, re.IGNORECASE)

        if alias_match:
            columns.append(alias_match.group(1).strip())
        else:
            # Remove table prefix
            columns.append(col.split(".")[-1])

    return columns

