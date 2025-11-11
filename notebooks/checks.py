# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "duckdb==1.4.1",
#     "marimo",
#     "polars==1.35.1",
#     "pyarrow==22.0.0",
#     "pytest==8.4.2",
#     "sqlglot==27.29.0",
# ]
# ///

import marimo

__generated_with = "0.17.7"
app = marimo.App(width="columns", sql_output="polars")


@app.cell(column=0, hide_code=True)
def _(mo):
    mo.md(r"""
    ## Boilerplate
    """)
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        -- SELECT * FROM "stg_wowlogs_with_sessions" LIMIT 1000
        """,
        engine=engine
    )
    return


@app.cell
def _():
    import duckdb

    DATABASE_URL = "wow_test.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=False)
    return duckdb, engine


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _():
    import pytest
    return (pytest,)


@app.cell
def _():
    return


@app.cell(column=1, hide_code=True)
def _(mo):
    mo.md(r"""
    ## Test against tables
    """)
    return


@app.cell
def _(df_no_bots, pl, pytest):
    @pytest.mark.parametrize("uid", [17528])
    def test_bots_are_caught(uid):
        subset = df_no_bots.filter(pl.col("player_id") == uid)
        assert subset.shape[0] == 0

    @pytest.mark.parametrize("uid", [13796, 53727])
    def test_normies_are_kept(uid):
        subset = df_no_bots.filter(pl.col("player_id") == uid)
        assert subset.shape[0] > 0
    return


@app.cell
def _(engine, mo, stg_wowlogs_no_bots):
    df_no_bots = mo.sql(
        f"""
        SELECT * FROM "stg_wowlogs_no_bots"
        """,
        output=False,
        engine=engine
    )
    return (df_no_bots,)


@app.cell
def _():
    return


@app.cell(column=2, hide_code=True)
def _(mo):
    mo.md(r"""
    ## Aaaand a DuckDB trick
    """)
    return


@app.cell
def _(duckdb, pl):
    from pathlib import Path
    from typing import Dict, Union


    def run_sql_with_mocks(
        sql: str,
        mocks: Dict[str, pl.DataFrame],
        return_polars: bool = True
    ) -> Union[pl.DataFrame, duckdb.DuckDBPyRelation]:
        con = duckdb.connect(":memory:")

        for i, (table_ref, df) in enumerate(mocks.items()):
            var_name = f"mock_{i}"  # Create unique variable name
            sql = sql.replace(table_ref, var_name)
            con.register(var_name, df)

        result = con.execute(sql)

        if return_polars:
            return result.pl()
        else:
            return result


    sql = Path("wow_analytics/target/compiled/wow_analytics/models/staging/stg_wowlogs_no_bots.sql").read_text()
    return run_sql_with_mocks, sql


@app.cell
def _(run_sql_with_mocks, sql, stats, users):
    run_sql_with_mocks(sql, {
        '"wow_test"."main"."stg_wowlogs_with_sessions"': users,
        '"wow_test"."main"."player_session_stats"': stats 
    })
    return


@app.cell
def _(pl):
    stats = pl.DataFrame([{
      "player_id": 83,
      "total_sessions": 60,
      "total_events": 164,
      "avg_session_duration_minutes": 17.83,
      "median_session_duration_minutes": 5.0,
      "max_session_duration_minutes": 120,
      "min_session_duration_minutes": 0,
      "avg_events_per_session": 2.73,
      "max_events_per_session": 13,
      "likely_bot_long_session": 0,
      "likely_bot_avg_session": 0,
      "is_likely_bot": 0
    }])
    return (stats,)


@app.cell
def _(pl):
    users = pl.DataFrame([{
      "player_id": 83,
      "guild": 21.0,
      "level": 25,
      "race": "Tauren",
      "class": "Warrior",
      "where": "Undercity",
      "datetime": "2006-01-01 00:00:44",
      "session_number": "1",
      "session_id": "83-1",
      "minutes_since_last_event": None,
      "is_session_start": 1
    }])
    return (users,)


@app.cell
def _():
    return


@app.cell(column=3)
def _(sql):
    print(sql)
    return


if __name__ == "__main__":
    app.run()
