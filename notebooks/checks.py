# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "duckdb==1.4.1",
#     "polars==1.35.1",
#     "pyarrow==22.0.0",
#     "pytest==8.4.2",
#     "sqlglot==27.29.0",
# ]
# ///

import marimo

__generated_with = "0.17.7"
app = marimo.App(sql_output="polars")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _():
    import duckdb

    DATABASE_URL = "wow_test.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=False)
    return (engine,)


@app.cell
def _(engine, mo, stg_wowlogs_with_sessions):
    _df = mo.sql(
        f"""
        SELECT * FROM "stg_wowlogs_with_sessions" LIMIT 1000
        """,
        engine=engine
    )
    return


@app.cell
def _():
    import pytest
    return (pytest,)


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
        engine=engine
    )
    return (df_no_bots,)


@app.cell
def _(engine, mo, player_session_stats):
    _df = mo.sql(
        f"""
        SELECT * FROM "player_session_stats" LIMIT 100
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
