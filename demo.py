# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "polars==1.35.1",
#     "sqlalchemy==2.0.44",
# ]
# ///
#
# To run with production database:
#   DBT_ENV=prod marimo run demo.py
#
# To run with dev database (default):
#   marimo run demo.py

import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium", sql_output="polars")


@app.cell
def _():
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM churn_analysis LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import json

    with open("../target/manifest.json") as f:
        manifest = json.load(f)

    # Get compiled SQL for a specific model
    model = manifest["nodes"]["model.wow_analytics.stg_players"]
    compiled_sql = model["compiled_code"]
    return (compiled_sql,)


@app.cell
def _(compiled_sql):
    print(compiled_sql)
    return


@app.cell
def _():
    import sqlalchemy
    import os

    # Use environment variable to switch between dev and prod
    # Default to dev (test database) for safety
    ENV = os.getenv("DBT_ENV", "dev")

    if ENV == "prod":
        DATABASE_URL = "duckdb:///wow.duckdb"
        print("🚨 Connected to PRODUCTION database (wow.duckdb)")
    else:
        DATABASE_URL = "duckdb:///wow_test.duckdb"
        print("✅ Connected to DEV database (wow_test.duckdb)")

    engine = sqlalchemy.create_engine(DATABASE_URL)
    return (engine,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
