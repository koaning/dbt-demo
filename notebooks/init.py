# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "anthropic==0.72.0",
#     "duckdb==1.4.1",
#     "marimo",
#     "numpy==2.3.4",
#     "polars==1.33.1",
#     "pyarrow==22.0.0",
#     "python-lsp-ruff==2.2.2",
#     "python-lsp-server==1.13.1",
#     "websockets==15.0.1",
# ]
# ///

import marimo

__generated_with = "0.17.7"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    return


@app.cell
def _():
    import polars as pl 

    df = pl.read_parquet("wow-full.parquet")
    return (df,)


@app.cell
def _():
    import duckdb
    _conn = duckdb.connect("wow.duckdb")
    _conn.execute("CREATE OR REPLACE TABLE wowlogs AS SELECT * FROM df")
    return


@app.cell
def _(df):
    df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
