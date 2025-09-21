import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import pandas as pd
    import polars as pl

    return Path, pd, pl


@app.cell
def _(Path):
    root = Path(__file__).parent.parent
    data_path = root / "data" / "diamonds.csv"
    return (data_path,)


@app.cell
def _(data_path, pd):
    raw_pd_df = pd.read_csv(data_path, index_col=0)
    raw_pd_df
    return (raw_pd_df,)


@app.cell
def _(raw_pd_df):
    pd_df = raw_pd_df[raw_pd_df.cut == "Ideal"]
    pd_df = pd_df[pd_df.carat > 0.2]
    pd_df.groupby("color", as_index=False)[["price"]].agg(func="mean")
    return


@app.cell
def _(data_path, pl):
    raw_pl_df = pl.read_csv(data_path)
    raw_pl_df
    return (raw_pl_df,)


@app.cell
def _(raw_pl_df):
    raw_pl_df.sql("""
    SELECT color, avg(price)
    FROM self
    WHERE cut = 'Ideal' AND carat > 0.2
    GROUP BY color
    ORDER BY color
    """)
    return


if __name__ == "__main__":
    app.run()
