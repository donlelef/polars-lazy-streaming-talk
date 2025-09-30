import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import polars as pl

    return Path, pl


@app.cell
def _(Path, pl):
    root = Path(__file__).parent.parent
    data_path = root / "data" / "diamonds.csv"
    df = (
        pl.read_csv(data_path)
        .filter(pl.col.cut == "Ideal", pl.col.carat > 0.2)
        .group_by("color")
        .agg(pl.col.price.mean())
        .sort("color")
    )
    df
    return (data_path,)


@app.cell
def _(data_path, pl):
    lazy_df = (
        pl.scan_csv(data_path)
        .filter(pl.col.cut == "Ideal", pl.col.carat > 0.2)
        .group_by("color")
        .agg(pl.col.price.mean())
        .sort("color")
    )
    lazy_df.show_graph(engine="streaming", plan_stage="physical")
    return


@app.cell
def _(data_path, pl):
    (
        pl.scan_csv(data_path)
        .filter(pl.col.cut == "Ideal", pl.col.carat > 0.2)
        .group_by("color")
        .agg(pl.col.price.mean())
        .sort("color")
        .collect()
    )
    return


if __name__ == "__main__":
    app.run()
