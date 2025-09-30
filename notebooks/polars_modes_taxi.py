import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
    # Polars Query Modes

    Welcome! In this notebook, we’ll dive into two powerful features of Polars: lazy queries and streaming mode.

    For more details and the theoretical background, please refer to the slides from the talk "Advanced Polars: Lazy Query and Streaming Mode."

    Let’s begin by importing `polars` and other helpful libraries, and setting up the data paths.
    """
    )
    return


@app.cell
def _():
    import time
    import polars as pl
    import marimo as mo
    from pathlib import Path

    return Path, mo, pl, time


@app.cell
def _(Path):
    data_dir = Path(__file__).parent.parent / "data"
    taxi_path = data_dir / "yellow_tripdata_*.parquet"
    zone_path = data_dir / "taxi_zone_lookup.csv"
    return taxi_path, zone_path


@app.cell
def _(mo):
    mo.md(
        """
    The data comes from the [NYC Taxi public dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

    For convenience, a snapshot is also included in this repository.

    We will work with two datasets:

    * **Rides** — stored across multiple Parquet files
    * **Zone lookup** — a reference file for location codes, stored in a CSV

    The first few rows of each dataset are shown below.
    """
    )
    return


@app.cell
def _(pl, zone_path):
    pl.read_csv(zone_path).head()
    return


@app.cell
def _(pl, taxi_path):
    pl.read_parquet(taxi_path).head()
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Eager Mode

    In eager mode, the query is executed line by line, with no optimisations applied.

    Additionally, the entire dataset is loaded into the machine's physical memory.

    This approach is inefficient for large datasets.
    """
    )
    return


@app.cell
def _(pl, taxi_path, zone_path):
    (
        pl.read_parquet(taxi_path)
        .join(pl.read_csv(zone_path), left_on="PULocationID", right_on="LocationID")
        .filter(pl.col("total_amount") > 25)
        .with_columns(
            duration=(
                pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")
            ).dt.total_minutes()
        )
        .filter(pl.col("duration") > 0)
        .group_by("Zone")
        .agg(
            (pl.col("total_amount") / pl.col("duration"))
            .mean()
            .alias("cost_per_minute")
        )
        .sort("cost_per_minute", descending=True)
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
    Let's create a query and a `QueryOptFlags` object representing no optimisations.

    The query returns a `LazyFrame`, which doesn't hold data directly, but instead represents a query execution plan.

    The data is not processed until the `collect` method is called.
    """
    )
    return


@app.cell
def _(pl, taxi_path, zone_path):
    no_optimization = pl.QueryOptFlags()
    no_optimization.no_optimizations()
    query = (
        pl.scan_parquet(taxi_path)
        .join(pl.scan_csv(zone_path), left_on="PULocationID", right_on="LocationID")
        .filter(pl.col("total_amount") > 25)
        .with_columns(
            duration=(
                pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")
            ).dt.total_minutes()
        )
        .filter(pl.col("duration") > 0)
        .group_by("Zone")
        .agg(
            (pl.col("total_amount") / pl.col("duration"))
            .mean()
            .alias("cost_per_minute")
        )
        .sort("cost_per_minute", descending=True)
    )
    return no_optimization, query


@app.cell
def _(mo):
    mo.md(
        r"""
    Now, let's run the query without any optimisations and measure the elapsed time.

    Please keep in mind that the efficiency provided by Polars' Rust engine and Apache Arrow is still being harnessed.
    """
    )
    return


@app.cell
def _(no_optimization, query, time):
    no_optimization_tic = time.perf_counter()
    query.collect(optimizations=no_optimization)
    no_optimization_toc = time.perf_counter()
    print(f"Elapsed time: {no_optimization_toc - no_optimization_tic} seconds")
    return


@app.cell
def _(mo):
    mo.md(r"""3 seconds is not bad, but we can do much better.""")
    return


@app.cell
def _(query, time):
    tic = time.perf_counter()
    query.collect()
    toc = time.perf_counter()
    print(f"Elapsed time: {toc - tic} seconds")
    return


@app.cell
def _(mo):
    mo.md(
        """
    By running the query with all optimisations enabled, we reduce the execution time to 0.6 seconds, achieving an 80% improvement over the unoptimized query.

    In the following cells, we will compare the execution graphs of both queries and highlight how projections and filters are pushed down to the data reading operations. This ensures that the query performs the minimum number of operations possible.

    For more details on these optimisations, refer to the slides of the talk.
    """
    )
    return


@app.cell
def _(query):
    query.show_graph(optimized=False)
    return


@app.cell
def _(query):
    query.show_graph(optimized=True)
    return


@app.cell
def _(query):
    query.show_graph(optimized=True, engine="streaming", plan_stage="physical")
    return


if __name__ == "__main__":
    app.run()
