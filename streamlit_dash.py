import streamlit as st
import polars as pl
from typing import Tuple
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def load_data() -> pl.LazyFrame:
    df = pl.scan_parquet(source="./src_adjusted/auto_file/*.parquet")
    # st.text(df.columns)
    df = df.with_columns(
        is_clean=pl.col("VendorID").is_in([1, 2])
        & pl.col("RatecodeID").is_in([1, 2, 3, 4, 5, 6])
        & pl.col("store_and_fwd_flag").is_in(["Y", "N"])
        & pl.col("payment_type").is_in([1, 2, 3, 4, 5, 6])
        & pl.col("trip_distance").gt(0)
        & pl.col("fare_amount").gt(0)
        & pl.col("tpep_pickup_datetime").is_not_null()
        & pl.col("tpep_dropoff_datetime").is_not_null()
        & pl.col("passenger_count").gt(0)
    )
    return df


@st.cache_data
def clean_corrupted_stats(_data: pl.LazyFrame) -> pl.DataFrame:
    df = (
        _data.with_columns(
            pl.col("filename")
            .str.replace(".src/yellow_taxi_", "")
            .str.replace(".parquet", "")
            .str.replace(".", "")
            .alias("period")
        )
        .groupby("is_clean", "period")
        .agg(pl.count().alias("cnt"))
    ).collect()
    return df


def prep_percentages(cc_df: pl.DataFrame) -> pl.DataFrame:
    return cc_df.with_columns(
        (pl.col("cnt") * pl.lit(100) / pl.col("cnt").sum().over("period")).alias(
            "percentage_of_total"
        )
    )


if __name__ == "__main__":
    st.title("Yellow Taxi Zone")
    data = load_data()
    dt = clean_corrupted_stats(data)
    df2 = prep_percentages(dt).filter(pl.col("is_clean") == True).sort("period")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=df2["period"], y=df2["cnt"], name="Clean record counts"),
        secondary_y=False,
    )
    fig.add_trace(
        go.Line(
            x=df2["period"],
            y=df2["percentage_of_total"],
            name="Clean records proportion",
        )            ,secondary_y=True,
    )

    fig.update_layout(
    title_text="Clean records information"
)

# Set x-axis title
fig.update_xaxes(title_text="period")

# Set y-axes titles
fig.update_yaxes(title_text="<b>Clean events count</b>", secondary_y=False, linecolor='#071633')
fig.update_yaxes(title_text="<b>Clean events percentage</b>", secondary_y=True)
st.plotly_chart(fig)
