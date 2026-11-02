"""Streamlit dashboard. Run the pipeline first, then:  streamlit run dashboard/app.py"""
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

OUT = Path(__file__).resolve().parents[1] / "outputs"
DOW = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}

st.set_page_config(page_title="NYC Taxi Analytics", layout="wide")
st.title("NYC Taxi Trip Analytics and Demand Prediction")


def load(name):
    p = OUT / f"{name}.csv"
    return pd.read_csv(p) if p.exists() else None


if not (OUT / "hourly_pattern.csv").exists():
    st.warning("No outputs found. Run `python run_pipeline.py` first.")
    st.stop()

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Demand patterns", "Zones", "Fares and tips", "Model results", "Data quality and scaling"])

with tab1:
    hp = load("hourly_pattern")
    hp["day"] = hp["dow"].map(DOW)
    pivot = hp.pivot(index="day", columns="hour", values="avg_pickups").reindex(list(DOW.values()))
    st.plotly_chart(px.imshow(pivot, aspect="auto", color_continuous_scale="YlOrRd",
                              labels=dict(x="Hour of day", y="Day", color="Avg pickups"),
                              title="Average city-wide pickups: weekday x hour"), use_container_width=True)
    mt = load("monthly_trend")
    st.plotly_chart(px.line(mt, x="pickup_month", y="trips", markers=True, title="Trips per month"),
                    use_container_width=True)

with tab2:
    zp = load("zone_pickups")
    n = st.slider("Top N zones", 5, 30, 15)
    st.plotly_chart(px.bar(zp.head(n), x="pickups", y="Zone", color="Borough", orientation="h",
                           title="Busiest pickup zones").update_yaxes(autorange="reversed"),
                    use_container_width=True)
    st.plotly_chart(px.bar(zp.groupby("Borough", as_index=False)["pickups"].sum(), x="Borough", y="pickups",
                           title="Pickups by borough"), use_container_width=True)

with tab3:
    c1, c2 = st.columns(2)
    c1.plotly_chart(px.line(load("fare_vs_distance"), x="dist_bin", y="avg_fare", markers=True,
                            title="Average fare by trip distance (miles)"), use_container_width=True)
    c2.plotly_chart(px.line(load("tips_by_hour"), x="pickup_hour", y="avg_tip_pct", markers=True,
                            title="Average tip % by hour (card payments)"), use_container_width=True)
    st.subheader("Airport vs other trips")
    st.dataframe(load("airport_vs_other"), use_container_width=True)

with tab4:
    mr = load("model_results")
    if mr is None:
        st.info("Run the pipeline without --skip-models to see model results.")
    else:
        st.dataframe(mr, use_container_width=True)
        st.plotly_chart(px.bar(mr, x="model", y="rmse", title="RMSE by model (lower is better)"),
                        use_container_width=True)
        fi = load("feature_importance")
        if fi is not None:
            st.plotly_chart(px.bar(fi, x="importance", y="feature", orientation="h",
                                   title="Feature importance (best tree model)").update_yaxes(autorange="reversed"),
                            use_container_width=True)
        pr = load("predictions")
        if pr is not None:
            zone = st.selectbox("Zone", sorted(pr["PULocationID"].unique()))
            d = pr[pr["PULocationID"] == zone].melt(id_vars=["PULocationID", "hour_ts"],
                                                    value_vars=["pickups", "prediction"])
            st.plotly_chart(px.line(d, x="hour_ts", y="value", color="variable",
                                    title=f"Actual vs predicted pickups, zone {zone}"), use_container_width=True)

with tab5:
    cr = load("cleaning_report")
    if cr is not None:
        st.subheader("Rows failing each cleaning rule")
        st.dataframe(cr, use_container_width=True)
    bm = load("benchmark")
    if bm is not None:
        st.plotly_chart(px.line(bm.melt(id_vars="months"), x="months", y="value", color="variable", markers=True,
                                title="Runtime: Pandas vs Spark", labels={"value": "seconds"}),
                        use_container_width=True)
    else:
        st.info("Run `python -m src.benchmark` to add the Pandas vs Spark comparison.")
