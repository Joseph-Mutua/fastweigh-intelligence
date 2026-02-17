from __future__ import annotations

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from ops_intelligence.config import load_config

st.set_page_config(page_title="Fast-Weigh Operations Intelligence", layout="wide")
st.title("Operations Intelligence Pack")

cfg = load_config()
try:
    conn = duckdb.connect(cfg.warehouse_path, read_only=True)
except duckdb.IOException:
    st.warning(
        "Warehouse database not found yet. Run `ops-intel pipeline` first, then refresh this page."
    )
    st.stop()


@st.cache_data(ttl=120)
def read_df(sql: str) -> pd.DataFrame:
    return conn.execute(sql).df()


plant_df = read_df("SELECT * FROM gold_plant_ops_daily ORDER BY service_date DESC")
dispatch_df = read_df("SELECT * FROM gold_dispatch_daily ORDER BY service_date DESC")
billing_df = read_df("SELECT * FROM gold_billing_ar_daily ORDER BY as_of_date DESC")
hauler_df = read_df("SELECT * FROM gold_hauler_productivity_daily ORDER BY service_date DESC")

tab1, tab2, tab3, tab4 = st.tabs(["Plant Ops", "Dispatch", "Billing/AR", "Hauler Productivity"])

with tab1:
    st.subheader("Scale + In-Yard Efficiency")
    if plant_df.empty:
        st.info("No plant KPI data available yet.")
    else:
        latest = plant_df.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Tickets", f"{int(latest['tickets_count'])}")
        c2.metric("Avg Time In Yard (min)", f"{latest['avg_time_in_yard_minutes']:.1f}")
        c3.metric("Tickets / Lane Hour", f"{latest['tickets_per_lane_hour']:.2f}")

        fig = px.line(
            plant_df,
            x="service_date",
            y="avg_time_in_yard_minutes",
            color="location_id",
            title="Time-in-Yard Trend",
        )
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Dispatch Delivery Performance")
    if dispatch_df.empty:
        st.info("No dispatch KPI data available yet.")
    else:
        latest = dispatch_df.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Deliveries", f"{int(latest['deliveries'])}")
        c2.metric("On-Time Rate", f"{latest['on_time_delivery_rate'] * 100:.1f}%")
        c3.metric("Avg Delivery Minutes", f"{latest['avg_delivery_minutes']:.1f}")

        fig = px.bar(
            dispatch_df,
            x="service_date",
            y="on_time_delivery_rate",
            color="location_id",
            title="On-Time Delivery Rate",
        )
        st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.subheader("Billing / AR Visibility")
    if billing_df.empty:
        st.info("No AR KPI data available yet.")
    else:
        latest = billing_df.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Open AR", f"${latest['total_open_ar']:,.2f}")
        c2.metric("AR 90+", f"${latest['ar_90_plus']:,.2f}")
        c3.metric("Customers With Open AR", f"{int(latest['customers_with_open_ar'])}")

        ar_series = billing_df[["as_of_date", "ar_current", "ar_1_30", "ar_31_60", "ar_61_90", "ar_90_plus"]]
        fig = px.area(
            ar_series.melt(id_vars=["as_of_date"], var_name="bucket", value_name="amount"),
            x="as_of_date",
            y="amount",
            color="bucket",
            title="AR Aging Buckets",
        )
        st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.subheader("Hauler Productivity + Pay Accuracy")
    if hauler_df.empty:
        st.info("No hauler KPI data available yet.")
    else:
        c1, c2 = st.columns(2)
        c1.dataframe(
            hauler_df[["service_date", "hauler_id", "loads_completed", "active_delivery_minutes"]],
            use_container_width=True,
        )
        fig = px.scatter(
            hauler_df,
            x="loads_completed",
            y="pay_variance_pct",
            color="hauler_id",
            title="Load Volume vs Pay Variance %",
        )
        c2.plotly_chart(fig, use_container_width=True)
