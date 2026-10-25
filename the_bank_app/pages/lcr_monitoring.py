"""
LCR Monitoring - FINMA Liquidity Coverage Ratio showcase.

Tabs map to the spec requirements:
  Overview (REQ-UI-01), HQLA & Caps (REQ-ENG-04), Flows & Inflow Cap (REQ-ENG-03),
  Behavioral (REQ-ENG-01/02), Scenarios (REQ-ENG-05), Variance (REQ-ANL-01/03, REQ-UI-03),
  Drill-down (REQ-ANL-02, REQ-UI-02, REQ-PER), Currency (REQ-ING-03), Data Quality (REQ-DQ-01),
  Simulator (what-if), Reporting (SNB monthly summary, alerts).
"""
import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.data_loaders import (
    load_lcr_alerts, load_lcr_monthly_summary, load_hqla_holdings_detail,
    load_lcr_scenario_daily, load_lcr_events, load_lcr_liquidity_gap, load_lcr_cutoff_deltas,
    load_lcr_variance_drivers, load_lcr_drilldown, load_lcr_drilldown_positions,
    load_lcr_exceptions, run_lcr_simulation,
)
from utils.ui_helpers import STATUS_COLORS, kpi_row, page_header

FAIL_LIMIT, PASS_LIMIT = 100.0, 110.0
SCENARIO_LABELS = {"BASE": "Regulatory (BASE)", "BANK_STRESS": "Bank stress",
                   "MARKET_STRESS": "Market stress", "COMBINED": "Combined stress"}
STATUS_COLOR = {k: STATUS_COLORS[k] for k in ("PASS", "WARNING", "FAIL")}


def chf_m(value):
    return f"CHF {value / 1e6:,.0f}M"


def status_of(lcr):
    return "FAIL" if lcr < FAIL_LIMIT else "WARNING" if lcr < PASS_LIMIT else "PASS"


def add_status_bands(fig, y_max):
    """Shade FAIL (<100%) and WARNING (100-110%) bands behind an LCR chart."""
    fig.add_hrect(y0=0, y1=FAIL_LIMIT, fillcolor=STATUS_COLOR["FAIL"], opacity=0.07, line_width=0)
    fig.add_hrect(y0=FAIL_LIMIT, y1=PASS_LIMIT, fillcolor=STATUS_COLOR["WARNING"], opacity=0.08, line_width=0)
    fig.add_hline(y=FAIL_LIMIT, line_dash="dot", line_color=STATUS_COLOR["FAIL"],
                  annotation_text="100% minimum", annotation_position="bottom right")
    fig.add_hline(y=PASS_LIMIT, line_dash="dot", line_color=STATUS_COLOR["WARNING"],
                  annotation_text="110% internal target", annotation_position="top right")
    fig.update_yaxes(range=[max(0, min(80, y_max - 100)), y_max + 10], ticksuffix="%")


def add_event_markers(fig, events, show_labels=True):
    for _, ev in events.iterrows():
        fig.add_vline(x=pd.Timestamp(ev["EVENT_DATE"]), line_dash="dash", line_color="#8E8E93", line_width=1)
        if show_labels:
            fig.add_annotation(x=pd.Timestamp(ev["EVENT_DATE"]), yref="paper", y=1.02, showarrow=False,
                               text=ev["EVENT_ID"], font=dict(size=10, color="#8E8E93"))


page_header("LCR Monitoring", "FINMA Liquidity Coverage Ratio (LiqV / Circ. 2015/2) - regulatory LCR, stress scenarios, "
            "root-cause analysis and what-if simulation")

scen = load_lcr_scenario_daily()
if scen.empty:
    st.warning("No LCR data available. Run the LCR engine refresh (operation/execute_all_tasks_and_refresh_dts.sql).")
    st.stop()

scen["AS_OF_DATE"] = pd.to_datetime(scen["AS_OF_DATE"])
base = scen[scen["STRESS_SCENARIO"] == "BASE"].sort_values("AS_OF_DATE").reset_index(drop=True)
events = load_lcr_events()
dates = list(base["AS_OF_DATE"].dt.date)

as_of = st.select_slider("Reporting date", options=dates, value=dates[-1],
                         help="All tabs show the selected cut-off date; the trend shows the full history.")
cur = base[base["AS_OF_DATE"].dt.date == as_of].iloc[0]
prev = base[base["AS_OF_DATE"].dt.date < as_of].tail(1)
dod = float(cur["LCR_RATIO"] - prev["LCR_RATIO"].iloc[0]) if len(prev) else 0.0
status = status_of(cur["LCR_RATIO"])

kpi_row([
    ("LCR", f"{cur['LCR_RATIO']:.0f}%", f"{dod:+.1f}pp", "normal"),
    ("Status", status, None),
    ("HQLA (CHF)", f"{cur['HQLA_TOTAL'] / 1e6:,.0f}M", None),
    ("Outflows (CHF)", f"{cur['NET_OUTFLOW'] / 1e6:,.0f}M", None),
    ("Buffer (CHF)", f"{cur['LCR_BUFFER_CHF'] / 1e6:,.0f}M", None),
    ("Days < 100%", f"{int((base['LCR_RATIO'] < FAIL_LIMIT).sum())}", f"of {len(base)} days", "off"),
])

if status == "FAIL":
    st.error("LCR below the 100% regulatory minimum - FINMA notification and remediation plan required.")
elif status == "WARNING":
    st.warning("LCR between 100% and 110% - below the internal buffer target.")

tabs = st.tabs(["Overview", "HQLA & Caps", "Flows & Inflow Cap", "Behavioral", "Scenarios", "Variance",
                "Drill-down", "Currency", "Data Quality", "Simulator", "Reporting"])

# ---------------------------------------------------------------- Overview
with tabs[0]:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=base["AS_OF_DATE"], y=base["LCR_RATIO"], mode="lines", name="LCR",
                             line=dict(width=2.5, color="#007AFF")))
    fails = base[base["LCR_RATIO"] < FAIL_LIMIT]
    fig.add_trace(go.Scatter(x=fails["AS_OF_DATE"], y=fails["LCR_RATIO"], mode="markers", name="Breach",
                             marker=dict(color=STATUS_COLOR["FAIL"], size=7)))
    add_status_bands(fig, float(base["LCR_RATIO"].max()))
    add_event_markers(fig, events)
    fig.update_layout(height=420, margin=dict(t=40, b=10), legend=dict(orientation="h", y=-0.15),
                      title="Regulatory LCR - 90 days with status bands and liquidity events")
    st.plotly_chart(fig, width="stretch")
    if not events.empty:
        ev = events.copy()
        ev["AMOUNT_CHF"] = ev["AMOUNT_CHF"].map(chf_m)
        st.dataframe(ev[["EVENT_ID", "EVENT_DATE", "EVENT_TYPE", "DESCRIPTION", "AMOUNT_CHF"]],
                     hide_index=True, width="stretch")

# ---------------------------------------------------------------- HQLA & caps
with tabs[1]:
    l2b_disc = cur["L2B_TOTAL"] - cur["L2B_CAPPED"]
    l2_disc = cur["L2A_TOTAL"] + cur["L2B_CAPPED"] - cur["L2_CAPPED"]
    fig = go.Figure()
    for name, val, color in [("Level 1", cur["L1_TOTAL"], "#007AFF"), ("Level 2A", cur["L2A_TOTAL"], "#5AC8FA"),
                             ("Level 2B (after 15% cap)", cur["L2B_CAPPED"], "#AF52DE"),
                             ("Discarded by caps", l2b_disc + l2_disc, "#C7C7CC")]:
        fig.add_trace(go.Bar(x=["HQLA stock"], y=[val / 1e6], name=name, marker_color=color))
    fig.add_hline(y=(cur["L1_TOTAL"] + 2 / 3 * cur["L1_TOTAL"]) / 1e6, line_dash="dot",
                  annotation_text="L2 40% cap (L1 + 2/3 x L1)")
    fig.update_layout(barmode="stack", height=380, yaxis_title="CHF M", title="HQLA composition and regulatory caps")
    c1, c2 = st.columns([1, 1])
    c1.plotly_chart(fig, width="stretch")
    with c2:
        l2_share = cur["L2_CAPPED"] / cur["HQLA_TOTAL"] * 100 if cur["HQLA_TOTAL"] else 0
        l2b_room = 15 / 85 * (cur["L1_TOTAL"] + cur["L2A_TOTAL"]) - cur["L2B_TOTAL"]
        st.metric("Level 2 share of HQLA", f"{l2_share:.1f}%", "max 40%", delta_color="off")
        st.metric("Headroom to L2B 15% cap", chf_m(l2b_room))
        st.metric("Headroom to L2 40% cap", chf_m(2 / 3 * cur["L1_TOTAL"] - (cur["L2A_TOTAL"] + cur["L2B_CAPPED"])))
        st.caption("L2B capped at 15/85 of (L1 + L2A); L2 capped at 2/3 of L1 (FINMA LiqV Annex 2).")
    hq = load_hqla_holdings_detail()
    if not hq.empty:
        st.dataframe(hq, hide_index=True, width="stretch")

# ---------------------------------------------------------------- Flows
with tabs[2]:
    c1, c2 = st.columns(2)
    seg = pd.DataFrame({"Segment": ["Retail", "Corporate", "Financial institutions", "Committed facilities"],
                        "Outflow": [cur["OUTFLOW_RETAIL"], cur["OUTFLOW_CORP"], cur["OUTFLOW_FI"], cur["OUTFLOW_FACILITIES"]]})
    fig = px.bar(seg, x="Outflow", y="Segment", orientation="h", title="Stressed 30-day outflows by segment")
    fig.update_traces(marker_color="#FF9F0A")
    fig.update_layout(height=360, xaxis_title="CHF")
    c1.plotly_chart(fig, width="stretch")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=base["AS_OF_DATE"], y=base["INFLOW_TOTAL"] / 1e6, name="Weighted inflows", line_color="#34C759"))
    fig.add_trace(go.Scatter(x=base["AS_OF_DATE"], y=0.75 * base["OUTFLOW_TOTAL"] / 1e6, name="75% inflow cap",
                             line=dict(dash="dot", color="#FF3B30")))
    fig.update_layout(height=360, yaxis_title="CHF M", title="Inflows vs. 75% inflow cap",
                      legend=dict(orientation="h", y=-0.2))
    c2.plotly_chart(fig, width="stretch")
    cols = st.columns(4)
    cols[0].metric("Gross outflows", chf_m(cur["OUTFLOW_TOTAL"]))
    cols[1].metric("Inflows (weighted)", chf_m(cur["INFLOW_TOTAL"]))
    cols[2].metric("Inflows after cap", chf_m(cur["INFLOW_CAPPED"]))
    cols[3].metric("Cap utilisation", f"{cur['INFLOW_TOTAL'] / (0.75 * cur['OUTFLOW_TOTAL']) * 100:.0f}%")

# ---------------------------------------------------------------- Behavioral
with tabs[3]:
    gap = load_lcr_liquidity_gap(as_of)
    if gap.empty:
        st.info("No liquidity gap data for this date.")
    else:
        gap["Scenario"] = gap["STRESS_SCENARIO"].map(SCENARIO_LABELS)
        order = gap.sort_values("SORT_ORDER")["BUCKET_NAME"].unique().tolist()
        heat = gap.pivot_table(index="Scenario", columns="BUCKET_NAME", values="BEHAVIORAL_OUTFLOW_CHF")[order] / 1e6
        fig = px.imshow(heat, text_auto=".0f", aspect="auto", color_continuous_scale="Oranges",
                        title="Behavioral deposit run-down by maturity bucket (CHF M)")
        fig.update_layout(height=320)
        st.plotly_chart(fig, width="stretch")
        b = gap[gap["STRESS_SCENARIO"] == "BASE"].sort_values("SORT_ORDER")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=b["BUCKET_NAME"], y=b["NET_GAP_CHF"] / 1e6, name="Net gap",
                             marker_color=["#34C759" if v >= 0 else "#FF3B30" for v in b["NET_GAP_CHF"]]))
        fig.add_trace(go.Scatter(x=b["BUCKET_NAME"], y=b["CUMULATIVE_GAP_CHF"] / 1e6, name="Cumulative gap incl. HQLA",
                                 mode="lines+markers", line_color="#007AFF"))
        fig.update_layout(height=360, yaxis_title="CHF M", title="Maturity ladder (BASE): contractual + behavioral",
                          legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig, width="stretch")

# ---------------------------------------------------------------- Scenarios
with tabs[4]:
    s = scen.copy()
    s["Scenario"] = s["STRESS_SCENARIO"].map(SCENARIO_LABELS)
    fig = px.line(s, x="AS_OF_DATE", y="LCR_RATIO", color="Scenario", title="LCR by scenario")
    add_status_bands(fig, float(s["LCR_RATIO"].max()))
    add_event_markers(fig, events, show_labels=False)
    fig.update_layout(height=420, legend=dict(orientation="h", y=-0.15))
    st.plotly_chart(fig, width="stretch")
    today = s[s["AS_OF_DATE"].dt.date == as_of][["Scenario", "LCR_RATIO", "LCR_STATUS", "HQLA_TOTAL", "NET_OUTFLOW", "LCR_BUFFER_CHF"]]
    st.dataframe(today, hide_index=True, width="stretch")

# ---------------------------------------------------------------- Variance
with tabs[5]:
    comparison = st.segmented_control("Comparison", ["DOD", "WOW", "MOM"], default="DOD", key="lcr_cmp")
    deltas = load_lcr_cutoff_deltas(as_of, comparison or "DOD")
    drivers = load_lcr_variance_drivers(as_of, comparison or "DOD")
    if deltas.empty:
        st.info("No prior date available for this comparison.")
    else:
        total = deltas[deltas["ASSET_CLASS"] == "TOTAL"].iloc[0]
        cols = st.columns(3)
        cols[0].metric("LCR prior", f"{total['LCR_PRIOR']:.1f}%", str(total["PRIOR_DATE"]), delta_color="off")
        cols[1].metric("LCR now", f"{total['LCR_RATIO']:.1f}%")
        cols[2].metric("Change", f"{total['LCR_DELTA_PP']:+.2f}pp")
        if not drivers.empty:
            d = drivers.copy()
            d["Driver"] = d["COUNTERPARTY_NAME"] + " - " + d["POSITION_ID"]
            fig = px.bar(d.sort_values("CLIENT_IMPACT_PP"), x="CLIENT_IMPACT_PP", y="Driver", orientation="h",
                         color=d.sort_values("CLIENT_IMPACT_PP")["CLIENT_IMPACT_PP"] < 0,
                         color_discrete_map={True: STATUS_COLOR["FAIL"], False: STATUS_COLOR["PASS"]},
                         title="Top-10 client drivers (funding-adjusted impact, pp)")
            fig.update_layout(height=420, showlegend=False, xaxis_title="pp")
            st.plotly_chart(fig, width="stretch")
            st.dataframe(drivers, hide_index=True, width="stretch")
        st.dataframe(deltas[deltas["ASSET_CLASS"] != "TOTAL"], hide_index=True, width="stretch")

# ---------------------------------------------------------------- Drill-down
with tabs[6]:
    c1, c2 = st.columns([1, 3])
    dd_scen = c1.selectbox("Scenario", list(SCENARIO_LABELS), format_func=SCENARIO_LABELS.get, key="dd_scen")
    dd, secs = load_lcr_drilldown(as_of, dd_scen)
    if dd.empty:
        st.info("No drill-down data.")
    else:
        bu = c2.multiselect("Business unit", sorted(dd["BUSINESS_UNIT"].dropna().unique()), key="dd_bu")
        view = dd[dd["BUSINESS_UNIT"].isin(bu)] if bu else dd
        view = view[view["WEIGHTED_CHF"] > 0]
        fig = px.treemap(view, path=[px.Constant("LCR book"), "ASSET_CLASS", "PRODUCT_GROUP", "COUNTERPARTY_NAME"],
                         values="WEIGHTED_CHF", color="LCR_TREATMENT",
                         title="Weighted LCR amounts: asset class > product group > counterparty")
        fig.update_layout(height=520, margin=dict(t=40, l=0, r=0, b=0))
        st.plotly_chart(fig, width="stretch")
        st.caption(f"{int(dd['POSITIONS'].sum()):,} positions aggregated in {secs:.2f}s (first load; cached afterwards).")
        ac = st.selectbox("Contract level for asset class", sorted(view["ASSET_CLASS"].unique()), key="dd_ac")
        st.dataframe(load_lcr_drilldown_positions(as_of, dd_scen, ac), hide_index=True, width="stretch")

# ---------------------------------------------------------------- Currency
with tabs[7]:
    dd, _ = load_lcr_drilldown(as_of, "BASE")
    if not dd.empty:
        ccy = dd.groupby(["CURRENCY", "LCR_TREATMENT"], as_index=False)["WEIGHTED_CHF"].sum()
        fig = px.bar(ccy, x="CURRENCY", y="WEIGHTED_CHF", color="LCR_TREATMENT", barmode="group",
                     title="Weighted LCR amounts by currency (CHF, end-of-day FX)")
        fig.update_layout(height=400, yaxis_title="CHF")
        st.plotly_chart(fig, width="stretch")
        pivot = ccy.pivot_table(index="CURRENCY", columns="LCR_TREATMENT", values="WEIGHTED_CHF", fill_value=0)
        if {"HQLA", "OUTFLOW"} <= set(pivot.columns):
            pivot["HQLA / outflows"] = (pivot["HQLA"] / pivot["OUTFLOW"].where(pivot["OUTFLOW"] > 0)).round(2)
        st.dataframe(pivot, width="stretch")

# ---------------------------------------------------------------- Data quality
with tabs[8]:
    exc = load_lcr_exceptions(as_of)
    if exc.empty:
        st.success("No quarantined records for this date.")
    else:
        cols = st.columns(len(exc["ERROR_CODE"].unique()) + 1)
        cols[0].metric("Quarantined records", len(exc))
        for col, (code, n) in zip(cols[1:], exc["ERROR_CODE"].value_counts().items()):
            col.metric(code, n)
        st.caption("Invalid records are excluded from all LCR figures and listed here for remediation (REQ-DQ-01).")
        st.dataframe(exc, hide_index=True, width="stretch")

# ---------------------------------------------------------------- Simulator
with tabs[9]:
    st.caption("Recalculates the regulatory LCR with the official formula (L2B 15% cap, L2 40% cap, 75% inflow cap). "
               "Levers apply on top of the selected preset.")
    preset = st.segmented_control("Preset", list(SCENARIO_LABELS), default="BASE",
                                  format_func=SCENARIO_LABELS.get, key="sim_preset") or "BASE"
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Deposit run-off**")
        m_rs = st.slider("Retail stable x", 0.5, 3.0, 1.0, 0.1)
        m_rls = st.slider("Retail less stable x", 0.5, 3.0, 1.0, 0.1)
        m_co = st.slider("Corporate operational x", 0.5, 3.0, 1.0, 0.1)
        m_cn = st.slider("Corporate non-operational x", 0.5, 2.5, 1.0, 0.1)
    with c2:
        st.markdown("**Funding & flows**")
        m_fi = st.slider("Financial institutions x", 0.5, 1.0, 1.0, 0.05)
        m_fac = st.slider("Facility drawdowns x", 0.5, 4.0, 1.0, 0.1)
        m_in = st.slider("Inflows x", 0.0, 1.5, 1.0, 0.05)
        cap = st.slider("Inflow cap", 0.0, 0.75, 0.75, 0.05)
    with c3:
        st.markdown("**HQLA & shocks**")
        a_l1 = st.slider("L1 haircut add-on (pp)", 0, 20, 0)
        a_l2a = st.slider("L2A haircut add-on (pp)", 0, 40, 0)
        a_l2b = st.slider("L2B haircut add-on (pp)", 0, 40, 0)
        shock = st.slider("Market price shock (%)", 0, 30, 0)
        wd = st.number_input("Corporate withdrawal (CHF M)", 0, 2000, 0, 10)
    params = {"preset": preset, "retail_stable_mult": m_rs, "retail_less_stable_mult": m_rls,
              "corp_operational_mult": m_co, "corp_non_operational_mult": m_cn, "fi_mult": m_fi,
              "facilities_mult": m_fac, "inflow_mult": m_in, "inflow_cap_pct": cap,
              "haircut_addon_l1_pp": a_l1, "haircut_addon_l2a_pp": a_l2a, "haircut_addon_l2b_pp": a_l2b,
              "price_shock_pct": shock, "withdrawal_chf": wd * 1e6}
    sim = run_lcr_simulation(as_of, json.dumps(params, sort_keys=True))
    if not sim.empty:
        sim.columns = [c.strip('"').upper() for c in sim.columns]  # procedure result columns arrive quoted
        first, last = sim.iloc[0], sim.iloc[-1]
        cols = st.columns(4)
        cols[0].metric("Baseline LCR", f"{first['LCR_RATIO']:.1f}%")
        cols[1].metric("Simulated LCR", f"{last['LCR_RATIO']:.1f}%", f"{last['LCR_RATIO'] - first['LCR_RATIO']:+.1f}pp")
        cols[2].metric("Status", last["LCR_STATUS"])
        be = last["BREAKEVEN_WITHDRAWAL_CHF"]
        cols[3].metric("Extra withdrawal to 100%", chf_m(be) if pd.notna(be) else "n/a")
        steps = sim[sim["STEP_ORDER"] > 0]
        fig = go.Figure(go.Waterfall(
            x=["Baseline"] + steps["STEP_NAME"].tolist() + ["Simulated"],
            measure=["absolute"] + ["relative"] * len(steps) + ["total"],
            y=[first["LCR_RATIO"]] + steps["DELTA_PP"].fillna(0).tolist() + [0],
            decreasing={"marker": {"color": STATUS_COLOR["FAIL"]}}, increasing={"marker": {"color": STATUS_COLOR["PASS"]}},
            totals={"marker": {"color": "#007AFF"}}))
        fig.add_hline(y=FAIL_LIMIT, line_dash="dot", line_color=STATUS_COLOR["FAIL"])
        fig.update_layout(height=420, yaxis_ticksuffix="%", title="What-if waterfall")
        st.plotly_chart(fig, width="stretch")

# ---------------------------------------------------------------- Reporting
with tabs[10]:
    monthly = load_lcr_monthly_summary()
    if not monthly.empty:
        st.subheader("Monthly SNB summary")
        st.dataframe(monthly, hide_index=True, width="stretch")
    alerts = load_lcr_alerts()
    st.subheader("Alerts (latest date)")
    if alerts.empty:
        st.success("No active alerts.")
    else:
        st.dataframe(alerts, hide_index=True, width="stretch")
