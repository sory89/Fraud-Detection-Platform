"""
Fraud Detection Platform — Streamlit Dashboard
Reads directly from PostgreSQL + Redis
"""

import json
import os
import time
from decimal import Decimal

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import redis
import requests
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
DB_DSN   = os.getenv("DB_DSN",   "host=postgres port=5432 dbname=fraud user=fraud password=fraud")
REDIS_URL= os.getenv("REDIS_URL","redis://redis:6379/0")
API_BASE = os.getenv("API_BASE", "http://api:8000")

PLOTLY = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#161b22",
    font_color="#e6edf3",
    font_family="Inter, sans-serif",
    margin=dict(l=0, r=0, t=36, b=0),
    title_font_size=13,
)
GREEN  = "#23c45e"
RED    = "#f85149"
YELLOW = "#e3b341"
BLUE   = "#388bfd"

DARK_CSS = """
<style>
html,body,[data-testid="stAppViewContainer"],[data-testid="stAppViewBlockContainer"],.main{background-color:#0d1117!important;color:#e6edf3!important;}
.main .block-container{background:#0d1117!important;padding:1.5rem 2rem!important;max-width:100%!important;}
section[data-testid="stSidebar"],section[data-testid="stSidebar"]>div{background:#0d1117!important;border-right:1px solid #21262d!important;}
section[data-testid="stSidebar"] *{color:#e6edf3!important;}
[data-testid="metric-container"]{background:#161b22!important;border:1px solid #21262d!important;border-radius:8px!important;padding:1rem!important;}
[data-testid="stMetricLabel"]{font-size:.72rem!important;color:#8b949e!important;text-transform:uppercase;letter-spacing:.06em;}
[data-testid="stMetricValue"]{font-size:1.6rem!important;font-weight:700!important;color:#e6edf3!important;}
.stTabs [data-baseweb="tab-list"]{background:transparent!important;border-bottom:1px solid #21262d!important;}
.stTabs [data-baseweb="tab"]{background:transparent!important;color:#8b949e!important;border:none!important;border-bottom:2px solid transparent!important;font-size:.84rem!important;}
.stTabs [aria-selected="true"]{color:#23c45e!important;border-bottom-color:#23c45e!important;}
[data-testid="stDataFrame"]{border:1px solid #21262d!important;border-radius:8px!important;}
.stButton>button{background:transparent!important;border:1px solid #21262d!important;color:#e6edf3!important;border-radius:6px!important;}
h1{font-size:1.8rem!important;font-weight:700!important;color:#e6edf3!important;}
h2,h3{color:#e6edf3!important;font-weight:600!important;}
hr{border-color:#21262d!important;}
#MainMenu,footer,header{visibility:hidden!important;}
</style>
"""

st.set_page_config(page_title="Fraud Platform", page_icon="🛡️",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(DARK_CSS, unsafe_allow_html=True)


# ── DB / Redis helpers ────────────────────────────────────────────────────────
@st.cache_resource
def get_db():
    return psycopg2.connect(DB_DSN)


@st.cache_resource
def get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)


def query_df(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        df = pd.DataFrame(rows)
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].apply(
                lambda x: float(x) if isinstance(x, Decimal) else x
            )
        return df
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()


def query_val(sql: str, default=0):
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(sql)
            r = cur.fetchone()
        return r[0] if r and r[0] is not None else default
    except Exception:
        return default


def api_health() -> bool:
    try:
        return requests.get(f"{API_BASE}/health", timeout=2).status_code == 200
    except Exception:
        return False


def api_score(payload: dict) -> dict | None:
    try:
        r = requests.post(f"{API_BASE}/score", json=payload, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        st.markdown('<div style="font-size:1rem;font-weight:600;padding-bottom:.8rem">🛡️ Fraud <span style="color:#23c45e">Platform</span></div>', unsafe_allow_html=True)

        ok = api_health()
        color = "#23c45e" if ok else "#f85149"
        bg    = "#0F6E56" if ok else "#791F1F"
        label = "● API Connected" if ok else "✕ API unreachable"
        st.markdown(f'<div style="background:{bg};color:{color};font-weight:600;font-size:.78rem;border-radius:6px;padding:.35rem 1rem;text-align:center">{label}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="font-family:monospace;font-size:.7rem;color:#23c45e;background:rgba(35,196,94,.08);border:1px solid rgba(35,196,94,.2);border-radius:4px;padding:.2rem .5rem;margin:.3rem 0">{API_BASE}</div>', unsafe_allow_html=True)

        st.divider()
        if st.button("↺ Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        auto = st.checkbox("Auto-refresh 15s")
        if auto:
            time.sleep(15)
            st.cache_data.clear()
            st.rerun()

        st.divider()
        n_tx    = query_val("SELECT COUNT(*) FROM transactions")
        n_fraud = query_val("SELECT COUNT(*) FROM fraud_decisions WHERE is_fraud=true")
        n_alert = query_val("SELECT COUNT(*) FROM alerts")

        st.markdown('<p style="font-size:.72rem;color:#6e7681;margin:0">Database</p>', unsafe_allow_html=True)
        st.markdown(f'<div style="font-size:.78rem;line-height:2;color:#8b949e">Transactions <b style="color:#e6edf3;float:right">{n_tx:,}</b><br>Fraud decisions <b style="color:#f85149;float:right">{n_fraud:,}</b><br>Alerts <b style="color:#e3b341;float:right">{n_alert:,}</b></div>', unsafe_allow_html=True)

        st.divider()
        r = get_redis()
        try:
            info = r.info("memory")
            keys = r.dbsize()
            st.markdown(f'<div style="font-size:.72rem;color:#6e7681;margin:0">Redis</div>', unsafe_allow_html=True)
            st.markdown(f'<div style="font-size:.78rem;line-height:2;color:#8b949e">Keys <b style="color:#e6edf3;float:right">{keys:,}</b><br>Memory <b style="color:#e6edf3;float:right">{info["used_memory_human"]}</b></div>', unsafe_allow_html=True)
        except Exception:
            pass


# ── KPIs ──────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def load_kpis():
    return {
        "total_tx":     query_val("SELECT COUNT(*) FROM transactions"),
        "total_fraud":  query_val("SELECT COUNT(*) FROM fraud_decisions WHERE is_fraud=true"),
        "total_legit":  query_val("SELECT COUNT(*) FROM fraud_decisions WHERE is_fraud=false"),
        "total_alerts": query_val("SELECT COUNT(*) FROM alerts"),
        "avg_score":    query_val("SELECT ROUND(AVG(fraud_score)::numeric,3) FROM fraud_decisions"),
        "fraud_rate":   query_val("""
            SELECT ROUND(100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1)
            FROM fraud_decisions
        """),
    }


# ── Tab: Overview ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=15)
def load_overview():
    tx_by_hour = query_df("""
        SELECT DATE_TRUNC('minute', timestamp) AS ts, COUNT(*) AS n
        FROM transactions
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
        GROUP BY 1 ORDER BY 1
    """)
    fraud_by_rule = query_df("""
        SELECT UNNEST(triggered_rules) AS rule, COUNT(*) AS n
        FROM fraud_decisions WHERE is_fraud=true
        GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """)
    fraud_by_country = query_df("""
        SELECT t.country, COUNT(*) AS n
        FROM fraud_decisions d JOIN transactions t ON d.transaction_id=t.id
        WHERE d.is_fraud=true AND t.country IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)
    score_dist = query_df("""
        SELECT ROUND(fraud_score::numeric, 1) AS bucket, COUNT(*) AS n
        FROM fraud_decisions GROUP BY 1 ORDER BY 1
    """)
    return tx_by_hour, fraud_by_rule, fraud_by_country, score_dist


def tab_overview():
    kpis = load_kpis()
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("📊 Transactions",  f"{kpis['total_tx']:,}")
    c2.metric("🚨 Fraud",         f"{kpis['total_fraud']:,}")
    c3.metric("✅ Legit",          f"{kpis['total_legit']:,}")
    c4.metric("⚠️ Alerts",         f"{kpis['total_alerts']:,}")
    c5.metric("📈 Avg Score",      f"{float(kpis['avg_score'] or 0):.3f}")
    c6.metric("🔥 Fraud Rate",     f"{float(kpis['fraud_rate'] or 0):.1f}%")

    st.divider()
    tx_by_hour, fraud_by_rule, fraud_by_country, score_dist = load_overview()

    col_l, col_r = st.columns([1.6, 1])
    with col_l:
        st.markdown("#### Transaction volume (last 1h)")
        if not tx_by_hour.empty:
            fig = px.area(tx_by_hour, x="ts", y="n", color_discrete_sequence=[BLUE])
            fig.update_layout(**PLOTLY, height=260)
            fig.update_traces(line_width=1.5, fillcolor="rgba(56,139,253,0.15)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No transactions yet — waiting for data…")

    with col_r:
        st.markdown("#### Fraud score distribution")
        if not score_dist.empty:
            score_dist["bucket"] = pd.to_numeric(score_dist["bucket"], errors="coerce")
            fig = px.bar(score_dist, x="bucket", y="n",
                         color="bucket", color_continuous_scale=["#23c45e","#e3b341","#f85149"])
            fig.update_layout(**{**PLOTLY, "margin": dict(l=0,r=0,t=10,b=0)},
                              height=260, coloraxis_showscale=False)
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col_l2, col_r2 = st.columns(2)
    with col_l2:
        st.markdown("#### Fraud by rule")
        if not fraud_by_rule.empty:
            fig = px.bar(fraud_by_rule, x="n", y="rule", orientation="h",
                         color="n", color_continuous_scale="Reds")
            fig.update_layout(**PLOTLY, coloraxis_showscale=False,
                              yaxis={"categoryorder":"total ascending"}, height=280)
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No fraud detected yet.")

    with col_r2:
        st.markdown("#### Fraud by country")
        if not fraud_by_country.empty:
            fig = px.bar(fraud_by_country, x="n", y="country", orientation="h",
                         color="n", color_continuous_scale="OrRd")
            fig.update_layout(**PLOTLY, coloraxis_showscale=False,
                              yaxis={"categoryorder":"total ascending"}, height=280)
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, use_container_width=True)


# ── Tab: Alerts ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def load_alerts():
    return query_df("""
        SELECT a.id, a.transaction_id, a.user_id,
               ROUND(a.fraud_score::numeric,3) AS score,
               a.alert_type, a.message, a.acknowledged,
               a.created_at
        FROM alerts a
        ORDER BY a.created_at DESC LIMIT 200
    """)


def tab_alerts():
    st.markdown("#### 🚨 Fraud Alerts")

    col_f1, col_f2 = st.columns(2)
    ack_filter = col_f1.selectbox("Filter", ["All", "Unacknowledged", "Acknowledged"])
    type_filter = col_f2.selectbox("Alert type", ["All"] + [
        "HIGH_AMOUNT","HIGH_RISK_COUNTRY","SUSPICIOUS_IP","AMEX_LARGE","VELOCITY_1H","AMOUNT_SPIKE","MULTI_COUNTRY"
    ])

    df = load_alerts()
    if df.empty:
        st.info("No alerts yet — fraud decisions will appear here once Flink is running.")
        return

    if ack_filter == "Unacknowledged":
        df = df[df["acknowledged"] == False]
    elif ack_filter == "Acknowledged":
        df = df[df["acknowledged"] == True]
    if type_filter != "All":
        df = df[df["alert_type"] == type_filter]

    # Stats
    c1,c2,c3 = st.columns(3)
    c1.metric("Total alerts",        len(df))
    c2.metric("Unacknowledged",       len(df[df["acknowledged"] == False]))
    c3.metric("Avg fraud score",      f"{df['score'].mean():.3f}" if not df.empty else "—")

    st.divider()
    st.dataframe(
        df[["id","transaction_id","user_id","score","alert_type","acknowledged","created_at"]],
        use_container_width=True, hide_index=True,
        column_config={
            "score":       st.column_config.NumberColumn("Score", format="%.3f"),
            "acknowledged":st.column_config.CheckboxColumn("ACK"),
        }
    )


# ── Tab: Transactions ─────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def load_transactions():
    return query_df("""
        SELECT t.id, t.user_id, t.amount, t.currency, t.country,
               t.merchant, t.card_type, t.channel,
               ROUND(d.fraud_score::numeric,3) AS score,
               d.is_fraud, d.triggered_rules,
               t.timestamp
        FROM transactions t
        LEFT JOIN fraud_decisions d ON t.id=d.transaction_id
        ORDER BY t.timestamp DESC LIMIT 500
    """)


def tab_transactions():
    st.markdown("#### 💳 Transactions")

    col1, col2, col3 = st.columns(3)
    country_filter = col1.text_input("Country filter (e.g. NG,RU)")
    fraud_only     = col2.checkbox("Fraud only")
    limit          = col3.slider("Max rows", 50, 500, 100, 50)

    df = load_transactions()
    if df.empty:
        st.info("No transactions yet.")
        return

    if country_filter:
        countries = [c.strip().upper() for c in country_filter.split(",")]
        df = df[df["country"].isin(countries)]
    if fraud_only:
        df = df[df["is_fraud"] == True]

    df = df.head(limit)

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown("**Volume by country**")
        if not df.empty:
            cnt = df.groupby("country").size().reset_index(name="n").sort_values("n", ascending=False)
            fig = px.bar(cnt.head(10), x="country", y="n", color="n",
                         color_continuous_scale="Blues")
            fig.update_layout(**{**PLOTLY, "margin": dict(l=0,r=0,t=10,b=0)},
                              height=220, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("**Amount distribution**")
        if "amount" in df.columns and not df.empty:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
            fig = px.histogram(df, x="amount", nbins=30, color_discrete_sequence=[BLUE])
            fig.update_layout(**{**PLOTLY, "margin": dict(l=0,r=0,t=10,b=0)}, height=220)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.dataframe(
        df[["id","user_id","amount","currency","country","merchant","card_type","score","is_fraud","timestamp"]],
        use_container_width=True, hide_index=True,
        column_config={
            "amount":   st.column_config.NumberColumn("Amount", format="$%.2f"),
            "score":    st.column_config.NumberColumn("Score",  format="%.3f"),
            "is_fraud": st.column_config.CheckboxColumn("Fraud"),
        }
    )


# ── Tab: Score API ────────────────────────────────────────────────────────────
def tab_score_api():
    st.markdown("#### ⚡ Score a Transaction")

    with st.form("score_form"):
        c1, c2 = st.columns(2)
        tx_id   = c1.text_input("Transaction ID", value="tx-test-001")
        user_id = c2.text_input("User ID", value="user_0042")
        amount  = c1.number_input("Amount ($)", min_value=1.0, value=12500.0, step=100.0)
        country = c2.selectbox("Country", ["FR","US","GB","DE","NG","RU","CN","UA","ES","IT","JP","BR"])
        card    = c1.selectbox("Card type", ["visa","mastercard","amex","discover"])
        ip      = c2.text_input("IP Address", value="185.10.20.30")
        submitted = st.form_submit_button("🔍 Score", use_container_width=True)

    if submitted:
        result = api_score({
            "transaction_id": tx_id, "user_id": user_id,
            "amount": amount, "country": country,
            "card_type": card, "ip_address": ip,
        })
        if result:
            score = result["fraud_score"]
            is_fraud = result["is_fraud"]
            color = RED if is_fraud else GREEN
            label = "🚨 FRAUD" if is_fraud else "✅ LEGIT"

            st.markdown(f"""
            <div style="background:#161b22;border:2px solid {color};border-radius:12px;padding:1.5rem;text-align:center;margin:1rem 0">
              <div style="font-size:2rem;font-weight:700;color:{color}">{label}</div>
              <div style="font-size:3rem;font-weight:700;font-family:monospace;color:{color}">{score:.4f}</div>
              <div style="color:#8b949e;font-size:.9rem;margin-top:.5rem">
                model: {result['model_version']} · latency: {result['decision_ms']}ms · threshold: {result['threshold']}
              </div>
            </div>
            """, unsafe_allow_html=True)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Features used**")
                st.json(result.get("features_used", {}))

    st.divider()
    st.markdown("#### 📊 Recent Decisions")
    df = query_df("""
        SELECT transaction_id, ROUND(fraud_score::numeric,4) AS score,
               is_fraud, triggered_rules, decided_at
        FROM fraud_decisions ORDER BY decided_at DESC LIMIT 50
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True,
                     column_config={"score": st.column_config.NumberColumn("Score", format="%.4f"),
                                    "is_fraud": st.column_config.CheckboxColumn("Fraud")})
    else:
        st.info("No decisions yet — Flink pipeline processes transactions into this table.")


# ── Tab: Pipeline ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def load_pipeline():
    return query_df("""
        SELECT
          (SELECT COUNT(*) FROM transactions) AS total_tx,
          (SELECT COUNT(*) FROM fraud_decisions) AS total_decisions,
          (SELECT COUNT(*) FROM fraud_decisions WHERE is_fraud=true) AS fraud_count,
          (SELECT COUNT(*) FROM alerts) AS alert_count,
          (SELECT COUNT(*) FROM dlq_messages) AS dlq_count,
          (SELECT COUNT(*) FROM user_features) AS feature_count
    """)


def tab_pipeline():
    st.markdown("#### ⚡ Pipeline Status")

    df = load_pipeline()
    if not df.empty:
        r = df.iloc[0]
        c1,c2,c3,c4,c5,c6 = st.columns(6)
        c1.metric("Transactions",  int(r.get("total_tx", 0)))
        c2.metric("Decisions",     int(r.get("total_decisions", 0)))
        c3.metric("Fraud",         int(r.get("fraud_count", 0)))
        c4.metric("Alerts",        int(r.get("alert_count", 0)))
        c5.metric("DLQ",           int(r.get("dlq_count", 0)))
        c6.metric("User features", int(r.get("feature_count", 0)))

    st.divider()
    st.markdown("#### Architecture")
    st.markdown("""
    ```
    Payment API Producer (5 tx/s)  ──┐
                                      ├──▶  Kafka (transactions.payments + transactions.cdc)
    CDC Producer (Debezium-style)  ──┘         │
                                               ▼
                                    PyFlink Streaming
                                    normalize → score → route
                                         │          │         │
                                    decisions    alerts     DLQ
                                         │
                                    Feature Store
                                    Redis (online) + PostgreSQL (offline)
                                         │
                                    FastAPI /score  ──▶  < 50ms
                                         │
                                    Streamlit Dashboard
    ```
    """)

    st.divider()
    st.markdown("#### User Feature Store (top 20)")
    df_feat = query_df("""
        SELECT user_id, tx_count_1h, tx_count_24h,
               ROUND(avg_amount::numeric, 2) AS avg_amount,
               distinct_countries_24h,
               last_tx_at
        FROM user_features ORDER BY tx_count_24h DESC LIMIT 20
    """)
    if not df_feat.empty:
        st.dataframe(df_feat, use_container_width=True, hide_index=True)
    else:
        st.info("Feature store populates as transactions flow in.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    render_sidebar()
    st.title("🛡️ Fraud Detection Platform")

    t1, t2, t3, t4, t5 = st.tabs([
        "📊 Overview", "🚨 Alerts", "💳 Transactions", "⚡ Score API", "🔧 Pipeline"
    ])
    with t1: tab_overview()
    with t2: tab_alerts()
    with t3: tab_transactions()
    with t4: tab_score_api()
    with t5: tab_pipeline()


if __name__ == "__main__":
    main()
