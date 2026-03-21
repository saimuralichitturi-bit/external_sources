"""
dashboard/app.py
================
Streamlit Market Intelligence Dashboard
Pulls unified CSV files from Google Drive and displays real-time
inventory, pricing, sales estimates, and brand analytics
across Blinkit, Myntra, Amazon, and Flipkart.

Setup:
  1. Add credentials to .streamlit/secrets.toml (see .streamlit/secrets.toml.example)
  2. Run: streamlit run dashboard/app.py

Streamlit Cloud:
  Add GOOGLE_CREDENTIALS_JSON and GDRIVE_FOLDER_ID in Streamlit Secrets UI.
"""

import io, os, json, sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Market Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Platform colors ───────────────────────────────────────────────────────────
PLATFORM_COLORS = {
    "blinkit":  "#F5D000",  # yellow
    "myntra":   "#FF3F6C",  # pink
    "amazon":   "#FF9900",  # orange
    "flipkart": "#2874F0",  # blue
}

PLATFORM_ICONS = {
    "blinkit":  "🟡",
    "myntra":   "🛍️",
    "amazon":   "🟠",
    "flipkart": "🔵",
}

# ── Google Drive fetch ────────────────────────────────────────────────────────

def _get_folder_id() -> str:
    """Get Drive folder ID from Streamlit secrets or environment."""
    try:
        return st.secrets["GDRIVE_FOLDER_ID"]
    except Exception:
        return os.environ.get("GDRIVE_FOLDER_ID", "")


def _get_credentials_json() -> str:
    """Get service account credentials JSON string."""
    try:
        raw = st.secrets["GOOGLE_CREDENTIALS_JSON"]
        # If stored as TOML table, convert back to JSON string
        if isinstance(raw, dict):
            return json.dumps(dict(raw))
        return raw
    except Exception:
        return os.environ.get("GOOGLE_CREDENTIALS_JSON", "")


def _build_drive_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    raw = _get_credentials_json()

    if raw:
        info = json.loads(raw)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    else:
        # Try local credentials file
        for fname in ["credentials.json", "service_account.json"]:
            p = Path(__file__).parent.parent / fname
            if p.exists():
                creds = service_account.Credentials.from_service_account_file(str(p), scopes=scopes)
                break
        else:
            return None

    return build("drive", "v3", credentials=creds)


@st.cache_data(ttl=600, show_spinner=False)
def fetch_csv_from_drive(filename: str) -> pd.DataFrame | None:
    """
    Download a CSV file from Google Drive by name.
    Cached for 10 minutes — refreshes automatically.
    """
    folder_id = _get_folder_id()
    if not folder_id:
        return None

    try:
        import io
        from googleapiclient.http import MediaIoBaseDownload

        service = _build_drive_service()
        if not service:
            return None

        # Find file by name
        resp = service.files().list(
            q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
            fields="files(id, name, modifiedTime)",
            spaces="drive",
        ).execute()
        files = resp.get("files", [])
        if not files:
            return None

        fid = files[0]["id"]
        request = service.files().get_media(fileId=fid)
        buf = io.BytesIO()
        dl = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = dl.next_chunk()

        buf.seek(0)
        return pd.read_csv(buf, dtype=str)

    except Exception as e:
        st.warning(f"Drive fetch error for {filename}: {e}")
        return None


def load_local_csv(filename: str) -> pd.DataFrame | None:
    """Fallback: load from local data/ directory."""
    data_dir = Path(__file__).parent.parent / "data"
    path = data_dir / filename
    if path.exists():
        return pd.read_csv(path, dtype=str)
    return None


def get_df(filename: str) -> pd.DataFrame:
    """Try Drive first, fall back to local data/."""
    df = fetch_csv_from_drive(filename)
    if df is None or df.empty:
        df = load_local_csv(filename)
    if df is None:
        return pd.DataFrame()
    return df


# ── Data loading + typing ─────────────────────────────────────────────────────

@st.cache_data(ttl=600, show_spinner=False)
def load_snapshots() -> pd.DataFrame:
    df = get_df("unified_snapshots.csv")
    if df.empty:
        return df
    num_cols = ["price", "mrp", "discount_pct", "avg_rating", "rating_count",
                "inventory", "stock_count"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    if "is_oos" in df.columns:
        df["is_oos"] = df["is_oos"].map({"True": True, "False": False, "true": True, "false": False})
    return df


@st.cache_data(ttl=600, show_spinner=False)
def load_estimates() -> pd.DataFrame:
    df = get_df("unified_estimates.csv")
    if df.empty:
        return df
    for c in ["daily_units_est", "monthly_units_est"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    return df


@st.cache_data(ttl=600, show_spinner=False)
def load_brands() -> pd.DataFrame:
    df = get_df("unified_brands.csv")
    if df.empty:
        return df
    for c in ["product_count", "total_daily_units_est", "total_monthly_units_est"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    return df


# ── Sidebar ───────────────────────────────────────────────────────────────────

def render_sidebar(snap_df: pd.DataFrame, est_df: pd.DataFrame):
    st.sidebar.title("📊 Market Intel")

    # Refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")

    # Platform filter
    all_platforms = ["blinkit", "myntra", "amazon", "flipkart"]
    available = sorted(snap_df["platform"].dropna().unique().tolist()) if not snap_df.empty else all_platforms
    sel_platforms = st.sidebar.multiselect(
        "Platforms",
        options=available,
        default=available,
    )

    # Date range
    if not snap_df.empty and "scraped_at" in snap_df.columns:
        valid_dates = snap_df["scraped_at"].dropna()
        if not valid_dates.empty:
            min_date = valid_dates.min().date()
            max_date = valid_dates.max().date()
            date_range = st.sidebar.date_input(
                "Date range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_date, end_date = date_range
            else:
                start_date, end_date = min_date, max_date
        else:
            start_date = end_date = None
    else:
        start_date = end_date = None

    # Location filter (Blinkit)
    locations = []
    if not snap_df.empty and "location" in snap_df.columns:
        locs = snap_df[snap_df["platform"] == "blinkit"]["location"].dropna().unique().tolist()
        locs = [l for l in locs if l]
        if locs:
            locations = st.sidebar.multiselect("Blinkit Locations", options=sorted(locs), default=sorted(locs))

    st.sidebar.markdown("---")
    st.sidebar.caption("Data refreshes every 10 min from Google Drive")

    return sel_platforms, start_date, end_date, locations


# ── Tab: Overview ─────────────────────────────────────────────────────────────

def tab_overview(snap_df: pd.DataFrame, est_df: pd.DataFrame, brand_df: pd.DataFrame, platforms: list):
    st.header("Platform Overview")

    if snap_df.empty:
        st.info("No snapshot data yet. Run the scrapers and upload to Drive.")
        return

    df = snap_df[snap_df["platform"].isin(platforms)]

    # KPI cards
    cols = st.columns(4)
    for i, plat in enumerate(["blinkit", "myntra", "amazon", "flipkart"]):
        if plat not in platforms:
            continue
        sub = df[df["platform"] == plat]
        with cols[i % 4]:
            icon = PLATFORM_ICONS.get(plat, "")
            n_products = sub["product_id"].nunique()
            n_brands   = sub["brand"].nunique()
            last_run   = sub["scraped_at"].max() if "scraped_at" in sub.columns else None
            last_str   = last_run.strftime("%b %d %H:%M") if pd.notna(last_run) else "—"
            st.metric(
                label=f"{icon} {plat.capitalize()}",
                value=f"{n_products:,} products",
                delta=f"{n_brands} brands",
            )
            st.caption(f"Last scraped: {last_str}")

    st.markdown("---")

    # Products per platform bar chart
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Products by Platform")
        plat_counts = (
            df.groupby("platform")["product_id"]
            .nunique()
            .reset_index()
            .rename(columns={"product_id": "products"})
        )
        plat_counts["color"] = plat_counts["platform"].map(PLATFORM_COLORS)
        fig = px.bar(
            plat_counts, x="platform", y="products",
            color="platform",
            color_discrete_map=PLATFORM_COLORS,
            text="products",
        )
        fig.update_layout(showlegend=False, height=300, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Daily Sales Estimates by Platform")
        if not est_df.empty:
            est_sub = est_df[est_df["platform"].isin(platforms)]
            plat_est = (
                est_sub.groupby("platform")["daily_units_est"]
                .sum()
                .reset_index()
                .rename(columns={"daily_units_est": "daily_units"})
            )
            fig2 = px.bar(
                plat_est, x="platform", y="daily_units",
                color="platform",
                color_discrete_map=PLATFORM_COLORS,
                text_auto=".0f",
            )
            fig2.update_layout(showlegend=False, height=300, margin=dict(t=10, b=10))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No estimates data yet.")

    # OOS rate
    st.subheader("Out-of-Stock Rate by Platform")
    if "is_oos" in df.columns:
        oos = (
            df.groupby("platform")
            .apply(lambda x: (x["is_oos"].sum() / len(x) * 100) if len(x) else 0)
            .reset_index()
            .rename(columns={0: "oos_pct"})
        )
        fig3 = px.bar(oos, x="platform", y="oos_pct",
                      color="platform", color_discrete_map=PLATFORM_COLORS,
                      text_auto=".1f", labels={"oos_pct": "OOS %"})
        fig3.update_layout(showlegend=False, height=250, margin=dict(t=10, b=10))
        st.plotly_chart(fig3, use_container_width=True)


# ── Tab: Inventory ────────────────────────────────────────────────────────────

def tab_inventory(snap_df: pd.DataFrame, platforms: list, locations: list,
                  start_date, end_date):
    st.header("Inventory Tracker")

    if snap_df.empty:
        st.info("No inventory data available.")
        return

    df = snap_df[snap_df["platform"].isin(platforms)].copy()

    if start_date and end_date and "scraped_at" in df.columns:
        df = df[
            (df["scraped_at"].dt.date >= start_date) &
            (df["scraped_at"].dt.date <= end_date)
        ]

    # Blinkit inventory time-series
    blinkit_df = df[df["platform"] == "blinkit"].copy()
    if not blinkit_df.empty and "inventory" in blinkit_df.columns:
        st.subheader("Blinkit Inventory Over Time")

        if locations:
            blinkit_df = blinkit_df[blinkit_df["location"].isin(locations)]

        # Product selector
        top_brands = blinkit_df["brand"].value_counts().head(10).index.tolist()
        sel_brand = st.selectbox("Filter by brand (Blinkit)", ["All"] + top_brands)
        if sel_brand != "All":
            blinkit_df = blinkit_df[blinkit_df["brand"] == sel_brand]

        top_products = blinkit_df["name"].value_counts().head(20).index.tolist()
        sel_products = st.multiselect("Select products", top_products, default=top_products[:5])
        if sel_products:
            blinkit_df = blinkit_df[blinkit_df["name"].isin(sel_products)]

        if not blinkit_df.empty and "scraped_at" in blinkit_df.columns:
            fig = px.line(
                blinkit_df.sort_values("scraped_at"),
                x="scraped_at", y="inventory",
                color="name", facet_col="location" if locations and len(locations) > 1 else None,
                labels={"inventory": "Units in Stock", "scraped_at": "Time", "name": "Product"},
                title="Blinkit Inventory (0=OOS, 50=50+)",
            )
            fig.update_layout(height=400, margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Select at least one product.")
    else:
        if "blinkit" in platforms:
            st.info("No Blinkit inventory data yet.")

    # Myntra inventory (stock_count or OOS)
    myntra_df = df[df["platform"] == "myntra"].copy()
    if not myntra_df.empty:
        st.subheader("Myntra Stock Status")
        c1, c2 = st.columns(2)
        with c1:
            if "is_oos" in myntra_df.columns:
                in_stock = (~myntra_df["is_oos"].fillna(False)).sum()
                oos      = myntra_df["is_oos"].fillna(False).sum()
                fig_pie = px.pie(
                    values=[in_stock, oos], names=["In Stock", "OOS"],
                    color_discrete_sequence=["#2ecc71", "#e74c3c"],
                    title="In Stock vs OOS",
                )
                fig_pie.update_layout(height=280, margin=dict(t=40, b=0))
                st.plotly_chart(fig_pie, use_container_width=True)
        with c2:
            if "discount_pct" in myntra_df.columns:
                fig_disc = px.histogram(
                    myntra_df, x="discount_pct", nbins=20,
                    title="Discount Distribution (%)",
                    color_discrete_sequence=[PLATFORM_COLORS["myntra"]],
                )
                fig_disc.update_layout(height=280, margin=dict(t=40, b=0))
                st.plotly_chart(fig_disc, use_container_width=True)

    # Amazon / Flipkart low-stock
    af_df = df[df["platform"].isin(["amazon", "flipkart"])].copy()
    low_stock = af_df[af_df["stock_count"].notna() & (af_df["stock_count"] > 0)]
    if not low_stock.empty:
        st.subheader("Amazon / Flipkart — Low Stock Alerts")
        st.caption("Products where platform shows 'Only N left in stock'")
        disp = low_stock[["platform", "name", "brand", "stock_count", "price", "scraped_at"]].sort_values(
            "stock_count"
        )
        st.dataframe(disp, use_container_width=True, hide_index=True)


# ── Tab: Pricing ──────────────────────────────────────────────────────────────

def tab_pricing(snap_df: pd.DataFrame, platforms: list, start_date, end_date):
    st.header("Price Tracker")

    if snap_df.empty:
        st.info("No pricing data available.")
        return

    df = snap_df[snap_df["platform"].isin(platforms)].copy()
    if start_date and end_date and "scraped_at" in df.columns:
        df = df[
            (df["scraped_at"].dt.date >= start_date) &
            (df["scraped_at"].dt.date <= end_date)
        ]

    c1, c2 = st.columns(2)

    # Average price by platform
    with c1:
        st.subheader("Avg Price by Platform")
        avg_price = (
            df.groupby("platform")["price"]
            .mean()
            .dropna()
            .reset_index()
            .rename(columns={"price": "avg_price"})
        )
        fig = px.bar(avg_price, x="platform", y="avg_price",
                     color="platform", color_discrete_map=PLATFORM_COLORS,
                     text_auto=".0f", labels={"avg_price": "Avg Price (Rs.)"})
        fig.update_layout(showlegend=False, height=300, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # Discount distribution
    with c2:
        st.subheader("Avg Discount % by Platform")
        avg_disc = (
            df.groupby("platform")["discount_pct"]
            .mean()
            .dropna()
            .reset_index()
            .rename(columns={"discount_pct": "avg_discount"})
        )
        fig2 = px.bar(avg_disc, x="platform", y="avg_discount",
                      color="platform", color_discrete_map=PLATFORM_COLORS,
                      text_auto=".1f", labels={"avg_discount": "Avg Discount %"})
        fig2.update_layout(showlegend=False, height=300, margin=dict(t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)

    # Price history for a selected product
    st.subheader("Price History — Search a Product")
    search_term = st.text_input("Product name (partial match)", placeholder="e.g. Lay's, Nike, Protein")
    if search_term:
        mask = df["name"].str.contains(search_term, case=False, na=False)
        matched = df[mask]
        if matched.empty:
            st.info("No products match that search.")
        else:
            fig3 = px.line(
                matched.sort_values("scraped_at"),
                x="scraped_at", y="price",
                color="name", line_group="platform",
                symbol="platform",
                labels={"price": "Price (Rs.)", "scraped_at": "Date"},
            )
            fig3.update_layout(height=380, margin=dict(t=10, b=10))
            st.plotly_chart(fig3, use_container_width=True)

    # Top discounted products
    st.subheader("Top 20 Discounted Products Right Now")
    latest = (
        df.sort_values("scraped_at", ascending=False)
        .drop_duplicates(subset=["platform", "product_id"])
    )
    top_disc = latest.nlargest(20, "discount_pct")[
        ["platform", "name", "brand", "price", "mrp", "discount_pct"]
    ]
    st.dataframe(top_disc, use_container_width=True, hide_index=True)


# ── Tab: Sales Estimates ───────────────────────────────────────────────────────

def tab_sales(est_df: pd.DataFrame, brand_df: pd.DataFrame, platforms: list):
    st.header("Sales Estimates")

    if est_df.empty:
        st.info("No sales estimate data yet.")
        return

    df = est_df[est_df["platform"].isin(platforms)].copy()

    # Latest estimates per product (deduplicate)
    if "scraped_at" in df.columns:
        df = df.sort_values("scraped_at", ascending=False).drop_duplicates(
            subset=["platform", "product_id"]
        )

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Total Daily Units by Platform")
        plat_total = (
            df.groupby("platform")["daily_units_est"]
            .sum()
            .dropna()
            .reset_index()
        )
        fig = px.bar(plat_total, x="platform", y="daily_units_est",
                     color="platform", color_discrete_map=PLATFORM_COLORS,
                     text_auto=".0f", labels={"daily_units_est": "Units/Day"})
        fig.update_layout(showlegend=False, height=300, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Confidence Distribution")
        conf_counts = df["confidence"].value_counts().reset_index()
        conf_counts.columns = ["confidence", "count"]
        color_map = {"high": "#2ecc71", "medium": "#f39c12", "low": "#e74c3c", "none": "#95a5a6"}
        fig2 = px.pie(conf_counts, values="count", names="confidence",
                      color="confidence", color_discrete_map=color_map)
        fig2.update_layout(height=300, margin=dict(t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)

    # Top products table
    st.subheader("Top Products by Estimated Daily Sales")
    sel_plat = st.selectbox("Platform", ["All"] + list(platforms), key="est_plat")
    sub = df if sel_plat == "All" else df[df["platform"] == sel_plat]
    top = sub.nlargest(30, "daily_units_est")[
        ["platform", "name", "brand", "keyword", "daily_units_est", "monthly_units_est", "confidence"]
    ]
    top["daily_units_est"]   = top["daily_units_est"].round(0).astype("Int64")
    top["monthly_units_est"] = top["monthly_units_est"].round(0).astype("Int64")
    st.dataframe(top, use_container_width=True, hide_index=True)


# ── Tab: Brands ───────────────────────────────────────────────────────────────

def tab_brands(brand_df: pd.DataFrame, est_df: pd.DataFrame, platforms: list):
    st.header("Brand Analytics")

    if brand_df.empty:
        st.info("No brand data yet.")
        return

    df = brand_df[brand_df["platform"].isin(platforms)].copy()

    # Latest per brand
    if "scraped_at" in df.columns:
        df = df.sort_values("scraped_at", ascending=False).drop_duplicates(
            subset=["platform", "brand", "location"]
        )

    # Top brands bar
    st.subheader("Top Brands by Daily Sales Estimate")
    top_brands = (
        df.groupby(["brand", "platform"])["total_daily_units_est"]
        .sum()
        .dropna()
        .reset_index()
        .sort_values("total_daily_units_est", ascending=False)
        .head(25)
    )
    fig = px.bar(
        top_brands, x="brand", y="total_daily_units_est",
        color="platform", color_discrete_map=PLATFORM_COLORS,
        barmode="stack",
        labels={"total_daily_units_est": "Daily Units", "brand": "Brand"},
    )
    fig.update_layout(height=400, margin=dict(t=10, b=80), xaxis_tickangle=-40)
    st.plotly_chart(fig, use_container_width=True)

    # Brand share treemap
    st.subheader("Brand Share — Treemap")
    sel_plat_brand = st.selectbox("Platform", ["All"] + sorted(platforms), key="brand_plat")
    sub = df if sel_plat_brand == "All" else df[df["platform"] == sel_plat_brand]
    brand_share = (
        sub.groupby("brand")["total_daily_units_est"]
        .sum()
        .dropna()
        .reset_index()
        .nlargest(40, "total_daily_units_est")
    )
    if not brand_share.empty:
        fig2 = px.treemap(
            brand_share, path=["brand"], values="total_daily_units_est",
            color="total_daily_units_est",
            color_continuous_scale="Blues",
        )
        fig2.update_layout(height=400, margin=dict(t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)

    # Full brand table
    st.subheader("Brand Table")
    disp = df[["platform", "brand", "product_count",
               "total_daily_units_est", "total_monthly_units_est", "location"]].copy()
    disp = disp.sort_values("total_daily_units_est", ascending=False)
    disp["total_daily_units_est"]   = disp["total_daily_units_est"].round(1)
    disp["total_monthly_units_est"] = disp["total_monthly_units_est"].round(0)
    st.dataframe(disp, use_container_width=True, hide_index=True)


# ── Tab: Raw Data ─────────────────────────────────────────────────────────────

def tab_raw(snap_df: pd.DataFrame, est_df: pd.DataFrame, brand_df: pd.DataFrame, platforms: list):
    st.header("Raw Data")
    tabs = st.tabs(["Snapshots", "Estimates", "Brands"])
    with tabs[0]:
        df = snap_df[snap_df["platform"].isin(platforms)] if not snap_df.empty else snap_df
        st.caption(f"{len(df):,} rows")
        st.dataframe(df.head(500), use_container_width=True, hide_index=True)
        if not df.empty:
            csv = df.to_csv(index=False).encode()
            st.download_button("Download snapshots CSV", csv, "snapshots.csv", "text/csv")
    with tabs[1]:
        df = est_df[est_df["platform"].isin(platforms)] if not est_df.empty else est_df
        st.caption(f"{len(df):,} rows")
        st.dataframe(df.head(500), use_container_width=True, hide_index=True)
        if not df.empty:
            csv = df.to_csv(index=False).encode()
            st.download_button("Download estimates CSV", csv, "estimates.csv", "text/csv")
    with tabs[2]:
        df = brand_df[brand_df["platform"].isin(platforms)] if not brand_df.empty else brand_df
        st.caption(f"{len(df):,} rows")
        st.dataframe(df.head(500), use_container_width=True, hide_index=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load data
    with st.spinner("Loading data from Google Drive..."):
        snap_df  = load_snapshots()
        est_df   = load_estimates()
        brand_df = load_brands()

    # Check Drive connection
    folder_id = _get_folder_id()
    if not folder_id:
        st.warning(
            "Google Drive not configured. Add `GDRIVE_FOLDER_ID` and "
            "`GOOGLE_CREDENTIALS_JSON` to `.streamlit/secrets.toml` or environment variables. "
            "Showing local data/ files for now."
        )

    # Sidebar
    platforms, start_date, end_date, locations = render_sidebar(snap_df, est_df)

    # Tab navigation
    tab_names = ["Overview", "Inventory", "Pricing", "Sales Estimates", "Brands", "Raw Data"]
    tabs = st.tabs(tab_names)

    with tabs[0]:
        tab_overview(snap_df, est_df, brand_df, platforms)

    with tabs[1]:
        tab_inventory(snap_df, platforms, locations, start_date, end_date)

    with tabs[2]:
        tab_pricing(snap_df, platforms, start_date, end_date)

    with tabs[3]:
        tab_sales(est_df, brand_df, platforms)

    with tabs[4]:
        tab_brands(brand_df, est_df, platforms)

    with tabs[5]:
        tab_raw(snap_df, est_df, brand_df, platforms)

    # Footer
    st.markdown("---")
    last_snap = snap_df["scraped_at"].max() if not snap_df.empty and "scraped_at" in snap_df.columns else None
    last_str  = last_snap.strftime("%Y-%m-%d %H:%M") if pd.notna(last_snap) else "—"
    st.caption(f"Last data: {last_str}  |  Cache TTL: 10 min  |  Data source: Google Drive")


if __name__ == "__main__":
    main()
