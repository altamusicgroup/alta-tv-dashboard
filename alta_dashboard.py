import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime
import time
import os
import base64

# Page config - must be first Streamlit command
st.set_page_config(
    page_title="ALTA MUSIC GROUP - Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for TV display
st.markdown(
    """
<style>
    /* Import Special Gothic font */
    @import url('https://fonts.cdnfonts.com/css/special-gothic');

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Full screen styling with black background */
    .block-container {
        padding-top: 0.05rem !important;
        padding-bottom: 0rem !important;
        max-width: 100% !important;
        background-color: #000000 !important;

        /* TV fit (light) */
        transform: scale(0.75) !important;
        transform-origin: top center !important;
        }

    /* Force black background everywhere */
    .stApp {
        background-color: #000000 !important;
    }

    /* Apply Special Gothic font globally */
    * {
        font-family: 'Special Gothic', sans-serif !important;
        color: #FFFFFF !important;
    }

    /* ===== TV HORIZONTAL FIXES ===== */

    /* Remove Streamlit side gutters (reclaim width) */
    section.main > div {
        padding-left: 0.25rem !important;
        padding-right: 0.25rem !important;
    }

    /* Tighten column spacing */
    div[data-testid="column"] {
        padding-left: 0.25rem !important;
        padding-right: 0.25rem !important;
    }

    /* Remove extra spacing between consecutive markdown blocks */
    div[data-testid="stMarkdown"] {
        margin-bottom: 0 !important;
        padding-bottom: 0 !important;
    }
    div[data-testid="stMarkdown"] + div[data-testid="stMarkdown"] {
        margin-top: 0 !important;
        padding-top: 0 !important;
    }

    /* Logo spacing control (handles transparent padding inside the PNG) */
    .logo-wrap {
        display: flex;
        justify-content: center;
        align-items: center;
        margin-top: 0 !important;
        margin-bottom: -2.2rem !important;
    }

    .logo-img {
        display: block;
        transform: translateY(-18px);
    }

    /* Title styling */
    h1 {
        font-size: 2.8rem !important;
        text-align: center !important;
        margin-top: -0.6rem !important;
        margin-bottom: 0.2rem !important;
        font-weight: 700 !important;
        color: #FFFFFF !important;
    }

    h2 {
        font-size: 1.6rem !important;
        margin-top: 0.8rem !important;
        margin-bottom: 0.5rem !important;
        color: #FFFFFF !important;
    }

    /* Subtitle text */
    p {
        font-size: 0.85rem !important;
        margin-top: 0 !important;
        margin-bottom: 0.25rem !important;
        text-align: center !important;
    }

    /* Minimal spacing between sections */
    hr {
        margin-top: 0.3rem !important;
        margin-bottom: 0.3rem !important;
    }

    /* Custom metric cards with inline delta arrows */
    .metric-card {
        background: #000000;
        text-align: center;
        padding: 0.15rem 0;

        /* Prevent clipping of arrows/percents on TV browsers */
        overflow: visible !important;
    }

    .metric-label {
        font-size: 1.35rem !important;
        font-weight: 800;
        line-height: 1.1;
        opacity: 0.95;
    }

    .metric-card {
    background: #000000;
    text-align: center;
    padding: 0.15rem 0;
    overflow: visible !important;
    }

    .metric-label {
        font-size: 1.35rem !important;
        font-weight: 800;
        line-height: 1.1;
        opacity: 0.95;
    }

    .metric-value {
        font-size: 3.2rem !important;
        font-weight: 900;
        line-height: 1.05;
        white-space: nowrap;
    }

    .metric-delta {
        margin-top: 0.15rem;
        font-size: 1.5rem !important;
        font-weight: 900;
        white-space: nowrap;
    }

    .metric-delta-up { color: #19C37D !important; }
    .metric-delta-down { color: #FF4D4D !important; }
    .metric-delta-flat { color: #AAAAAA !important; }

    .metric-value {
        font-size: 3.2rem !important;
        font-weight: 900;
        line-height: 1.05;
        white-space: nowrap;
    }

    .metric-delta-up,
    .metric-delta-down,
    .metric-delta-flat {
        font-size: 1.4rem !important;
        font-weight: 900;
        white-space: nowrap;

        /* squeeze a few pixels back */
        letter-spacing: -0.02em;
    }

    .metric-delta-up { color: #19C37D !important; }   /* green */
    .metric-delta-down { color: #FF4D4D !important; } /* red */
    .metric-delta-flat { color: #AAAAAA !important; } /* neutral */

    /* Info boxes */
    .stAlert {
        background-color: #1A1A1A !important;
        color: #FFFFFF !important;
    }
</style>
""",
    unsafe_allow_html=True
)

# Password protection
def check_password():
    """Returns True if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets.get("dashboard_password", "alta2024"):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown("## ALTA MUSIC GROUP Dashboard")
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown("## ALTA MUSIC GROUP Dashboard")
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        st.error("Password incorrect")
        return False
    else:
        return True


if not check_password():
    st.stop()


# Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    """Create Snowflake connection using Streamlit secrets"""
    try:
        # Key-pair auth (2FA accounts)
        if "private_key" in st.secrets["snowflake"]:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            private_key_text = st.secrets["snowflake"]["private_key"]

            passphrase = None
            if "private_key_passphrase" in st.secrets["snowflake"]:
                passphrase = st.secrets["snowflake"]["private_key_passphrase"].encode()

            p_key = serialization.load_pem_private_key(
                private_key_text.encode(),
                password=passphrase,
                backend=default_backend()
            )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            conn = snowflake.connector.connect(
                user=st.secrets["snowflake"]["user"],
                account=st.secrets["snowflake"]["account"],
                warehouse=st.secrets["snowflake"]["warehouse"],
                database=st.secrets["snowflake"]["database"],
                schema=st.secrets["snowflake"]["schema"],
                private_key=pkb
            )
        else:
            # Password auth
            conn = snowflake.connector.connect(
                user=st.secrets["snowflake"]["user"],
                password=st.secrets["snowflake"]["password"],
                account=st.secrets["snowflake"]["account"],
                warehouse=st.secrets["snowflake"]["warehouse"],
                database=st.secrets["snowflake"]["database"],
                schema=st.secrets["snowflake"]["schema"]
            )

        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        st.info("If you have 2FA enabled, make sure you're using RSA key-pair authentication (see README)")
        st.stop()


# Query functions
@st.cache_data(ttl=300)
def get_overall_metrics():
    """
    Returns current 7-day window and previous 7-day window metrics.
      - current: [today-6, today] inclusive
      - previous: [today-13, today-7] inclusive
    """
    conn = get_snowflake_connection()
    query = """
    WITH base AS (
      SELECT
        activity_date,
        total_streams,
        total_listeners,
        artist_name,
        isrc,
        tiktok_views,
        tiktok_creations
      FROM ANALYTICS_PROD.STREAMING.TRACK_DAILY_PERFORMANCE_VW
      WHERE activity_date >= DATEADD(day, -13, CURRENT_DATE())
        AND activity_date <= CURRENT_DATE()
    )
    SELECT
      /* Current 7 days */
      SUM(CASE WHEN activity_date >= DATEADD(day, -6, CURRENT_DATE()) THEN total_streams ELSE 0 END)        AS curr_total_streams,
      SUM(CASE WHEN activity_date >= DATEADD(day, -6, CURRENT_DATE()) THEN total_listeners ELSE 0 END)      AS curr_total_listeners,
      COUNT(DISTINCT CASE WHEN activity_date >= DATEADD(day, -6, CURRENT_DATE()) THEN artist_name END)      AS curr_total_artists,
      COUNT(DISTINCT CASE WHEN activity_date >= DATEADD(day, -6, CURRENT_DATE()) THEN isrc END)            AS curr_total_tracks,
      SUM(CASE WHEN activity_date >= DATEADD(day, -6, CURRENT_DATE()) THEN tiktok_views ELSE 0 END)        AS curr_total_tiktok_views,
      SUM(CASE WHEN activity_date >= DATEADD(day, -6, CURRENT_DATE()) THEN tiktok_creations ELSE 0 END)    AS curr_total_tiktok_creations,

      /* Previous 7 days */
      SUM(CASE WHEN activity_date < DATEADD(day, -6, CURRENT_DATE()) THEN total_streams ELSE 0 END)         AS prev_total_streams,
      SUM(CASE WHEN activity_date < DATEADD(day, -6, CURRENT_DATE()) THEN total_listeners ELSE 0 END)       AS prev_total_listeners,
      COUNT(DISTINCT CASE WHEN activity_date < DATEADD(day, -6, CURRENT_DATE()) THEN artist_name END)       AS prev_total_artists,
      COUNT(DISTINCT CASE WHEN activity_date < DATEADD(day, -6, CURRENT_DATE()) THEN isrc END)             AS prev_total_tracks,
      SUM(CASE WHEN activity_date < DATEADD(day, -6, CURRENT_DATE()) THEN tiktok_views ELSE 0 END)          AS prev_total_tiktok_views,
      SUM(CASE WHEN activity_date < DATEADD(day, -6, CURRENT_DATE()) THEN tiktok_creations ELSE 0 END)      AS prev_total_tiktok_creations
    FROM base;
    """
    df = pd.read_sql(query, conn)
    row = df.iloc[0]
    return {str(k).lower(): row[k] for k in row.index}


@st.cache_data(ttl=300)
def get_artist_leaderboard():
    """Get top 10 artists by streams for last 7 days"""
    conn = get_snowflake_connection()
    query = """
    SELECT 
        artist_name,
        SUM(total_streams) as streams,
        SUM(tiktok_views) as tiktok_views
    FROM ANALYTICS_PROD.STREAMING.ARTIST_DAILY_PERFORMANCE_VW
    WHERE activity_date >= CURRENT_DATE() - 7
    GROUP BY artist_name
    ORDER BY streams DESC
    LIMIT 10
    """
    df = pd.read_sql(query, conn)
    return df


# Arrow helpers
def _pct_change(curr: float, prev: float) -> float:
    curr = float(curr or 0)
    prev = float(prev or 0)
    if prev == 0:
        return 0.0 if curr == 0 else 1.0
    return (curr - prev) / prev


def render_metric_card(label: str, curr_value, prev_value=None, is_int=True, show_delta=True):
    curr = float(curr_value or 0)
    prev = float(prev_value or 0) if prev_value is not None else None

    value_txt = f"{int(curr):,}" if is_int else f"{curr:.1f}"

    if (not show_delta) or (prev is None):
        st.markdown(
            f"""
            <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value_txt}</div>
            <div class="metric-delta {cls}">{arrow} {pct_txt}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
        return

    pct = _pct_change(curr, prev)
    if curr > prev:
        arrow = "▲"
        cls = "metric-delta-up"
    elif curr < prev:
        arrow = "▼"
        cls = "metric-delta-down"
    else:
        arrow = "—"
        cls = "metric-delta-flat"

    sign = "+" if pct > 0 else ""
    pct_txt = f"{sign}{pct*100:.0f}%"

    st.markdown(
        f"""
        <div class="metric-card">
          <div class="metric-label">{label}</div>
          <div class="metric-value-row">
            <div class="metric-value">{value_txt}</div>
            <div class="{cls}">{arrow} {pct_txt}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )


# Auto-refresh
if "refresh_counter" not in st.session_state:
    st.session_state.refresh_counter = 0


def main():
    logo_path = "./components/ALTA-ICON-CIRCLE-(WHITE).png"

    if os.path.exists(logo_path):
        with open(logo_path, "rb") as f:
            logo_data = base64.b64encode(f.read()).decode()

        st.markdown(
            f"""
            <div class="logo-wrap">
                <img class="logo-img" src="data:image/png;base64,{logo_data}" width="160" />
            </div>
            """,
            unsafe_allow_html=True
        )

    st.markdown("# ALTA MUSIC GROUP")

    st.markdown(
        f"<p>Last 7 Days • Updated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>",
        unsafe_allow_html=True
    )

    try:
        metrics = get_overall_metrics()
        artists = get_artist_leaderboard()

        st.markdown("<hr style='margin: 2.0rem 0;'>", unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            render_metric_card("Total Streams", metrics["curr_total_streams"], metrics["prev_total_streams"], is_int=True, show_delta=True)
        with col2:
            render_metric_card("Listeners", metrics["curr_total_listeners"], metrics["prev_total_listeners"], is_int=True, show_delta=True)
        with col3:
            render_metric_card("TikTok Views", metrics["curr_total_tiktok_views"], metrics["prev_total_tiktok_views"], is_int=True, show_delta=True)
        with col4:
            render_metric_card("TikTok Creations", metrics["curr_total_tiktok_creations"], metrics["prev_total_tiktok_creations"], is_int=True, show_delta=True)

        # Second row (2 metrics, centered)
        st.markdown("<hr style='margin: 2.0rem 0;'>", unsafe_allow_html=True)
        col_l, c1, c2, col_r = st.columns([1, 2, 2, 1])

        with c1:
            render_metric_card("Active Artists", metrics["curr_total_artists"], show_delta=False)

        with c2:
            render_metric_card("Active Tracks", metrics["curr_total_tracks"], show_delta=False)

        # Artist leaderboard (unchanged)
        st.markdown("<hr style='margin: 0.5rem 0;'>", unsafe_allow_html=True)
        st.markdown("## Top Artists (Last 7 Days)")

        import streamlit.components.v1 as components

        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
        <style>
        body {
            background-color: #000000;
            margin: 0;
            padding: 0;
            font-family: 'Special Gothic', sans-serif;
        }
        .custom-table {
            width: 100%;
            background-color: #000000 !important;
            color: #FFFFFF !important;
            border-collapse: collapse;
            font-size: 1.1rem;
            margin: 0;
        }
        .custom-table th {
            background-color: #000000 !important;
            color: #FFFFFF !important;
            padding: 10px 15px;
            text-align: left;
            border-bottom: 2px solid #FFFFFF;
            font-weight: bold;
            font-size: 1.2rem;
        }
        .custom-table td {
            background-color: #000000 !important;
            color: #FFFFFF !important;
            padding: 8px 15px;
            border-bottom: 1px solid #333333;
        }
        .custom-table tr:hover td {
            background-color: #1A1A1A !important;
        }
        .rank-col {
            width: 50px;
            text-align: center;
        }
        </style>
        </head>
        <body>
        <table class="custom-table">
        <thead>
        <tr>
            <th class="rank-col">#</th>
            <th>Artist</th>
            <th>Streams</th>
            <th>TikTok Views</th>
        </tr>
        </thead>
        <tbody>
        """

        for idx in range(min(10, len(artists))):
            row = artists.iloc[idx]
            html_content += f"""
            <tr>
                <td class="rank-col">{idx + 1}</td>
                <td>{row['ARTIST_NAME']}</td>
                <td>{int(row['STREAMS']):,}</td>
                <td>{int(row['TIKTOK_VIEWS']):,}</td>
            </tr>
            """

        html_content += """
        </tbody>
        </table>
        </body>
        </html>
        """

        components.html(html_content, height=450, scrolling=False)

        st.markdown(
            "<p style='margin-top: 0.5rem !important;'>Dashboard auto-refreshes every 5 minutes</p>",
            unsafe_allow_html=True
        )

    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Please check your Snowflake connection settings.")

    time.sleep(300)
    st.session_state.refresh_counter += 1
    st.rerun()


if __name__ == "__main__":
    main()
