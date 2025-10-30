# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import streamlit as st
import pandas as pd
import time
import sys
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import os
import logging
from datetime import datetime
from azure.identity import AzureCliCredential
from openmirroring_operations import OpenMirroringClient
from metric_operations import MetricOperationsClient

# Parse command line arguments
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))

# Configuration from command line arguments
host_root_fqdn = args.get('--host-root-fqdn', 'https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/f0a2c69e-ad20-4cd1-b35b-409776de3d66')
schema_name = args.get('--schema-name', 'microsoft')
table_name = args.get('--table-name', 'employees')
poll_interval = int(args.get('--poll', 30))
metrics_to_plot = args.get('--metrics', 'latest_parquet_file_landing_zone_size_mb,latest_parquet_file_tables_size_mb,latest_delta_committed_file_size_mb').split(',')

# Setup logging
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - [%(module)s.%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s",
)

st.set_page_config(
    page_title="Open Mirroring Metrics Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📊 Open Mirroring Metrics Monitor")
st.markdown(f"**Table**: `{schema_name}.{table_name}` | **Poll Interval**: {poll_interval}s")

@st.cache_resource
def get_db_connection():
    """Initialize DuckDB connection for storing metrics data."""
    db_path = os.path.join(os.getcwd(), 'mirroring_metrics.db')
    conn = duckdb.connect(db_path)
    return conn

@st.cache_resource
def get_metric_client():
    """Initialize the metric operations client."""
    logger = logging.getLogger(__name__)
    credential = AzureCliCredential()
    mirroring_client = OpenMirroringClient(credential=credential, host=host_root_fqdn, logger=logger)
    return MetricOperationsClient(mirroring_client=mirroring_client, host=host_root_fqdn, logger=logger)

def create_metrics_table():
    """Create the metrics table if it doesn't exist."""
    conn = get_db_connection()
    
    table_exists = conn.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='metrics_data'
    """).fetchone()
    
    if not table_exists:
        conn.execute("""
            CREATE TABLE metrics_data (
                timestamp TIMESTAMP,
                schema_name VARCHAR,
                table_name VARCHAR,
                metric_name VARCHAR,
                metric_value VARCHAR,
                metric_value_numeric DOUBLE
            )
        """)
        conn.execute("""
            CREATE INDEX idx_metrics_timestamp ON metrics_data(timestamp)
        """)
        conn.execute("""
            CREATE INDEX idx_metrics_name ON metrics_data(metric_name)
        """)

def collect_and_store_metrics():
    """Collect metrics and store them in the database."""
    try:
        metric_client = get_metric_client()
        all_metrics = metric_client.get_all_metrics(schema_name=schema_name, table_name=table_name)
        
        conn = get_db_connection()
        current_time = datetime.now()
        
        # Insert metrics into database
        for metric_name, metric_value in all_metrics.items():
            # Try to convert to numeric for plotting
            try:
                metric_value_numeric = float(metric_value) if isinstance(metric_value, (int, float)) or (isinstance(metric_value, str) and metric_value.replace('.', '', 1).isdigit()) else None
            except (ValueError, TypeError):
                metric_value_numeric = None
            
            conn.execute("""
                INSERT INTO metrics_data 
                (timestamp, schema_name, table_name, metric_name, metric_value, metric_value_numeric)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [current_time, schema_name, table_name, metric_name, str(metric_value), metric_value_numeric])
        
        # Clean up old data (keep last 7 days)
        conn.execute("""
            DELETE FROM metrics_data 
            WHERE timestamp < now() - INTERVAL 7 DAY
        """)
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def get_metric_history(metric_name, hours=24):
    """Get historical data for a specific metric."""
    conn = get_db_connection()
    df = conn.execute(f"""
        SELECT timestamp, metric_value, metric_value_numeric
        FROM metrics_data 
        WHERE metric_name = ? 
        AND schema_name = ?
        AND table_name = ?
        AND timestamp >= now() - INTERVAL {hours} HOUR
        ORDER BY timestamp ASC
    """, [metric_name, schema_name, table_name]).fetchdf()
    
    return df

def get_available_metrics():
    """Get list of available metrics from the database."""
    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT DISTINCT metric_name
            FROM metrics_data 
            WHERE schema_name = ? AND table_name = ?
            ORDER BY metric_name
        """, [schema_name, table_name]).fetchdf()
        return df['metric_name'].tolist() if not df.empty else []
    except:
        return []

def format_metric_name(metric_name):
    """Format metric name for display."""
    return metric_name.replace('_', ' ').title()

def create_metric_chart(metric_name, df):
    """Create a plotly chart for the metric."""
    if df.empty:
        return None
    
    # Determine if this is a numeric metric that can be plotted as a line
    numeric_data = df[df['metric_value_numeric'].notna()]
    
    if not numeric_data.empty:
        # Line chart for numeric data
        fig = px.line(
            numeric_data, 
            x='timestamp', 
            y='metric_value_numeric',
            title=f"{format_metric_name(metric_name)}",
            labels={'metric_value_numeric': 'Value', 'timestamp': 'Time'}
        )
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Value",
            showlegend=False,
            height=400
        )
    else:
        # Text display for non-numeric data
        latest_value = df.iloc[-1]['metric_value'] if not df.empty else 'No data'
        fig = go.Figure()
        fig.add_annotation(
            text=f"Latest: {latest_value}",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=20)
        )
        fig.update_layout(
            title=f"{format_metric_name(metric_name)}",
            showlegend=False,
            height=200,
            xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
            yaxis=dict(showgrid=False, showticklabels=False, zeroline=False)
        )
    
    return fig

# Initialize database
create_metrics_table()

# Sidebar configuration
st.sidebar.header("Configuration")
st.sidebar.write(f"**Host**: {host_root_fqdn}")
st.sidebar.write(f"**Schema**: {schema_name}")
st.sidebar.write(f"**Table**: {table_name}")
st.sidebar.write(f"**Poll Interval**: {poll_interval}s")

# Time range selector
time_range = st.sidebar.selectbox(
    "Time Range",
    options=[1, 6, 12, 24, 48, 168],  # hours
    format_func=lambda x: f"{x} hour{'s' if x > 1 else ''}" if x < 24 else f"{x//24} day{'s' if x//24 > 1 else ''}",
    index=3  # Default to 24 hours
)

# Metrics selector
available_metrics = get_available_metrics()
if available_metrics:
    default_metrics = [m for m in metrics_to_plot if m in available_metrics]
    if not default_metrics:
        default_metrics = available_metrics[:3]  # Show first 3 if none match
    
    selected_metrics = st.sidebar.multiselect(
        "Select Metrics to Display",
        options=available_metrics,
        default=default_metrics
    )
else:
    selected_metrics = metrics_to_plot
    st.sidebar.info("No historical data available yet. Default metrics will be shown after first collection.")

# Auto-refresh toggle
auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)

# Manual refresh button
if st.sidebar.button("Refresh Now"):
    st.rerun()

# Status area
status_placeholder = st.empty()
metrics_container = st.container()

# Main monitoring loop
if auto_refresh:
    # Collect metrics
    with status_placeholder:
        with st.spinner("Collecting metrics..."):
            success, error = collect_and_store_metrics()
        
        if success:
            st.success(f"✅ Metrics collected at {datetime.now().strftime('%H:%M:%S')}")
        else:
            st.error(f"❌ Error collecting metrics: {error}")

# Display metrics
with metrics_container:
    if selected_metrics:
        # Create columns for metrics layout
        cols = st.columns(min(len(selected_metrics), 2))  # Max 2 columns
        
        for i, metric_name in enumerate(selected_metrics):
            col = cols[i % 2]
            
            with col:
                df = get_metric_history(metric_name, time_range)
                
                if not df.empty:
                    fig = create_metric_chart(metric_name, df)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No data available for {format_metric_name(metric_name)}")
    else:
        st.info("Select metrics from the sidebar to display charts")

# Display recent metrics table
st.subheader("Recent Metrics Summary")
try:
    conn = get_db_connection()
    recent_df = conn.execute("""
        SELECT metric_name, metric_value, timestamp
        FROM (
            SELECT metric_name, metric_value, timestamp,
                   ROW_NUMBER() OVER (PARTITION BY metric_name ORDER BY timestamp DESC) as rn
            FROM metrics_data
            WHERE schema_name = ? AND table_name = ?
        ) ranked
        WHERE rn = 1
        ORDER BY metric_name
    """, [schema_name, table_name]).fetchdf()
    
    if not recent_df.empty:
        recent_df['metric_name'] = recent_df['metric_name'].apply(format_metric_name)
        recent_df.columns = ['Metric', 'Value', 'Last Updated']
        st.dataframe(recent_df, use_container_width=True)
    else:
        st.info("No metrics data available yet")
except Exception as e:
    st.error(f"Error loading recent metrics: {e}")

# Auto-refresh mechanism
if auto_refresh:
    time.sleep(poll_interval)
    st.rerun()