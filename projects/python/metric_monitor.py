# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import concurrent.futures
import duckdb
import logging
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import sys
import threading
import time

from datetime import datetime
from azure.identity import AzureCliCredential
from openmirroring_operations import OpenMirroringClient
from metric_operations import MetricOperationsClient

args = dict(zip(sys.argv[1::2], sys.argv[2::2]))

host_root_fqdn = args.get('--host-root-fqdn', 'https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/f0a2c69e-ad20-4cd1-b35b-409776de3d66')
schema_name = args.get('--schema-name', 'microsoft')
table_name = args.get('--table-name', 'employees')
poll_interval = int(args.get('--poll', 30))
metrics_to_plot = args.get('--metrics', 'latest_parquet_file_landing_zone_size_mb,latest_parquet_file_tables_size_mb,latest_delta_committed_file_size_mb').split(',')

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - [%(module)s.%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s",
)

st.set_page_config(
    page_title="Open Mirroring Metrics Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
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
            CREATE TABLE IF NOT EXISTS metrics_data (
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
    """Collect only the specified metrics and store them in the database."""
    
    try:
        collection_start_time = time.time()
        
        metric_client = get_metric_client()
        conn = get_db_connection()
        current_time = datetime.now()
        
        def collect_single_metric(metric_name):
            """Collect a single metric."""
            try:
                metric_value = metric_client.get_metric(metric_name, schema_name=schema_name, table_name=table_name)
                
                try:
                    metric_value_numeric = float(metric_value) if isinstance(metric_value, (int, float)) or (isinstance(metric_value, str) and metric_value.replace('.', '', 1).isdigit()) else None
                except (ValueError, TypeError):
                    metric_value_numeric = None
                
                return (metric_name, metric_value, metric_value_numeric)
                
            except Exception as e:
                return (metric_name, None, None)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(metrics_to_plot), 10)) as executor:
            future_to_metric = {executor.submit(collect_single_metric, metric): metric for metric in metrics_to_plot}
            
            for future in concurrent.futures.as_completed(future_to_metric):
                metric_name, metric_value, metric_value_numeric = future.result()
                
                if metric_value is not None:
                    conn.execute("""
                        INSERT INTO metrics_data 
                        (timestamp, schema_name, table_name, metric_name, metric_value, metric_value_numeric)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [current_time, schema_name, table_name, metric_name, str(metric_value), metric_value_numeric])
        
        collection_end_time = time.time()
        collection_duration = collection_end_time - collection_start_time
        
        conn.execute("""
            INSERT INTO metrics_data 
            (timestamp, schema_name, table_name, metric_name, metric_value, metric_value_numeric)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [current_time, schema_name, table_name, "metric_collection_time", f"{collection_duration:.2f}s", collection_duration])
        
        conn.execute("""
            DELETE FROM metrics_data 
            WHERE timestamp < now() - INTERVAL 7 DAY
        """)
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def format_metric_name(metric_name):
    """Format metric name for display."""
    return metric_name.replace('_', ' ').title()

def get_combined_metrics_data(hours=24):
    """Get historical data for all specified metrics combined."""
    conn = get_db_connection()
    placeholders = ','.join(['?' for _ in metrics_to_plot])
    
    df = conn.execute(f"""
        SELECT timestamp, metric_name, metric_value, metric_value_numeric
        FROM metrics_data 
        WHERE metric_name IN ({placeholders})
        AND schema_name = ?
        AND table_name = ?
        AND timestamp >= now() - INTERVAL {hours} HOUR
        ORDER BY timestamp ASC, metric_name
    """, metrics_to_plot + [schema_name, table_name]).fetchdf()
    
    return df

def create_combined_chart(df):
    """Create a single chart with all metrics and a legend."""
    if df.empty:
        return None
    
    numeric_df = df[df['metric_value_numeric'].notna()]
    non_numeric_df = df[df['metric_value_numeric'].isna()]
    
    fig = go.Figure()
    
    if not numeric_df.empty:
        for metric_name in numeric_df['metric_name'].unique():
            metric_data = numeric_df[numeric_df['metric_name'] == metric_name]
            fig.add_trace(go.Scatter(
                x=metric_data['timestamp'],
                y=metric_data['metric_value_numeric'],
                mode='lines+markers',
                name=format_metric_name(metric_name),
                line=dict(width=2)
            ))
    
    if not non_numeric_df.empty:
        y_position = 0.95
        for metric_name in non_numeric_df['metric_name'].unique():
            latest_value = non_numeric_df[non_numeric_df['metric_name'] == metric_name].iloc[-1]['metric_value']
            fig.add_annotation(
                text=f"{format_metric_name(metric_name)}: {latest_value}",
                xref="paper", yref="paper",
                x=0.02, y=y_position,
                showarrow=False,
                font=dict(size=12),
                align="left"
            )
            y_position -= 0.05
    
    fig.update_layout(
        title="Open Mirroring Metrics",
        xaxis_title="Time",
        yaxis_title="Value",
        height=500,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        )
    )
    
    return fig

create_metrics_table()

time_range = 24
auto_refresh = True

status_placeholder = st.empty()

if auto_refresh:
    with status_placeholder:
        with st.spinner("Collecting metrics..."):
            success, error = collect_and_store_metrics()
        
        if success:
            st.success(f"✅ Metrics collected at {datetime.now().strftime('%H:%M:%S')}")
        else:
            st.error(f"❌ Error collecting metrics: {error}")

df = get_combined_metrics_data(time_range)

if not df.empty:
    fig = create_combined_chart(df)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No metrics data available yet. Data will appear after first collection.")

if auto_refresh:
    time.sleep(poll_interval)
    st.rerun()