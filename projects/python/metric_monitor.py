# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import base64
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

host_root_fqdns = args.get("--host-root-fqdns", "https://msit-onelake.dfs.fabric.microsoft.com/061901d0-4d8b-4c91-b78f-2f11189fe530/f0a2c69e-ad20-4cd1-b35b-409776de3d66").split(",")
schema_names = args.get("--schema-names", "microsoft").split(",")
table_names = args.get("--table-names", "employees").split(",")
poll_interval = int(args.get("--poll", 30))
metrics_to_plot = args.get("--metrics", "lag_seconds_max_timestamp_parquet_file_landing_zone_to_delta_committed_file").split(",")
fabric_sql_connection_string_base64s = args.get("--fabric-sql-connection-string-base64s")
fabric_sql_database_names = args.get("--fabric-sql-database-name")

fabric_sql_connection_strings = []
for b64_conn in fabric_sql_connection_string_base64s.split(","):
    try:
        decoded_conn = base64.b64decode(b64_conn.strip()).decode("utf-8")
        fabric_sql_connection_strings.append(decoded_conn)
    except Exception as e:
        st.error(f"Error decoding base64 connection string: {e}")
        st.stop()

fabric_sql_database_names_list = fabric_sql_database_names.split(",")

if len(host_root_fqdns) != len(schema_names) or len(schema_names) != len(table_names):
    st.error(f"Number of host FQDNs ({len(host_root_fqdns)}), schema names ({len(schema_names)}), and table names ({len(table_names)}) must all match")
    st.stop()

if len(host_root_fqdns) != len(fabric_sql_connection_strings) or len(host_root_fqdns) != len(fabric_sql_database_names_list):
    st.error(f"Number of host FQDNs ({len(host_root_fqdns)}), SQL connection strings ({len(fabric_sql_connection_strings)}), and database names ({len(fabric_sql_database_names_list)}) must all match")
    st.stop()

host_schema_table_triplets = list(zip(host_root_fqdns, schema_names, table_names))
schema_table_pairs = list(zip(schema_names, table_names))

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - [%(module)s.%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s",
)

st.set_page_config(page_title="Open Mirroring Metrics Monitor", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

st.title("📊 Open Mirroring Metrics Monitor")
table_list = ", ".join([f"`{schema}.{table}`" for schema, table in schema_table_pairs])
st.markdown(f"**Tables**: {table_list} | **Poll Interval**: {poll_interval}s")


@st.cache_resource
def get_db_connection():
    """Initialize DuckDB connection for storing metrics data."""
    db_path = os.path.join(os.getcwd(), "mirroring_metrics.db")
    conn = duckdb.connect(db_path)
    return conn


@st.cache_resource
def get_metric_clients():
    """Initialize the metric operations clients for all hosts."""
    logger = logging.getLogger(__name__)
    credential = AzureCliCredential()
    clients = {}

    for i, host_fqdn in enumerate(host_root_fqdns):
        mirroring_client = OpenMirroringClient(credential=credential, host=host_fqdn, logger=logger)
        clients[host_fqdn] = MetricOperationsClient(mirroring_client=mirroring_client, host=host_fqdn, logger=logger, fabric_sql_connection_string=fabric_sql_connection_strings[i], fabric_sql_database_name=fabric_sql_database_names_list[i])

    return clients


def create_metrics_table():
    """Create the metrics table if it doesn't exist."""
    conn = get_db_connection()

    table_exists = conn.execute(
        """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='metrics_data'
    """
    ).fetchone()

    if not table_exists:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metrics_data (
                timestamp TIMESTAMP,
                host_fqdn VARCHAR,
                schema_name VARCHAR,
                table_name VARCHAR,
                metric_name VARCHAR,
                metric_value VARCHAR,
                metric_value_numeric DOUBLE
            )
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics_data(timestamp)
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics_data(metric_name)
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_metrics_schema_table ON metrics_data(schema_name, table_name)
        """
        )


def collect_and_store_metrics():
    """Collect only the specified metrics and store them in the database for all schema-table pairs."""

    try:
        collection_start_time = time.time()

        metric_clients = get_metric_clients()
        conn = get_db_connection()
        current_time = datetime.now()

        def collect_single_metric(metric_name, host_fqdn, schema_name, table_name):
            """Collect a single metric for a specific host-schema-table triplet."""
            try:
                metric_client = metric_clients[host_fqdn]
                metric_value = metric_client.get_metric(metric_name, schema_name=schema_name, table_name=table_name)

                try:
                    metric_value_numeric = float(metric_value) if isinstance(metric_value, (int, float)) or (isinstance(metric_value, str) and metric_value.replace(".", "", 1).isdigit()) else None
                except (ValueError, TypeError):
                    metric_value_numeric = None

                return (host_fqdn, schema_name, table_name, metric_name, metric_value, metric_value_numeric)

            except Exception as e:
                return (host_fqdn, schema_name, table_name, metric_name, None, None)

        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(metrics_to_plot) * len(host_schema_table_triplets), 20)) as executor:
            for host_fqdn, schema_name, table_name in host_schema_table_triplets:
                for metric_name in metrics_to_plot:
                    future = executor.submit(collect_single_metric, metric_name, host_fqdn, schema_name, table_name)
                    futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                host_fqdn, schema_name, table_name, metric_name, metric_value, metric_value_numeric = future.result()

                if metric_value is not None:
                    conn.execute(
                        """
                        INSERT INTO metrics_data 
                        (timestamp, host_fqdn, schema_name, table_name, metric_name, metric_value, metric_value_numeric)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        [current_time, host_fqdn, schema_name, table_name, metric_name, str(metric_value), metric_value_numeric],
                    )

        collection_end_time = time.time()
        collection_duration = collection_end_time - collection_start_time

        for host_fqdn, schema_name, table_name in host_schema_table_triplets:
            conn.execute(
                """
                INSERT INTO metrics_data 
                (timestamp, host_fqdn, schema_name, table_name, metric_name, metric_value, metric_value_numeric)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                [current_time, host_fqdn, schema_name, table_name, "metric_collection_duration", f"{collection_duration:.2f}s", collection_duration],
            )

        conn.execute(
            """
            DELETE FROM metrics_data 
            WHERE timestamp < now() - INTERVAL 7 DAY
        """
        )

        return True, None

    except Exception as e:
        return False, str(e)


def format_metric_name(metric_name):
    """Format metric name for display."""
    return metric_name.replace("_", " ").title()


def get_metrics_data_for_table(schema_name, table_name, hours=24):
    """Get historical data for all specified metrics for a specific schema-table pair."""
    conn = get_db_connection()
    metrics_to_plot_with_duration = metrics_to_plot + ["metric_collection_duration"]
    placeholders = ",".join(["?" for _ in metrics_to_plot_with_duration])

    df = conn.execute(
        f"""
        SELECT timestamp, host_fqdn, schema_name, table_name, metric_name, metric_value, metric_value_numeric
        FROM metrics_data 
        WHERE metric_name IN ({placeholders})
        AND schema_name = ?
        AND table_name = ?
        AND timestamp >= now() - INTERVAL {hours} HOUR
        ORDER BY timestamp ASC, metric_name
    """,
        metrics_to_plot_with_duration + [schema_name, table_name],
    ).fetchdf()

    return df


def get_all_metrics_data(hours=24):
    """Get historical data for all specified metrics for all schema-table pairs."""
    conn = get_db_connection()
    metrics_to_plot_with_duration = metrics_to_plot + ["metric_collection_duration"]
    placeholders = ",".join(["?" for _ in metrics_to_plot_with_duration])

    df = conn.execute(
        f"""
        SELECT timestamp, host_fqdn, schema_name, table_name, metric_name, metric_value, metric_value_numeric
        FROM metrics_data 
        WHERE metric_name IN ({placeholders})
        AND timestamp >= now() - INTERVAL {hours} HOUR
        ORDER BY timestamp ASC, schema_name, table_name, metric_name
    """,
        metrics_to_plot_with_duration,
    ).fetchdf()

    return df


def create_chart_for_table(df, schema_name, table_name):
    """Create a chart for a specific schema-table pair."""
    if df.empty:
        return None

    numeric_df = df[df["metric_value_numeric"].notna()]
    non_numeric_df = df[df["metric_value_numeric"].isna()]

    fig = go.Figure()

    if not numeric_df.empty:
        for metric_name in numeric_df["metric_name"].unique():
            metric_data = numeric_df[numeric_df["metric_name"] == metric_name]
            fig.add_trace(go.Scatter(x=metric_data["timestamp"], y=metric_data["metric_value_numeric"], mode="lines+markers", name=format_metric_name(metric_name), line=dict(width=2)))

    if not non_numeric_df.empty:
        y_position = 0.95
        for metric_name in non_numeric_df["metric_name"].unique():
            latest_value = non_numeric_df[non_numeric_df["metric_name"] == metric_name].iloc[-1]["metric_value"]
            fig.add_annotation(text=f"{format_metric_name(metric_name)}: {latest_value}", xref="paper", yref="paper", x=0.02, y=y_position, showarrow=False, font=dict(size=12), align="left")
            y_position -= 0.05

    fig.update_layout(title=f"Open Mirroring Metrics - {schema_name}.{table_name}", xaxis_title="Time", yaxis_title="Value", height=500, showlegend=True, legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02))

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

any_data = False
for schema_name, table_name in schema_table_pairs:
    st.subheader(f"📊 {schema_name}.{table_name}")

    df = get_metrics_data_for_table(schema_name, table_name, time_range)

    if not df.empty:
        any_data = True
        fig = create_chart_for_table(df, schema_name, table_name)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No metrics data available yet for {schema_name}.{table_name}. Data will appear after first collection.")

if not any_data:
    st.info("No metrics data available yet. Data will appear after first collection.")

if auto_refresh:
    time.sleep(poll_interval)
    st.rerun()
