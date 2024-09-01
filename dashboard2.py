import streamlit as st
from snowflake.connector import connect
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from dotenv import load_dotenv


load_dotenv()

# Function to get the connection to Snowflake
def get_snowflake_connection():
    return connect(
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        account=os.getenv('ACCOUNT'),
        warehouse=st.session_state.warehouse,
        database=st.session_state.database,
        schema=st.session_state.schema
    )

# Function to get failsafe bytes
def get_failsafe_bytes(conn):
    query = """
    SELECT FAILSAFE_BYTES
    FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
    WHERE TABLE_NAME = '{}'
    """.format(st.session_state.get('table'))
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else None

# Function to get retention time
def get_retention_time(conn):
    query = """
    SELECT RETENTION_TIME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = '{}'
    """.format(st.session_state.get('table'))
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else None

# Function to get time travel bytes
def get_time_travel_bytes(conn):
    query = """
    SELECT TIME_TRAVEL_BYTES
    FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
    WHERE TABLE_NAME = '{}'
    """.format(st.session_state.get('table'))
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else None

# Function to get DML history
def get_dml_history(conn):
    query = """
    SELECT START_TIME, ROWS_ADDED, ROWS_UPDATED, ROWS_REMOVED
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_DML_HISTORY
    WHERE TABLE_NAME = '{}'
    """.format(st.session_state.get('table'))
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(result, columns=['START_TIME', 'ROWS_ADDED', 'ROWS_UPDATED', 'ROWS_REMOVED'])
    return df

# Function to get pruning history
def get_pruning_history(conn):
    query = """
    SELECT START_TIME, NUM_SCANS, ROWS_SCANNED, ROWS_PRUNED, PARTITIONS_PRUNED, PARTITIONS_SCANNED
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_PRUNING_HISTORY
    WHERE TABLE_NAME = '{}'
    """.format(st.session_state.get('table'))
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(result, columns=['START_TIME', 'NUM_SCANS', 'ROWS_SCANNED', 'ROWS_PRUNED', 'PARTITIONS_PRUNED', 'PARTITIONS_SCANNED'])
    return df

# Function to plot DML history
def plot_dml_history(df, time_interval):
    fig, ax = plt.subplots(figsize=(12, 6))
    if not df.empty:
        # Convert START_TIME to datetime
        df['START_TIME'] = pd.to_datetime(df['START_TIME'])
        df.set_index('START_TIME', inplace=True)
        
        # Resample based on user choice
        if time_interval == 'Weekly':
            df_resampled = df.resample('W').sum()
            x_label_format = mdates.DateFormatter('%Y-%m-%d')
            x_locator = mdates.WeekdayLocator()
        elif time_interval == 'Daily':
            df_resampled = df.resample('D').sum()
            x_label_format = mdates.DateFormatter('%Y-%m-%d')
            x_locator = mdates.DayLocator()
        
        ax.plot(df_resampled.index, df_resampled['ROWS_ADDED'], label='Rows Added', color='green')
        ax.plot(df_resampled.index, df_resampled['ROWS_UPDATED'], label='Rows Updated', color='blue')
        ax.plot(df_resampled.index, df_resampled['ROWS_REMOVED'], label='Rows Removed', color='red')
        
        # Formatting the x-axis based on user choice
        ax.xaxis.set_major_locator(x_locator)
        ax.xaxis.set_major_formatter(x_label_format)
        fig.autofmt_xdate()
        
        ax.set_xlabel('Time')
        ax.set_ylabel('Number of Rows')
        ax.set_title(f'DML Operations Over Time ({time_interval})')
        ax.legend()
        
    else:
        ax.text(0.5, 0.5, "No DML history data available for the selected table.", horizontalalignment='center', verticalalignment='center')
    
    return fig

# Function to plot pruning history
def plot_pruning_history(df, time_interval):
    fig, ax = plt.subplots(figsize=(12, 6))
    if not df.empty:
        # Convert START_TIME to datetime
        df['START_TIME'] = pd.to_datetime(df['START_TIME'])
        df.set_index('START_TIME', inplace=True)
        
        # Resample based on user choice
        if time_interval == 'Weekly':
            df_resampled = df.resample('W').sum()
            x_label_format = mdates.DateFormatter('%Y-%m-%d')
            x_locator = mdates.WeekdayLocator()
        elif time_interval == 'Daily':
            df_resampled = df.resample('D').sum()
            x_label_format = mdates.DateFormatter('%Y-%m-%d')
            x_locator = mdates.DayLocator()
        
        ax.plot(df_resampled.index, df_resampled['NUM_SCANS'], label='Number of Scans', color='purple')
        ax.plot(df_resampled.index, df_resampled['ROWS_SCANNED'], label='Rows Scanned', color='orange')
        ax.plot(df_resampled.index, df_resampled['ROWS_PRUNED'], label='Rows Pruned', color='cyan')
        ax.plot(df_resampled.index, df_resampled['PARTITIONS_PRUNED'], label='Partition Pruned', color='magenta')
        ax.plot(df_resampled.index, df_resampled['PARTITIONS_SCANNED'], label='Partition Scanned', color='brown')
        
        # Formatting the x-axis based on user choice
        ax.xaxis.set_major_locator(x_locator)
        ax.xaxis.set_major_formatter(x_label_format)
        fig.autofmt_xdate()
        
        ax.set_xlabel('Time')
        ax.set_ylabel('Counts')
        ax.set_title(f'Table Pruning Operations Over Time ({time_interval})')
        ax.legend()
        
    else:
        ax.text(0.5, 0.5, "No pruning history data available for the selected table.", horizontalalignment='center', verticalalignment='center')
    
    return fig

# Function to display Dashboard 2
def show_dashboard2():
    st.header("Data Statistics")

    # Apply CSS for KPI styling
    st.markdown("""
        <style>
        .kpi-box {
            border: 2px solid #2c2c2c;
            border-radius: 10px;
            padding: 10px;
            background-color: white;
            color: black;
            font-size: 1em;
            text-align: center;
            height: 130px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            box-shadow: 2px 2px 8px rgba(0, 0, 0, 0.1);
        }
        .kpi-box h3 {
            font-size: 1.2em;
            margin: 0;
        }
        .kpi-box p {
            margin: 0;
            font-size: 1.8em;
        }
        </style>
    """, unsafe_allow_html=True)

    conn = get_snowflake_connection()

    # Retrieve the metrics
    failsafe_bytes = get_failsafe_bytes(conn)
    retention_time = get_retention_time(conn)
    time_travel_bytes = get_time_travel_bytes(conn)
    
    # Retrieve DML history
    dml_history_df = get_dml_history(conn)
    
    # Retrieve pruning history
    pruning_history_df = get_pruning_history(conn)

    conn.close()

    # Display the KPIs
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(f'<div class="kpi-box"><h3>Failsafe Bytes</h3><p>{failsafe_bytes} bytes</p></div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown(f'<div class="kpi-box"><h3>Retention Time</h3><p>{retention_time} days</p></div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown(f'<div class="kpi-box"><h3>Time Travel Bytes</h3><p>{time_travel_bytes} bytes</p></div>', unsafe_allow_html=True)

    st.write("")  # Add some space between KPIs and graphs

    # Dropdown menu for selecting time interval
    time_interval = st.selectbox("Select Time Interval for Graphs", ("Daily", "Weekly"))

    # Create two columns for the graphs
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("DML History")
        dml_fig = plot_dml_history(dml_history_df, time_interval)
        st.pyplot(dml_fig)

    with col2:
        st.subheader("Queries Statistics")
        pruning_fig = plot_pruning_history(pruning_history_df, time_interval)
        st.pyplot(pruning_fig)
