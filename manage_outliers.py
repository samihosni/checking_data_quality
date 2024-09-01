import streamlit as st
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from config import get_id_column

load_dotenv()


def show_manage_outliers():
    st.header("Outliers Overview")

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
        .separator {
            border-top: 1px solid #2c2c2c;
            margin: 20px 0;
        }
        .table-container {
            margin-top: 30px;
        }
        </style>
    """, unsafe_allow_html=True)

    if st.session_state.get('data_loaded', False):
        conn = snowflake.connector.connect(
            user=os.getenv('USER'),
            password=os.getenv('PASSWORD'),
            account=os.getenv('ACCOUNT'),
            warehouse=st.session_state.warehouse,
            database=st.session_state.database,
            schema=st.session_state.schema
        )
        
        cursor = conn.cursor()

        query_columns = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{st.session_state.table}'
        """
        cursor.execute(query_columns)
        columns = [row[0] for row in cursor.fetchall()]

        id_column = get_id_column()

        nulls_query = "SELECT "
        nulls_query += ", ".join([f"SUM(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END) AS \"{col}_NULL_COUNT\"" for col in columns])
        nulls_query += f" FROM {st.session_state.table};"

        cursor.execute(nulls_query)
        nulls_result = cursor.fetchone()

        if nulls_result:
            total_nulls = sum(nulls_result)

            nulls_percentages = {
                columns[i]: (nulls_result[i] / total_nulls * 100) if total_nulls > 0 else 0
                for i in range(len(columns))
            }

            # KPI - Pourcentage de remplissage
            total_values = len(columns) * total_nulls  # Nombre total de cellules
            filled_percentage = ((total_values - total_nulls) / total_values) * 100 if total_values > 0 else 0

            # KPI - Top colonne avec des valeurs nulles
            top_null_column = columns[nulls_result.index(max(nulls_result))] if nulls_result else None
            
            df_nulls = pd.DataFrame(list(nulls_percentages.items()), columns=['Column', 'Null Percentage'])

            case_insensitive_columns = ", ".join([f"LOWER(\"{col}\")" for col in columns if col != id_column])
            duplicates_query = f"""
            SELECT COUNT(*) AS DUPLICATE_COUNT
            FROM (
                SELECT *, COUNT(*) OVER (PARTITION BY {case_insensitive_columns}) AS Count
                FROM {st.session_state.table}
            ) WHERE Count > 1;
            """

            cursor.execute(duplicates_query)
            duplicates_result = cursor.fetchone()[0]

            total_rows_query = f"SELECT COUNT(*) FROM {st.session_state.table};"
            cursor.execute(total_rows_query)
            total_rows = cursor.fetchone()[0]

            duplicate_percentage = (duplicates_result / total_rows * 100) if total_rows > 0 else 0

            nulls_detail_query = f"""
            SELECT *, 'NULL' AS OBSERVATION
            FROM {st.session_state.table}
            WHERE {' OR '.join([f"\"{col}\" IS NULL" for col in columns])}
            """
            nulls_df = pd.read_sql(nulls_detail_query, conn)

            duplicates_detail_query = f"""
            SELECT *, 'DUPLICATED' AS OBSERVATION
            FROM (
                SELECT *, COUNT(*) OVER (PARTITION BY {case_insensitive_columns}) AS Count
                FROM {st.session_state.table}
            ) WHERE Count > 1
            """
            duplicates_df = pd.read_sql(duplicates_detail_query, conn)

            combined_df = pd.concat([nulls_df, duplicates_df]).drop_duplicates(ignore_index=True)

            combined_df['DELETE'] = False
            combined_df = combined_df.where(pd.notnull(combined_df), 'null')
            combined_df.index += 1

            def highlight_null_cells(value):
                return 'background-color: red' if value == 'null' else ''

            def highlight_duplicate_rows(row):
                if row['OBSERVATION'] == 'DUPLICATED':
                    return ['background-color: yellow' if value != 'null' else 'background-color: red' for value in row]
                return [''] * len(row)

            st.markdown('<div class="separator"></div>', unsafe_allow_html=True)

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.markdown(f'<div class="kpi-box"><h3>Total Null Values</h3><p>{int(total_nulls)}</p></div>', unsafe_allow_html=True)

            with col2:
                st.markdown(f'<div class="kpi-box"><h3>Total Duplicate Rows</h3><p>{int(duplicates_result)}</p></div>', unsafe_allow_html=True)

            with col3:
                st.markdown(f'<div class="kpi-box"><h3>Filled Percentage</h3><p>{filled_percentage:.2f}%</p></div>', unsafe_allow_html=True)

            with col4:
                st.markdown(f'<div class="kpi-box"><h3>Top Null Column</h3><p>{top_null_column}</p></div>', unsafe_allow_html=True)

            st.markdown('<div class="separator"></div>', unsafe_allow_html=True)

            col1, col2 = st.columns([1, 1])

            with col1:
                st.subheader("Null Values")
                fig_nulls, ax_nulls = plt.subplots(figsize=(4, 3))
                ax_nulls.bar(df_nulls['Column'], df_nulls['Null Percentage'], color='skyblue')
                ax_nulls.set_xlabel('Column', fontsize=8)
                ax_nulls.set_ylabel('Null Percentage (%)', fontsize=8)
                ax_nulls.set_title('Null Values Percentage by Column', fontsize=10)
                plt.xticks(rotation=45, ha='right', fontsize=6)
                plt.yticks(fontsize=6)
                st.pyplot(fig_nulls)

            with col2:
                st.subheader("Duplicate Rows")
                fig_pie, ax_pie = plt.subplots(figsize=(4, 3))
                ax_pie.pie([duplicates_result, total_rows - duplicates_result], 
                           labels=['Duplicates', 'Non-Duplicates'], 
                           autopct='%1.1f%%', 
                           colors=['#ff9999','#66b3ff'],
                           textprops={'fontsize': 6})
                ax_pie.axis('equal')
                plt.title('Percentage of Duplicate Rows', fontsize=10)
                plt.legend(fontsize=6)
                st.pyplot(fig_pie)

            st.markdown('<div class="table-container">', unsafe_allow_html=True)
            st.subheader("Outliers Values")
            outliers_df = combined_df.drop(columns=['COUNT', 'DELETE'], errors='ignore')
            styled_outliers_df = outliers_df.style.applymap(highlight_null_cells).apply(highlight_duplicate_rows, axis=1)
            st.dataframe(styled_outliers_df, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

            st.subheader("Edit Table Data")
            editable_df = combined_df.copy().drop(columns=['COUNT'], errors='ignore')
            aggrid_response = AgGrid(
                editable_df,
                editable=True,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                fit_columns_on_grid_load=True,
                allow_unsafe_jscode=True
            )

            modified_df = aggrid_response['data']
            conn = snowflake.connector.connect(
                user='samihosni',
                password='0549757418Sam.',
                account='aob93227.us-west-2',
                warehouse=st.session_state.warehouse,
                database=st.session_state.database,
                schema=st.session_state.schema
            )

            if st.button("Save Changes"):
                for index, row in modified_df.iterrows():
                    if row['DELETE']:
                        delete_query = f"DELETE FROM {st.session_state.table} WHERE {id_column} = '{row[id_column]}';"
                        conn.cursor().execute(delete_query)
                    else:
                        update_query = f"UPDATE {st.session_state.table} SET {', '.join([f'\"{col}\" = \'{row[col]}\'' for col in columns])} WHERE {id_column} = '{row[id_column]}';"
                        conn.cursor().execute(update_query)

                st.success("Changes saved successfully !")