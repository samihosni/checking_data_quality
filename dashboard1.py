import streamlit as st
import snowflake.connector
import numpy as np
import pandas as pd
import decimal
import os
from dotenv import load_dotenv

import plotly.express as px
load_dotenv()


def show_dashboard1():
    st.header("Table Overview")
    
    # Appliquer le style CSS
    st.markdown("""
        <style>
        /* Encadrement de la matrice des KPIs */
        .kpi-matrix {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            justify-content: center;
        }

        /* Encadrement individuel des KPIs */
        .kpi-box {
            border: 2px solid #2c2c2c; /* Bordure gris foncé */
            border-radius: 5px; /* Coins légèrement arrondis */
            padding: 10px;
            background-color: white; /* Fond blanc */
            color: black; /* Couleur du texte */
            font-size: 0.85em; /* Taille de police plus petite */
            text-align: center;
            margin: 5px;
            width: 150px; /* Largeur fixe */
            height: 100px; /* Hauteur fixe */
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
        }

        /* Ajustement automatique de la taille du texte pour les KPIs */
        .kpi-box h3 {
            font-size: 1em; /* Taille du texte pour le label */
            margin: 0;
        }
        
        .kpi-box p {
            margin: 0;
            font-size: 0.9em; /* Taille du texte pour la valeur */
        }
        </style>
    """, unsafe_allow_html=True)

    if st.session_state.get('data_loaded', False):
        # Connexion à Snowflake
        conn = snowflake.connector.connect(
            user=os.getenv('USER'),
            password=os.getenv('PASSWORD'),
            account=os.getenv('ACCOUNT'),
            warehouse=st.session_state.warehouse,
            database=st.session_state.database,
            schema=st.session_state.schema
        )

        # Requête pour récupérer les informations
        query = f"""
        SELECT 
            TABLE_CATALOG AS "Database Name",
            TABLE_SCHEMA AS "Schema Name",
            TABLE_NAME AS "Table Name",
            LAST_ALTERED AS "LAST_ALTERED",
            LAST_DDL AS "LAST_DDL",
            LAST_DDL_BY AS "LAST_DDL_BY", 
            CREATED AS "Created At",
            COMMENT AS "Comment",       
            TABLE_OWNER AS "Table Owner",   
            (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = t.TABLE_NAME) AS "Total Number of Columns",
            ROW_COUNT AS "Total Number of Rows",
            (SELECT COUNT(DISTINCT DATA_TYPE) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = t.TABLE_NAME) AS "Total Number of Types"
        FROM INFORMATION_SCHEMA.TABLES t
        WHERE TABLE_NAME = '{st.session_state.table}'
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()

        # Assurez-vous que la colonne "Total Number of Rows" est convertie en entier
        df['Total Number of Rows'] = df['Total Number of Rows'].astype(int)

        # Créer la liste des types de colonnes pour le pie chart
        types_query = f"""
        SELECT DATA_TYPE, COUNT(*) as count 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{st.session_state.table}' 
        GROUP BY DATA_TYPE
        """
        cursor.execute(types_query)
        types_df = cursor.fetch_pandas_all()

        # Affichage des KPIs dans une matrice 3x4 et un pie chart à droite
        kpi_cols, pie_chart_col = st.columns([2, 1])

        with kpi_cols:
            st.markdown('<div class="kpi-matrix">', unsafe_allow_html=True)
            cols = st.columns(4)

            # Créer une liste de tuples (label, value) à afficher
            metrics = [(column, df[column].values[0]) for column in df.columns]

            # Affichage des KPIs en format 3x4
            for i in range(0, len(metrics), 4):
                with st.container():
                    row = st.columns(4)  # Crée une rangée de 4 colonnes
                    for j in range(4):
                        if i + j < len(metrics):
                            label, value = metrics[i + j]
                            # Convertir les dates en chaînes de caractères
                            if isinstance(value, (np.datetime64, pd.Timestamp)):
                                value = str(value)
                            # Convertir les décimales en float
                            elif isinstance(value, decimal.Decimal):
                                value = float(value)
                            # Assurer que la valeur est d'un type accepté par st.metric
                            if value is None:
                                value = "N/A"
                            with row[j]:
                                st.markdown(f'<div class="kpi-box"><h3>{label}</h3><p>{value}</p></div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        with pie_chart_col:
            fig = px.pie(types_df, values='COUNT', names='DATA_TYPE', 
                         title='Data types',
                         hole=0.4,  # Cercle vide au milieu
                         color_discrete_sequence=px.colors.sequential.Blues_r  # Couleurs claires
                         )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)

        conn.close()
    else:
        st.warning("Please enter the login information on the home page.")
