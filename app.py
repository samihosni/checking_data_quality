import streamlit as st
import os
from streamlit_option_menu import option_menu
from snowflake.connector import connect
from dotenv import load_dotenv


# Import other pages
from dashboard1 import show_dashboard1
from dashboard2 import show_dashboard2
from manage_outliers import show_manage_outliers

st.set_page_config(
    page_title="BI4YOU Checking Data Quality",
    page_icon="assets/BI4YOU-Logo.jpeg",  # Chemin vers votre icône
    layout="wide",  # 'centered' ou 'wide'
    initial_sidebar_state="expanded",  # 'expanded' ou 'collapsed'
)

load_dotenv()

def get_columns(table_name):
    """ Function to get the list of columns from Snowflake table """
    conn = connect(
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        account=os.getenv('ACCOUNT'),
        warehouse=st.session_state.get('warehouse'),
        database=st.session_state.get('database'),
        schema=st.session_state.get('schema')
    )
    query_columns = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name.upper()}'
    """
    cursor = conn.cursor()
    cursor.execute(query_columns)
    columns = [row[0] for row in cursor.fetchall()]
    conn.close()
    return columns

def main():
    # Initialize session state variables if not already set
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'id_selected' not in st.session_state:
        st.session_state.id_selected = False
    if 'columns' not in st.session_state:
        st.session_state.columns = []
    if 'id_column' not in st.session_state:
        st.session_state.id_column = ''
    if 'redirect' not in st.session_state:
        st.session_state.redirect = None

    st.title("Checking Data Quality")
    
    # Chemin vers l'image locale du logo
    logo_path = "assets/BI4YOU-Logo.jpeg"  # Assurez-vous que ce chemin est correct
    
    # Création d'un conteneur pour le logo et la barre de navigation
    with st.container():
        col1, col2 = st.columns([1, 5])
        
        # Afficher le logo dans la première colonne
        with col1:
            st.image(logo_path, width=80)  # Affiche le logo avec une largeur de 80 pixels
        
        # Afficher la barre de navigation dans la deuxième colonne
        with col2:
            selected = option_menu(
                menu_title=None,
                options=["Input Form", "Table Overview", "Outliers Overview", "Data Statistics"],
                icons=["house", "table", "bar-chart", "exclamation-triangle"],
                menu_icon="cast",
                default_index=0,
                orientation="horizontal",
                styles={
                    "container": {
                        "padding": "0!important",
                        "background-color": "#FFFFFF",
                        "display": "flex",
                        "align-items": "center",  # Aligner verticalement au centre
                        "justify-content": "center",  # Centrer horizontalement
                        "white-space": "nowrap"  # Éviter les retours à la ligne
                    },
                    "icon": {"color": "#2c2c2c", "font-size": "13px"},  # Icon color and size
                    "nav-link": {
                        "font-size": "16px", 
                        "text-align": "center", 
                        "padding": "10px 20px",  # Ajuster le padding pour éviter l'excès d'espace
                        "border": "2px solid #2c2c2c",  # Border color for non-active items
                        "border-radius": "5px",
                        "color": "#2c2c2c",  # Text color for non-active items
                        "background-color": "transparent",
                        "white-space": "nowrap"  # Éviter les retours à la ligne
                    },
                    "nav-link-selected": {
                        "background-color": "#fff",  # Background color for the active item
                        "border-color": "#ff0000",  # Border color for the active item
                        "color": "#ff0000",  # Text color for the active item
                        "font-weight": "bold"
                    }
                }
            )

    # Display the appropriate page based on navigation and state
    if st.session_state.redirect:
        if st.session_state.redirect == "dashboard1":
            show_dashboard1()
        elif st.session_state.redirect == "dashboard2":
            show_dashboard2()
        elif st.session_state.redirect == "manage_outliers":
            show_manage_outliers()
        st.session_state.redirect = None  # Reset redirect state

    elif selected == "Input Form":
        show_home()
    elif selected == "Table Overview":
        show_dashboard1()
    elif selected == "Outliers Overview":
        show_manage_outliers()
    elif selected == "Data Statistics":
        show_dashboard2()

def show_home():
    st.header("Enter the informations")
    
    # Inputs for connection
    st.session_state.warehouse = st.text_input("Warehouse", value=st.session_state.get('warehouse', ''))
    st.session_state.database = st.text_input("Database", value=st.session_state.get('database', ''))
    st.session_state.schema = st.text_input("Schema", value=st.session_state.get('schema', ''))
    st.session_state.table = st.text_input("Table", value=st.session_state.get('table', ''))

    if st.button("Submit"):
        st.session_state.data_loaded = True
        if st.session_state.table:
            columns = get_columns(st.session_state.table)
            if columns:
                st.session_state.columns = columns
                st.session_state.id_selected = False  # Reset the ID selection flag
                st.session_state.id_column = ''  # Reset the selected ID column
            else:
                st.warning("No columns found for the specified table.")
    
    if st.session_state.data_loaded and not st.session_state.id_selected:
        if st.session_state.columns:
            selected_column = st.selectbox("Select Column as ID", options=[''] + st.session_state.columns)

            if st.button("Confirm ID"):
                st.session_state.id_column = selected_column
                st.session_state.id_selected = True
                st.session_state.redirect = "dashboard1"  # Set the redirect to dashboard1
    

if __name__ == "__main__":
    main()
