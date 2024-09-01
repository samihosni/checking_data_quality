
import streamlit as st

def get_id_column():
    return st.session_state.get('id_column', '')
