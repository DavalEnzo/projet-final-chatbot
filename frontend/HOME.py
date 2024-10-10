import streamlit as st
import random
import time


st.set_page_config(
    page_title="HOME",
    page_icon="👋",
)
st.write("# IA pour la modération des commentaires! 👋")

st.sidebar.success("Select a demo above.")

st.markdown(
    """
    Anlayse de donnée et mise en place d'un model d'IA pour la modération
    des prompt  .
    **👈 d'après un grand nombre de tweet nous allons tenter
     d'implémenter une ia capable de modérer les comentaires!
    ### Notre Stack?
        - Streamlit pour le Front
        - Flask pour le Backend 
    
    ### Les modèles utilisés : 
        - Régression logistiques 
        - Gradient Boost classfier
        - classificateur darbre de décisions
"""
)

