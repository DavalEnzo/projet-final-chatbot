import streamlit as st
import time
import requests
import numpy as np
import pandas as pd
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st 
import matplotlib.pyplot as plt
from collections import Counter


# Exemple de données

# Création d'un DataFrame à partir des données


st.write("# Welcome to the Chat talk with me👋")
# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("What is up?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    api_url = "http://backend:5550/api/data"
    api_data = {"message": prompt}
    response = requests.post(api_url, json=api_data, )
        
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        if response.status_code == 200:
            assistant_response = response.json().get("message")  
        else:
            assistant_response = "Sorry, I couldn't fetch a response from the API."
        
      

        # Simulate stream of response with milliseconds delay
        for chunk in assistant_response.split():
            full_response += chunk + " "
            time.sleep(0.05)
            # Add a blinking cursor to simulate typing
            message_placeholder.markdown(full_response + "▌")
        message_placeholder.markdown(full_response)
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": full_response})

       