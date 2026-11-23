import streamlit as st

st.title("settings")
st.header("Settings")
st.write(f"You are logged in as {st.session_state.role}.")