import streamlit as st

ROLES = [None, "Admin", "Guest", "CO", "COR", "User"]

# add header
st.header("Log-in")

# selectbox
role = st.selectbox("Select your role", ROLES)

# log-in button
if st.button("Log-in"):
    st.session_state.role = role
    st.rerun()
