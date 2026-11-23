
import streamlit as st

# Define and add roles
if "role" not in st.session_state:
  st.session_state.role = None

ROLES = [None, "Admin", "Guest", "CO", "COR", "User"]

# page configuration & title
st.set_page_config(page_title="Modiscope", page_icon="🤖", layout="wide")
st.title("Modiscope - CLIN Optics 💡")

# Pages appear in the sidebar in this order; default=True is the page shown when the app opens
pg = st.navigation(
    [
        st.Page("1_🏢homepage.py", title="Home", icon="🏢", default=True),
        st.Page("pages/2_📊dashboard.py", title="Dashboard", icon="📊"),
        st.Page("pages/3_🖥️synthetic_simulation.py", title="Synthetic Simulation", icon="🖥️"),
        st.Page("pages/4_🤖chatbot.py", title="Chatbot", icon="🤖"),
        st.Page("pages/settings.py", title="Settings", icon=":material/settings:"),
    ]
)

with st.sidebar:
    st.markdown(
        """
        <p style="font-size: 12px; font-style: italic; color: #888;">
            This is a demo app. It is not intended for production use.
        </p>
        """,
        unsafe_allow_html=True,
    )

# Run the selected page (PIID / MCP form now lives on the Synthetic Simulation page)
pg.run()
