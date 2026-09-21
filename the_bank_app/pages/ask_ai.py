import streamlit as st

st.header("Ask AI")

with st.container(border=True):
    st.markdown(
        """**For the full conversational AI experience, use Snowflake CoWork.**
        CoWork provides multi-turn conversations with the MASTER_AGENT, including
        LCR monitoring, loan portfolio, wealth management, and customer 360 queries."""
    )
    st.link_button(
        "Open Snowflake CoWork",
        "https://ai.snowflake.com/sfseeurope/demo_mdaeppen",
        type="primary",
        width="stretch",
        icon=":material/open_in_new:",
    )
