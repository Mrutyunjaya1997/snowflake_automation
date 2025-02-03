"""
Cortex Analyst App
====================
This app allows users to interact with their data using natural language.
"""

import json  # To handle JSON data
import time
from typing import Dict, List, Optional, Tuple

import _snowflake  # For interacting with Snowflake-specific APIs
import pandas as pd
import streamlit as st  # Streamlit library for building the web app
from snowflake.cortex import Complete
from snowflake.core import Root
#from snowflake.ml.python.cortex import Classify_text
from snowflake.snowpark.context import (
    get_active_session,
)  # To interact with Snowflake sessions
from snowflake.snowpark.exceptions import SnowparkSQLException



### Default Values
NUM_CHUNKS = 3 # Num-chunks provided as context. Play with this to check how it affects your accuracy
slide_window = 5 # how many last conversations to remember. This is the slide window.

# service parameters
CORTEX_SEARCH_DATABASE = "CC_QUICKSTART_CORTEX_SEARCH_DOCS"
CORTEX_SEARCH_SCHEMA = "DATA"
CORTEX_SEARCH_SERVICE = "CC_SEARCH_SERVICE_CS"

# columns to query in the service
COLUMNS = [
    "chunk",
    "relative_path"
]

# List of available semantic model paths in the format: <DATABASE>.<SCHEMA>.<STAGE>/<FILE-NAME>
# Each path points to a YAML file defining a semantic model
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/Factory_semantic_model.yaml"
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
API_TIMEOUT = 50000  # in milliseconds

# Initialize a Snowpark session for executing queries
session = get_active_session()
root = Root(session)   
svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

def main():
    # Initialize session state
    if "messages" not in st.session_state:
        reset_session_state()
    show_header_and_sidebar()
    #if len(st.session_state.messages) == 0:
    #    process_user_input("What questions can I ask?")
    display_conversation()
    handle_user_inputs()
    handle_error_notifications()


def reset_session_state():
    """Reset important session state elements."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.messages_internal = []  # List to store conversation messages and llm output
    st.session_state.active_suggestion = None  # Currently selected suggestion


def show_header_and_sidebar():
    """Display the header and sidebar of the app."""
    # Set the title and introductory text of the app
    st.title("Cortex Analyst")
    st.markdown(
        "Welcome to Cortex Analyst! Type your questions below to interact with your data. "
    )

    # Sidebar with a reset button
    with st.sidebar:
        st.selectbox(
            "Selected semantic model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        # Center this button
        _, btn_container, _ = st.columns([2, 6, 2])
        if btn_container.button("Clear Chat History", use_container_width=True):
            reset_session_state()


def handle_user_inputs():
    """Handle user inputs from the chat interface."""
    # Handle chat input
    user_input = st.chat_input("What is your question?")
    if user_input:
        process_user_input(user_input)
    # Handle suggested question click
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)


def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occured!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False

def summarize_question_with_history(chat_history, question):
# To get the right context, use the LLM to first summarize the previous conversation
# This will be used to get embeddings and find similar chunks in the docs for context

    prompt = f"""
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natual language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
    
    sumary = Complete("snowflake-arctic", prompt)   

    sumary = sumary.replace("'", "")

    return sumary
def get_similar_chunks_search_service(query):

    #filter_obj = {"@eq": {"category": st.session_state.category_value} }
    response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)

    st.sidebar.json(response.json())
    
    return response.json()  
    
def create_prompt (myquestion):

    chat_history = st.session_state.messages
    prompt = f"""
    You are a data visualization expert. Your task is to analyze the question and data to recommend the most appropriate chart type based on these rules:
    1. LINE CHART: Time patterns, trends, "over time"
    2. SCATTER PLOT: Correlations, relationships between variables
    3. HISTOGRAM/BOX: Distributions, spreads, variations
    4. PIE/DONUT: Percentages, proportions, breakdowns
    5. BAR CHART: Only for direct category comparisons
    
    The raw data is provided between <data> and </data>. 
    Chat history is between <chat_history> and </chat_history>.
    
    Additional Rules:
    - If timestamp column is involved and question implies time analysis, prefer Line Chart
    - For correlation questions between numerical columns, always use Scatter Plot
    - For distribution analysis of numerical columns, use Histogram/Box Plot
    - Only use Bar Chart when no other chart type is more appropriate
    
    Return JSON output:
    {{
        "chartType": "selected chart type based on rules",
        "xAxis": {{
            "column": "appropriate column name",
            "label": "descriptive x-axis label"
        }},
        "yAxis": {{
            "column": "appropriate column name",
            "label": "descriptive y-axis label"
        }},
        "insights": "3-4 key observations from data"
    }}
    
    <chat_history>{chat_history}</chat_history>
    <data>{st.session_state.df.to_string()}</data>
    """
    return prompt

def answer_question(myquestion):

    prompt =create_prompt (myquestion)

    response = Complete('snowflake-arctic', prompt)   

    return response

def get_converted_question(myquestion):

    prompt =f"""User has asked this question. 
    Can you convert this to a simplified data related question to be 
    passed to a text to sql agent in order to get raw data based on given schema
    Schema Overview:
    Timestamp: For tracking when events or readings occurred.
    Machine Details:
        Machine Type (e.g., Drill, Lathe, Welder)
    Sensor Readings:
        Temperature (°C)
        Vibration (mm/s²)
        Power Usage (kW)
        Pressure (psi)
        Humidity (%)
    Production Metrics:
        Product ID
        Product Type (e.g., Widget A, Component B)
        Units Produced
        Defects Detected
    Machine Status:
        Status (e.g., Operational, Maintenance, Offline)
        Downtime (minutes)
User Question: {myquestion}
Provide me single sentence without any explanation
Converted Question:"""

    response = Complete('claude-3-5-sonnet', prompt)  
    return response

def process_user_input(prompt: str):
    """
    Process user input and update the conversation history.

    Args:
        prompt (str): The user's input.
    """
    converted_question=get_converted_question(prompt)
    # Create a new message, append to history and display imidiately
    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt+"\n Inferred question: "+ converted_question }],
    }
    st.session_state.messages.append(new_user_message)
    with st.chat_message("user"):
        user_msg_index = len(st.session_state.messages) - 1
        display_message(new_user_message["content"], user_msg_index)

    # Show progress indicator inside analyst chat message while waiting for response
    with st.chat_message("analyst"):
        with st.spinner("Waiting for Analyst's response..."):
            time.sleep(1)
            #classification_query =  f"""SELECT '' AS CLASSIFIED"""
           # classification_query =  f"""SELECT PARSE_JSON(SNOWFLAKE.CORTEX.CLASSIFY_TEXT('{prompt}'"""+ r""", [{'label': 'Request for data','description':'Specific requests for underlying data'}
#, {'label': 'Request for suggestion','description':'Requests asking to interpret based on data or unrelated requests'}
 #           ,{'label': 'Request for action','description':'Asking to perform definite action other than fetching data or answering question'}])):"label"::STRING AS Classified;"""
            #classify_df = session.sql(classification_query).to_pandas()
            #converted_question=get_converted_question(prompt)
            response, error_msg = get_analyst_response(st.session_state.messages)
            if error_msg is None:
                analyst_message = {
                    "role": "analyst",
                    #"content": [{"type":"text","text":classify_df["CLASSIFIED"].iloc[0]}],
                    "content": response["message"]["content"],
                    "request_id": 1#response["request_id"],
                }
            else:
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": response["request_id"],
                }
                st.session_state["fire_API_error_notify"] = True
            st.session_state.messages.append(analyst_message)
            st.rerun()


def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    """
    Send chat history to the Cortex Analyst API and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }

    # Send a POST request to the Cortex Analyst API endpoint
    # Adjusted to use positional arguments as per the API's requirement
    resp = _snowflake.send_snow_api_request(
        "POST",  # method
        API_ENDPOINT,  # path
        {},  # headers
        {},  # params
        request_body,  # body
        None,  # request_guid
        API_TIMEOUT,  # timeout in milliseconds
    )

    # Content is a string with serialized JSON object
    parsed_content = json.loads(resp["content"])

    # Check if the response is successful
    if resp["status"] < 400:
        # Return the content of the response as a JSON object
        return parsed_content, None
    else:
        # Craft readable error message
        error_msg = f"""
🚨 An Analyst API error has occurred 🚨

* response code: {resp['status']}
* request-id: {parsed_content['request_id']}
* error code: {parsed_content['error_code']}

Message:
{parsed_content['message']}

        """
        return parsed_content, error_msg


def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    Ensures that the latest message (bottom-most) is automatically visible.
    """
    # Create a container for the chat messages
    chat_container = st.container()

    # Iterate through all messages but ensure the latest message is visible
    with chat_container:
        for idx, message in enumerate(st.session_state.messages):
            role = message["role"]
            content = message["content"]
            with st.chat_message(role):
                display_message(content, idx)

    # Add JavaScript to scroll to the bottom of the chat container
    st.markdown(
        """
        <script>
        var chatContainer = window.parent.document.querySelector('section.main');
        if (chatContainer) {
            chatContainer.scrollTo({ top: chatContainer.scrollHeight, behavior: 'smooth' });
        }
        </script>
        """,
        unsafe_allow_html=True,
    )




def display_message(content: List[Dict[str, str]], message_index: int):
    """
    Display a single message content.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message.
    """
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            # Display suggestions as buttons
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion, key=f"suggestion_{message_index}_{suggestion_index}"
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            # Display the SQL query and results
            display_sql_query(item["statement"], message_index)
        else:
            # Handle other content types if necessary
            pass


@st.cache_data(show_spinner=False)
def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute the SQL query and convert the results to a pandas DataFrame.

    Args:
        query (str): The SQL query.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: The query results and the error message.
    """
    global session
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


def display_sql_query(sql: str, message_index: int):
    """
    Executes the SQL query and displays the results in form of data frame and charts.

    Args:
        sql (str): The SQL query.
        message_index (int): The index of the message.
    """


    # Display the results of the SQL query
    with st.expander("Results", expanded=True):
        with st.spinner("Running SQL..."):
            df, err_msg = get_query_exec_result(sql)
            if df is None:
                st.error(f"Could not execute generated SQL query. Error: {err_msg}")
                return

            if df.empty:
                st.write("Query returned no data")
                return
            st.session_state.df=df
            # Show query results in two tabs
            data_tab, chart_tab = st.tabs(["Data 📄", "Chart 📈 "])
            with data_tab:
                st.dataframe(df, use_container_width=True)

            with chart_tab:
                display_charts_tab(df, message_index)
    # Display the SQL query
    with st.expander("Chart JSON", expanded=False):
        #st.code(sql, language="sql")
        st.write(answer_question("prompt"))

def display_charts_tab(df: pd.DataFrame, message_index: int) -> None:
    """
    Display the charts tab.

    Args:
        df (pd.DataFrame): The query results.
        message_index (int): The index of the message.
    """
    # There should be at least 2 columns to draw charts
    if len(df.columns) >= 2:
        all_cols_set = set(df.columns)
        col1, col2 = st.columns(2)
        x_col = col1.selectbox(
            "X axis", all_cols_set, key=f"x_col_select_{message_index}"
        )
        y_col = col2.selectbox(
            "Y axis",
            all_cols_set.difference({x_col}),
            key=f"y_col_select_{message_index}",
        )
        chart_type = st.selectbox(
            "Select chart type",
            options=["Line Chart 📈", "Bar Chart 📊"],
            key=f"chart_type_{message_index}",
        )
        if chart_type == "Line Chart 📈":
            st.line_chart(df.set_index(x_col)[y_col])
        elif chart_type == "Bar Chart 📊":
            st.bar_chart(df.set_index(x_col)[y_col])
    else:
        st.write("At least 2 columns are required")


if __name__ == "__main__":
    main()
