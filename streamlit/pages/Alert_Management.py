# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 15:56:53 2023

@author: User
"""

import threading
import time
import traceback
import streamlit as st
import json
import snowflake.connector
import pandas as pd 

st.set_page_config(page_title="Manage Shares - TAGS", layout="wide", page_icon='snowflakelogo.png')

header_style = '''
    <style>
        thead, tbody, tfoot, tr, td, th{
            background-color:#fff;
            color:#000;
        } 
        # .glideDataEditor{
        #      background-color:#fff;
        # }
        section[data-testid="stSidebar"] div.stButton button {
          background-color: #049dbf;
          width: 300px;
          color: #fff;
        }
        .css-cgx0ld {
          background-color: #049dbf;
          color: #fff;
          font-size: 20px;
          float: right;
        }
        div.stButton button {
          background-color: #f7f7f7;
          color: #29b5e8;
          margin-top: 25px;
        }
        div[data-baseweb="select"] > div {
          background-color: #f7f7f7;
          color: #000;
        }
        
        .css-1b1e6xd {
          color: #000;
          font-weight: bold;
        }
        [data-testid='stSidebarNav'] > ul {
            min-height: 38vh;
        } 
        [data-testid="stSidebar"] {
                    background-image: url('https://pbsinfosystems.com/applogo.png');
                    background-repeat: no-repeat;
                    padding-top: 30px;
                    background-position: 20px 20px;
                }
    
    </style>
'''
st.markdown(header_style, unsafe_allow_html=True)

# Load credentials from app_config.json
def load_credentials():
    with open('app_config.json') as f:
        data = json.load(f)
        return data["snowflake"]

@st.cache_resource
def init_connection():
    # Use credentials from the JSON file
    creds = load_credentials()
    return snowflake.connector.connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"],
    )

con = init_connection()

cur = con.cursor()

st.markdown(" ### :card_file_box: **Alert Management**")

st.cache_data()

# Initialize session state variables
if 'dayvalue_multiselect' not in st.session_state:
    st.session_state.dayvalue_multiselect = []

def fetch_data(): 
    try:
        cur = con.cursor()
        sql = "select * from Metadata_AlertManagement"
        cur.execute(sql)
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])
        df['Edit'] = False  
        return df
    except Exception as e:
        st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>The 'Metadata_AlertManagement' table does not exist.</div>", unsafe_allow_html=True) 
        return 

# Main Streamlit app logic
df = fetch_data()

if df is not None:   
    df = df.rename(columns={
            'ALERT_MANAGEMENT_KEY' :'Row Id',
            'ALERT_NAME' : 'Alert Name',
            'SQL_QUERY':'Sql Query',
            'FREQUENCY' : 'Frequency',
            'EMAIL' : 'Email',
            'ACTIVATE_DAY' : 'Activate Day'

        })
else:
    st.write("")

with st.container():
    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        alert_name = st.text_input("ALERT_NAME", key='alertname')

    with col2:
        frequency_options = ['Daily', 'Weekly']
        def fq_change():
            st.session_state.fqrname = st.session_state.fqrname
        frequency  = st.selectbox(
            'Select Frequency Name',
            options=frequency_options,
            key='fqrname',
            on_change=fq_change
        )
        fq_listname = st.session_state.fqrname

    with col3:
        if frequency  != 'Daily':
            activeday_options = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

            # Initialize dayvalue_multiselect if not present in session state
            if 'dayvalue_multiselect' not in st.session_state:
                st.session_state.dayvalue_multiselect = []

            def days_change():
                st.session_state.dayvalue_multiselect = st.session_state.dayvalue_multiselect

            selected_days = st.multiselect("Select Active Days", activeday_options, key='dayvalue_multiselect', on_change=days_change)

with st.container():
    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        st.session_state.sqlqueryname = st.text_input("SQL_QUERY", key='sqlquery')
        sql_query = st.session_state.sqlqueryname
    with col2:
        st.session_state.emailname = st.text_input("EMAIL", key='email')
        email = st.session_state.emailname
    with col3:
        def add_record():
            # Retrieve values from session state
            alert_name = st.session_state.alertname
            frequency = st.session_state.fqrname
            sql_query = st.session_state.sqlqueryname
            email = st.session_state.emailname
            active_days = ",".join(st.session_state.dayvalue_multiselect) if st.session_state.dayvalue_multiselect else None
            
            # Validate required fields
            if not alert_name:
                st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please fill ALERT_NAME field</div>", unsafe_allow_html=True)
                time.sleep(1)
  
            elif not frequency:
                st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please fill FREQUENCY field</div>", unsafe_allow_html=True)
                time.sleep(1)

            elif not sql_query:
                st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please fill SQL_QUERY field</div>", unsafe_allow_html=True)
                time.sleep(1)

            elif not email:
                st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please fill EMAIL field</div>", unsafe_allow_html=True)
                time.sleep(1)

            elif frequency.lower() == "weekly" and not active_days:
                st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please fill ACTIVE_DAYS field for weekly frequency</div>", unsafe_allow_html=True)
                time.sleep(1)
            else:
                # Insert record into the database
                try:
                    cur.execute("INSERT INTO Metadata_AlertManagement (ALERT_NAME, FREQUENCY, SQL_QUERY, EMAIL, ACTIVATE_DAY) VALUES (%s, %s, %s, %s, %s)",(alert_name, frequency, sql_query, email, active_days))
                    con.commit()
                    st.success("Record added successfully.")

                except Exception as e:
                    st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>The 'Alert Management' table does not exist.</div>", unsafe_allow_html=True) 

        # Bind the add_record function to the button click event
        add_record_button = st.button("Add Record", on_click=add_record, use_container_width=True)
        if add_record_button and df is not None:
            st.experimental_rerun()
        elif add_record_button and df is None:
            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>The 'Alert Management' table does not exist.</div>", unsafe_allow_html=True) 

if df is not None:
    display_df = df.drop(columns=['FREQUENCY_DAY','SUBJECT', 'ISACTIVE', 'LASTRUNDATETIME', 'LASTRUNSTATUS', 'LASTRUNERROR','LASTSTARTDATETIME','COLUMNNAME','ISRUNNING']).copy(deep=True)
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")

    def generate_update_statements(display_df, rows_to_update):
        for index in rows_to_update.index:
            alert_id = display_df.loc[display_df.index == index, 'Row Id'].values[0]

            if alert_name and sql_query and email:
                if fq_listname == 'Daily':
                    try:
                        # Construct the SQL update query
                        update_query = f"UPDATE Metadata_AlertManagement SET ALERT_NAME = '{alert_name}', FREQUENCY = '{fq_listname}', SQL_QUERY = '{sql_query}', EMAIL = '{email}', ACTIVATE_DAY = Null WHERE ALERT_MANAGEMENT_KEY = '{alert_id}';"
                        # Execute the update query
                        cur.execute(update_query)
                        con.commit()
                        st.success("Record updated successfully.")
                    except Exception as e:
                        st.error(f"Error occurred while updating record: {e}")
                elif fq_listname == 'Weekly' and selected_days:
                    try:
                        # Construct the SQL update query
                        update_query = f"UPDATE Metadata_AlertManagement SET ALERT_NAME = '{alert_name}', FREQUENCY = '{fq_listname}', SQL_QUERY = '{sql_query}', EMAIL = '{email}', ACTIVATE_DAY = '{','.join(selected_days)}' WHERE ALERT_MANAGEMENT_KEY = '{alert_id}';"
                        
                        # Execute the update query
                        cur.execute(update_query)
                        con.commit()
                        st.success("Record updated successfully.")
                    except Exception as e:
                        st.error(f"Error occurred while updating record: {e}")
                else:
                    st.error('Error occurred: One or more values are empty or invalid')
            else:
                st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>One or more values are empty.</div>",  unsafe_allow_html=True)


    def split_frame(input_df, rows):
        df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
        return df

    with st.container():
        # ----------pagination-------------
        if len(display_df) == 0:
            st.write("")
        else:
            bottom_menu = st.columns((4, 1, 1))
            with bottom_menu[2]:
                data_length = len(display_df)
                max_batch_size = int(data_length * 0.1)
                max_batch_size = max(max_batch_size, 10)
                max_batch_size = ((max_batch_size + 4) // 5) * 5
                batch_size_options = [i for i in range(5, max_batch_size + 1, 5)]
                batch_size = st.selectbox("Records per page", options=batch_size_options)
            with bottom_menu[1]:
                total_pages = (len(display_df) + batch_size - 1) // batch_size
                current_page = st.number_input(
                    "Page", min_value=1, max_value=total_pages, step=1
                )

            with bottom_menu[0]:
                st.markdown(f"Page **{current_page}** of **{total_pages}** ")
            


            if total_pages > 0:
                edited_df = None 
                pages = split_frame(display_df, batch_size)
                pageframe = pages[current_page - 1]
                edited_df_delta = pd.DataFrame(st.data_editor(pageframe, use_container_width=True, key=f"data_editor_{current_page}",hide_index=True))
                edited_df = edited_df_delta
               

with st.container():
    try:
        update_clicked = False

        if edited_df is not None and isinstance(edited_df, pd.DataFrame):
            num_true = edited_df['Edit'].sum()
            if num_true > 1:
                st.markdown(f"<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select only one share to update.</div>", unsafe_allow_html=True)
            elif num_true == 1:
                if st.button("Update Selected Rows"):
                    update_clicked = True
                    rows_to_update = edited_df[edited_df['Edit']]
                    generate_update_statements(display_df, rows_to_update)
                    time.sleep(2)
                    st.experimental_rerun()
                thread = threading.Thread(target=generate_update_statements)
                thread.start()
            else:
                if update_clicked:
                    st.markdown(f"<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select at least one warehouse to update.</div>", unsafe_allow_html=True)

        else:
            st.write("No data available")

    except Exception as e:
        st.write('')
