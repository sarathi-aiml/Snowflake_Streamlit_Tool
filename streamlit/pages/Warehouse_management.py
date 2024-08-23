# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 15:56:53 2023

@author: User
"""

import threading
import streamlit as st
import json
import  snowflake.connector
import datetime
import pandas as pd
from PIL import Image
import time

st.set_page_config(page_title="Warehouse management - TAGS", layout="wide", page_icon='snowflakelogo.png')

header_style = '''
    <style>
        thead, tbody, tfoot, tr, td, th{
            background-color:#fff;
            color:#000;
        } 
        
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


st.markdown(" ### :card_file_box: **Warehouse Management**")
st.markdown("""<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">""", unsafe_allow_html=True)

def fetch_data(): 
    try:
        cur = con.cursor()
        sql = "select * from warehouse_Management"

        cur.execute(sql)
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])
        df['Edit'] = False  # Checkbox column for selection
        return df
    except Exception as e:
        st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>The 'Meatadata Warehouse Management' table does not exist.</div>",  
                    unsafe_allow_html=True)
        return


df = fetch_data()

if df is not None:
    df = df.rename(columns={
            'UNIQUEID' :'Row Id',
            'WAREHOUSE_NAME': 'Warehouse Name',
            'FREQUENCY': 'Frequency',
            'DATE' : 'Date',
            'ACTIVATE_DAY': 'Activate Day',
            'ACTIVATE_TIME':'Activate Time',
            'STATUS': 'Status',
            'SIZE': 'Size',
            'ISACTIVE': 'Is_Active'
        })
else:
    st.write("")
    
def get_warehouse_names():
    try:
        cur = con.cursor()
        query = "SHOW WAREHOUSES"
        cur.execute(query)
        warehouses = [row[0] for row in cur.fetchall()]
        return warehouses

    except Exception as e:
        st.error(f"Error: {e}")


with st.container():
    col1, col2, col3 = st.columns([1,1,2])
    with col1:
        warehouse_names = get_warehouse_names() 
        def wh_change():
            st.session_state.whname = st.session_state.whname
        option = st.selectbox(
            'Select Warehouse Name',
            options=warehouse_names,
            key='whname',
            on_change=wh_change
        )
        wh_listname = st.session_state.whname
    with col2: 
        frequency_options = ['Daily', 'Weekly']  # Replace with your actual options
        def fq_change():
            st.session_state.fqrname = st.session_state.fqrname
        option = st.selectbox(
            'Select Frequency Name',
            options=frequency_options,
            key='fqrname',
            on_change=fq_change
        )
        fq_listname = st.session_state.fqrname
    with col3:
        if fq_listname != 'Daily':
            activeday_options = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
            def days_change():
                st.session_state.dayvalue = st.session_state.dayvalue_multiselect

            # Get or initialize session state variables
            if 'dayvalue_multiselect' not in st.session_state:
                st.session_state.dayvalue_multiselect = []

            # Multiselect widget for selecting active days
            selected_days = st.multiselect("Select Active Days", activeday_options,key='dayvalue_multiselect', on_change=days_change)
            day_value = st.session_state.dayvalue_multiselect
with st.container():
    col1, col2, col3, col4= st.columns([2,2,2,2])        
    with col1:
        status_options = ['Resume', 'Suspend']
        def st_change():
            st.session_state.stvalue = st.session_state.stvalue
        option = st.selectbox(
            'Select Status',
            options=status_options,
            key='stvalue',
            on_change=st_change
        )
        statusvalue = st.session_state.stvalue
    with col2:    
        size_options = ['X-Small','Small','Medium','Large','X-Large','2X-Large','3X-Large','4X-Large','5X-Large','6X-Large']
        def size_change():
            st.session_state.sizevalue = st.session_state.sizevalue
        option = st.selectbox(
            'Select Size',
            options=size_options,
            key='sizevalue',
            on_change=size_change
        )
        size_value = st.session_state.sizevalue
    with col3:  

        def generate_time_options():
            """Generate time options with a 15-minute gap in 12-hour format."""
            times = []
            for hour in range(0, 24):
                for minute in range(0, 60, 15):
                    time = datetime.time(hour, minute)
                    times.append(time.strftime("%I:%M %p"))
            return times
        
        # List of time options with 15-minute gap in 12-hour format
        time_options_12_hour = generate_time_options()

        # Custom dropdown widget for time selection with options displayed in 12-hour format
        selected_time_12_hour = st.selectbox('Activate Time', time_options_12_hour)

        # Convert selected time to 24-hour format
        activation_time = datetime.datetime.strptime(selected_time_12_hour, "%I:%M %p").strftime("%H:%M")


    with col4:
        def addrecord():
            if wh_listname and fq_listname and statusvalue and size_value and activation_time: 
                try:
                    if fq_listname == "Daily":
                        whsql_insert = f"INSERT INTO warehouse_Management (WAREHOUSE_NAME, FREQUENCY, ACTIVATE_TIME, STATUS, SIZE, ISACTIVE, DATE) VALUES ('{wh_listname}', '{fq_listname}', '{activation_time}','{statusvalue}','{size_value}', True, CURRENT_DATE())"
                        
                    elif len(day_value) != 0 and fq_listname== "Weekly":
                        whsql_insert = f"INSERT INTO warehouse_Management (WAREHOUSE_NAME, FREQUENCY, ACTIVATE_DAY, ACTIVATE_TIME, STATUS, SIZE, ISACTIVE, DATE) VALUES ('{wh_listname}', '{fq_listname}','{', '.join(day_value)}', '{activation_time}','{statusvalue}','{size_value}', True, CURRENT_DATE())"
                        
                    elif len(day_value) == 0 and fq_listname == "Weekly":
                        st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select an active day for selected frequency</div>",
                        unsafe_allow_html=True)
                    cur.execute(whsql_insert)
                    con.commit()   
                    st.success("Record added successfully.")    
                except Exception as e:
                    st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Error occurred while adding record </div>", unsafe_allow_html=True)
        addrecordbtn = st.button("Add Record",on_click=addrecord, use_container_width=True)
        if addrecordbtn and df is not None:
            time.sleep(5)
            st.experimental_rerun()
        elif addrecordbtn and df is None:
            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>The 'Metadata Warehouse Management' table does not exist.</div>", unsafe_allow_html=True) 
              


if df is not None:
    qryres_df = df


    if "df_value" not in st.session_state:
        st.session_state.df_value = qryres_df


    def generate_update_statements(qryres_df, rows_to_update):
        for index in rows_to_update.index:  # Use the index from rows_to_delete to find UNIQUEID
            unique_id = qryres_df.loc[qryres_df.index == index, 'Row Id'].values[0]
            isactive = qryres_df.loc[qryres_df.index == index, 'Is_Active'].values[0]

            if wh_listname and fq_listname and statusvalue and size_value and activation_time: 
                try:
                    if fq_listname == "Daily":
                        whsql_insert = f"UPDATE warehouse_Management SET WAREHOUSE_NAME = '{wh_listname}',FREQUENCY = '{fq_listname}',ACTIVATE_DAY='', ACTIVATE_TIME = '{activation_time}',STATUS = '{statusvalue}',SIZE = '{size_value}', ISACTIVE = '{isactive}', DATE = CURRENT_DATE() WHERE UNIQUEID = {unique_id};"
                        
                    elif len(day_value) != 0 and fq_listname== "Weekly":
                        whsql_insert = f"UPDATE warehouse_Management SET WAREHOUSE_NAME = '{wh_listname}',FREQUENCY = '{fq_listname}', ACTIVATE_DAY='{', '.join(day_value)}', ACTIVATE_TIME = '{activation_time}',STATUS = '{statusvalue}',SIZE = '{size_value}', ISACTIVE = '{isactive}', DATE = CURRENT_DATE() WHERE UNIQUEID = {unique_id};"
                        
                       
                    elif len(day_value) == 0 and fq_listname == "Weekly":
                        st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select an active day for selected frequency</div>",
                        unsafe_allow_html=True)

                    cur.execute(whsql_insert)
                    con.commit()   
                    st.success("Record updated successfully.")
                except Exception as e:
                    st.error(f"Error occurred while updating record") 


    def updatechkval(edited_df, uniqueid):

        for uniqueval in uniqueid:
            is_active = edited_df.loc[edited_df['Row Id'] == uniqueval, 'Is_Active'].values[0]
            
            activate_container = st.empty()
            deactivate_container = st.empty()
            
            if is_active:  # Check if the checkbox is selected
                sql_update = f"UPDATE warehouse_Management SET ISACTIVE = TRUE WHERE UNIQUEID = {uniqueval};"
                cur.execute(sql_update)
                cur.execute(f"SELECT WAREHOUSE_NAME, FREQUENCY, ACTIVATE_DAY, ACTIVATE_TIME FROM warehouse_Management WHERE UNIQUEID = {uniqueval};")
                jobres = cur.fetchone()
                if jobres:
                    wrh_name, frqs_value, act_day, act_time = jobres
                    activate_output = f"Warehouse {wrh_name} scheduled {frqs_value}-{act_day} at {act_time} is activated successfully."
                    activate_container.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: green'>"+ activate_output +"</div>", unsafe_allow_html=True)
                    time.sleep(2)
                    activate_container.empty()
                    st.experimental_rerun()
                else:
                    st.write("Error fetching warehouse details.")
            else:
                sql_update = f"UPDATE warehouse_Management SET ISACTIVE = FALSE WHERE UNIQUEID = {uniqueval};"
                cur.execute(sql_update)
                cur.execute(f"SELECT WAREHOUSE_NAME, FREQUENCY, ACTIVATE_DAY, ACTIVATE_TIME FROM warehouse_Management WHERE UNIQUEID = {uniqueval};")
                jobres = cur.fetchone()
                if jobres:
                    wrh_name, frqs_value, act_day, act_time = jobres
                    deactivate_output = f"Warehouse {wrh_name} scheduled {frqs_value}-{act_day} at {act_time} has been deactivated."
                    deactivate_container.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>"+ deactivate_output +"</div>", unsafe_allow_html=True)
                    time.sleep(2)
                    activate_container.empty()
                    st.experimental_rerun()
                else:
                    st.write("Error fetching warehouse details.")
                    
                    
    st.markdown('<span style="color:#6b6f76 ; font-weight:bold;" ><i class="fas fa-info-circle"></i> Please select one record at a time for update. Select appropriate options from the drop down menus to add/update a warehouse</span>',unsafe_allow_html=True)    
    #edited_df = st.data_editor(qryres_df,hide_index=True,height=int((len(qryres_df.reset_index(drop=True))+1) * 35.5), use_container_width=True)

    def split_frame(input_df, rows):
        df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
        return df

    with st.container():
        # ----------pagination-------------
        if len(qryres_df) == 0:
            st.write("")
        else:
            bottom_menu = st.columns((4, 1, 1))
            with bottom_menu[2]:
                data_length = len(qryres_df)
                max_batch_size = int(data_length * 0.1)
                max_batch_size = max(max_batch_size, 10)
                max_batch_size = ((max_batch_size + 4) // 5) * 5
                batch_size_options = [i for i in range(5, max_batch_size + 1, 5)]
                batch_size = st.selectbox("Records per page", options=batch_size_options)
            with bottom_menu[1]:
                total_pages = (len(qryres_df) + batch_size - 1) // batch_size
                current_page = st.number_input(
                    "Page", min_value=1, max_value=total_pages, step=1
                )

            with bottom_menu[0]:
                st.markdown(f"Page **{current_page}** of **{total_pages}** ")
            


            if total_pages > 0:
                edited_df = None 
                pages = split_frame(qryres_df, batch_size)
                pageframe = pages[current_page - 1]
                edited_df_delta = pd.DataFrame(st.data_editor(pageframe, use_container_width=True, key=f"data_editor_{current_page}",hide_index=True))
                edited_df = edited_df_delta

    # -----------pagination-------------

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
                        generate_update_statements(qryres_df, rows_to_update)
                        time.sleep(2)
                        st.experimental_rerun()
                    thread = threading.Thread(target=updatechkval)
                    thread.start()
                else:
                    if update_clicked:
                        st.markdown(f"<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select at least one warehouse to update.</div>", unsafe_allow_html=True)

            else:
                st.write("No data available")
            

            filtered_qryres_df = qryres_df[qryres_df.index.isin(edited_df.index)]
            # st.write(filtered_qryres_df)
            if edited_df is not None and not edited_df.equals(st.session_state["df_value"]):
                seluniqueid = edited_df.iloc[(filtered_qryres_df["Is_Active"] != edited_df["Is_Active"]).values]["Row Id"].tolist()
                # st.write(seluniqueid)
                updatechkval(edited_df, seluniqueid)
               
            st.session_state["df_value"] = edited_df 
        except Exception as e:
            st.write('')
