# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 15:56:53 2023

@author: User
"""

from sqlite3 import ProgrammingError
import threading
import time
import streamlit as st
import json
import snowflake.connector
import pandas as pd
from PIL import Image
import datetime

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



def init_connection():
    # Load credentials from app_config.json
    with open('app_config.json') as f:
        creds = json.load(f)["snowflake"]

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

st.markdown(" ### :card_file_box: **Shares**")
st.markdown("""<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">""", unsafe_allow_html=True)
def fetch_data():
    try:
        cur = con.cursor()

        sql_select = "SELECT SHARE_MANAGEMENT_KEY, SHARE_NAME, OBJECT_TYPE, OBJECT_NAME, IS_SHARE_ACTIVE, CREATE_DATE, DEACTIVATED_DATE FROM Metadata_ShareManagement"
        cur.execute(sql_select)
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])

        df['Edit'] = False  # Checkbox column for selection
        return df
    except Exception as e:
        st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>The 'Metadata ShareManagement' table does not exist.</div>",  
                    unsafe_allow_html=True) 
        return 


df = fetch_data()


if df is not None:
    df = df.rename(columns={
        'SHARE_MANAGEMENT_KEY': 'Row Id',
        'SHARE_NAME': 'Share Name',
        'OBJECT_TYPE': 'Object Type',
        'OBJECT_NAME': 'Object Name',
        'IS_SHARE_ACTIVE': 'Is_Share_Active',
        'CREATE_DATE': 'Create Date',
        'DEACTIVATED_DATE': 'Deactivated Date'
    })
else:
    # Handle the case where df is None, for example, by displaying an error message.
    st.write("")


def fetch_object_names(object_type,db, schema, share_list):
    try:
        object_names = []
        sql = f"desc share {share_list}"
        cur.execute(sql)
        selt_db_sh = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        slect_db = pd.DataFrame(selt_db_sh, columns=columns)
        db_results = slect_db.loc[slect_db['kind'] == 'DATABASE']['name']

        sharedbname = None 
        for sharedbname in db_results:
            pass

        if sharedbname is None: 
            if object_type == "DATABASE":
                shr_sql = "SHOW DATABASES" 
            elif object_type == "SCHEMA":
                shr_sql = "SHOW DATABASES" 
            elif object_type == "TABLE":
                shr_sql = "SHOW DATABASES" 
            elif object_type == "VIEW":
                shr_sql = f"SHOW DATABASES"   
            else:
                st.write("")
        
        else:

            schemasql = 'select SCHEMA_NAME from ' + sharedbname + '.information_Schema.schemata;'
            cur.execute(schemasql)
            schemaresults = cur.fetchall()
            dbschemalist = []

            for row in schemaresults:
                schemaqryres = str(row[0])
                dbschemalist.append(schemaqryres)

            if object_type == "DATABASE": 
                if sharedbname:
                    object_names = [sharedbname]
                    return object_names
                else:
                   st.error("Not found")
            elif object_type == "SCHEMA":
                if sharedbname:
                    object_names = [sharedbname]
                    return object_names
                else:
                   st.error("Not found")
            elif object_type == "TABLE":
                if sharedbname:
                    object_names = [sharedbname]
                    return object_names
                else:
                   st.error("Not found")
            elif object_type == "VIEW":
                if sharedbname:
                    object_names = [sharedbname]
                    return object_names
                else:
                   st.error("Not found")
            else:
                st.error("Not found")

        cur.execute(shr_sql)
        ob_results = cur.fetchall()
        object_names = [row[1] for row in ob_results] 
        return object_names
    except ProgrammingError as e:
        st.error(f"Error fetching object names: {e}")
        return []


def fetch_object_names_by_type(object_type, db, schema, share_list):
    if object_type == "DATABASE":
        return fetch_object_names(object_type, None, None, share_list)
    elif object_type == "SCHEMA":
        return fetch_object_names(object_type, db, None, share_list)
    elif object_type == "TABLE":
        return fetch_object_names(object_type, db, schema, share_list)
    elif object_type == "VIEW":
        return fetch_object_names(object_type, db, schema, share_list)
    
object_type_options = [
    "DATABASE",
    "SCHEMA",
    "TABLE",
    "VIEW"
]   

def get_shares_names():

    try:
        cur = con.cursor()
        query = "SHOW SHARES"
        cur.execute(query)
        
        # Fetching all rows and metadata about columns
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        slect_db = pd.DataFrame(rows, columns=columns)
        # Finding the index of the 'kind' column dynamically
        db_results = slect_db.loc[slect_db['kind'] == 'OUTBOUND']['name']
        return db_results

    except Exception as e:
        st.error(f"Error: {e}")


share_names = get_shares_names()

if share_names.empty:
    st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please Create Shares</div>",  unsafe_allow_html=True) 
    #st.write("")
else:
    with st.container():
        col1, col2,col3 = st.columns(3, gap="large")

        sql = "show databases"
        cur.execute(sql)
        results = cur.fetchall()
        dblist = []
        for row in results:
            qryres = str(row[1])
            dblist.append(qryres)

        default_ix = dblist.index('SNOWFLAKE')
        #-------------------'Select shares'------------------------------------#
        with col1:
            def share_change():
                st.session_state.share_list = st.session_state.share_list
            option = st.selectbox(
                'Select shares',
                options=share_names,
                key='share_list',
                on_change=share_change
            )
            share_list = st.session_state.share_list
            
        
        #--------------- "Select Object Type"---------------------------------#
        with col2:
            def objecttype_changed():
                st.session_state.objectype = st.session_state.objectype
            object_type = st.selectbox(
                "Select Object Type",
                options= object_type_options, 
                key='objectype', 
                on_change=objecttype_changed)
            db = None  # Initialize db to None
            if 'name' in st.session_state:
                db = st.session_state.name  # Get the selected database name
            schema = None  
            object_namess = fetch_object_names_by_type(object_type, db, schema, share_list)
            objectype = st.session_state.objectype

        #----------------------'Select Database'-----------------------------------#
            

        
        with col3:
            def databaseschema_changed():
                st.session_state.object_name1 = st.session_state.object_name1
                return
            def db_changed():
                st.session_state.name = st.session_state.name


            if 'name' in st.session_state:
                default_ix = dblist.index(st.session_state['name'])
            else:
                default_ix = 0

            if object_type == "SCHEMA":
                if 'object_name1' not in st.session_state:
                    st.session_state.object_name1 = object_namess[0] if object_namess else ''
                option = st.selectbox(
                    'Select Database',
                    options=object_namess,
                    key='object_name1',
                    on_change=databaseschema_changed
                )
                st.session_state.object_name = option

            elif object_type == "TABLE":
                option = st.selectbox(
                    'Select Database',
                    options=object_namess,
                    key='name',
                    on_change=db_changed,
                )
            elif object_type == "VIEW":
                option = st.selectbox(
                    'Select Database',
                    options=object_namess,
                    key='name',
                    on_change=db_changed,
                )
            else:
                st.write("")

            # Initialize st.session_state.name if not already initialized
            if 'name' not in st.session_state:
                st.session_state.name = dblist[default_ix] if dblist else ''
            
            if 'object_name1' not in st.session_state:
                st.session_state.object_name1 = dblist[default_ix] if dblist else ''

            # Ensure db is set correctly
            db = st.session_state.name or ''

            selected_schema = st.session_state.object_name1


            # Proceed with querying the database only if db is not None
            if db:
                schsql = 'select SCHEMA_NAME from ' + selected_schema + '.information_Schema.schemata;'
                cur.execute(schsql)
                schresults = cur.fetchall()
                schemalist = []
                schemalist1=[]

                for row in schresults:
                    schqryres = str(row[0])
                    schqryres1 = str(row[0])
                    schemalist.append(schqryres)
                    schemalist1.append(schqryres1)



    #----------------------------------'Select Schema'-----------------------------#
    with st.container():
        col1, col2,col3 = st.columns(3, gap="large")
        with col1:
            def schema_changed():
                st.session_state.schname1 = st.session_state.schname1
            

            if object_type == "TABLE":

                schemaoption = st.selectbox(
                    'Select Schema',
                    options=schemalist1,
                    key='schname1',
                    on_change=schema_changed
                    
                )
            elif object_type == "VIEW":
                
                schemaoption = st.selectbox(
                    'Select Schema',
                    options=schemalist1,
                    key='schname1',
                    on_change=schema_changed
                    
                )
            else:
                st.write("")

            #schema = st.session_state.schname1

        #-------------------------'Select object_name'--------------------------#
        with col2:
            def objectname_change():
                st.session_state.object_name = st.session_state.object_name
            def tablename_change():
                st.session_state.tablename = st.session_state.tablename
            def viewname_changed():
                st.session_state.viewname =  st.session_state.viewname

            
            if object_type == "DATABASE":
                option = st.selectbox(
                    'Select object name',
                    options=object_namess,
                    key='object_name',
                    on_change=objectname_change,
                )
            
                object_namelist = st.session_state.object_name
            elif object_type == "SCHEMA":
                
                schemaoption = st.selectbox(
                    'Select object name',
                    options=schemalist,
                    key='schname1',
                    on_change=schema_changed,
                )
                schema_namelist=st.session_state.schname1

            elif object_type == "TABLE":
                sqltablequery = f"SHOW TABLES IN SCHEMA {selected_schema}.{schemaoption}"
                cur.execute(sqltablequery)
                tablelist = []
                tableresult = cur.fetchall()
                
                for row in tableresult:
                    tableres = str(row[1])
                    tablelist.append(tableres)
                
                schemaoption = st.selectbox(
                    'Select object name',
                    options=tablelist,
                    key='tablename',
                    on_change=tablename_change,
                )
                table_namelist = st.session_state.tablename
                schema_namelist=st.session_state.schname1

            elif object_type == "VIEW":
                sqlviewquery = f"SHOW VIEWS IN SCHEMA {selected_schema}.{schemaoption}"
                cur.execute(sqlviewquery)
                viewlist = []
                viewresult = cur.fetchall()
                
                for row in viewresult:
                    viewres = str(row[1])
                    viewlist.append(viewres)
                
                schemaoption = st.selectbox(
                    'Select object name',
                    options=viewlist,
                    key='viewname',
                    on_change=viewname_changed,
                )
                view_namelist = st.session_state.viewname
                schema_namelist=st.session_state.schname1


            else:
                st.write("")

        
        #---------------------ADD record------------------------------------------#
        with col3:
            def insert():

                if objectype:
                    if objectype == "DATABASE" and share_list and object_namelist:
                        try:
                            sql_insert = f"INSERT INTO Metadata_ShareManagement (SHARE_NAME, OBJECT_TYPE, OBJECT_NAME, IS_SHARE_ACTIVE, CREATE_DATE) VALUES ('{share_list}', '{objectype}', '{object_namelist}', True, CURRENT_TIMESTAMP())"
                            cur.execute(sql_insert)
                            con.commit()
                            st.success("Record added successfully.")
                        except Exception as e:
                            st.error(f"Error occurred while adding record: {e}")
                    elif objectype:
                        try:
                            if objectype == "SCHEMA" and share_list and schema_namelist:
                                sql_insert =  f"INSERT INTO Metadata_ShareManagement (SHARE_NAME, OBJECT_TYPE, OBJECT_NAME, IS_SHARE_ACTIVE, CREATE_DATE) VALUES ('{share_list}', '{objectype}', '{db}.{schema_namelist}', True, CURRENT_TIMESTAMP())"
                                cur.execute(sql_insert)
                                con.commit()
                                st.success("Record added successfully.")

                            elif objectype == "TABLE" and share_list and table_namelist and schema_namelist:
                                try:
                                    sql_insert =  f"INSERT INTO Metadata_ShareManagement (SHARE_NAME, OBJECT_TYPE, OBJECT_NAME, IS_SHARE_ACTIVE, CREATE_DATE) VALUES ('{share_list}', '{objectype}', '{db}.{schema_namelist}.{table_namelist}', True, CURRENT_TIMESTAMP())"
            
                                    cur.execute(sql_insert)
                                    con.commit()
                                    st.success("Record added successfully.")
                                except Exception as e:
                                    st.error(f"Error occurred while adding record: {e}")

                            elif objectype =="VIEW" and share_list and view_namelist and schema_namelist:
                                try:
                                    sql_insert =  f"INSERT INTO Metadata_ShareManagement (SHARE_NAME, OBJECT_TYPE, OBJECT_NAME, IS_SHARE_ACTIVE, CREATE_DATE) VALUES ('{share_list}', '{objectype}', '{db}.{schema_namelist}.{view_namelist}', True, CURRENT_TIMESTAMP())"
                                    cur.execute(sql_insert)
                                    con.commit()
                                    st.success("Record added successfully.")
                                
                                except Exception as e:
                                    st.error(f"Error occurred while adding record: {e}")
                            
                            else:
                                st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please Fill all the fileds if not then choose available options</div>",  
                        unsafe_allow_html=True) 

                        except Exception as e:
                            st.error(f"Error occurred while adding record: {e}")
    
                    else:
                        st.write("NOT FOUND")

            st.button("Add Record", on_click=insert, use_container_width=True)

    with st.container():
        if df is not None:
            qryres6 = df
            def generate_update_statements(qryres6, rows_to_update):
                for index in rows_to_update.index:  # Use the index from rows_to_delete to find UNIQUEID
                # Retrieve the UNIQUEID for each row marked for deletion
                    unique_id = qryres6.loc[qryres6.index == index, 'Row Id'].values[0]
                    isactive = qryres6.loc[qryres6.index == index, 'Is_Share_Active'].values[0]

                    if objectype == "DATABASE" and share_list and object_namelist:
                        try:
                            sql_update = f"UPDATE Metadata_ShareManagement SET IS_SHARE_ACTIVE = '{isactive}', CREATE_DATE = CURRENT_DATE(), SHARE_NAME = '{share_list}', OBJECT_TYPE = '{objectype}', OBJECT_NAME = '{object_namelist}' WHERE SHARE_MANAGEMENT_KEY = {unique_id};"
                            cur.execute(sql_update)
                            con.commit()  
                            st.success("Record updated successfully.")
                        except Exception as e:
                            st.error(f"Error occurred while adding record: {e}")
                    elif objectype == "SCHEMA" and share_list and schema_namelist:
                        try:
                            sql_update = f"UPDATE Metadata_ShareManagement SET IS_SHARE_ACTIVE = '{isactive}', CREATE_DATE = CURRENT_DATE(), SHARE_NAME = '{share_list}', OBJECT_TYPE = '{objectype}', OBJECT_NAME = '{db}.{schema_namelist}' WHERE SHARE_MANAGEMENT_KEY = {unique_id};"
                            cur.execute(sql_update)
                            con.commit()  
                            st.success("Record updated successfully.")
                        except Exception as e:
                            st.error(f"Error occurred while adding record: {e}")
                    elif objectype == "TABLE" and share_list and table_namelist and schema_namelist:
                        try:
                            sql_update = f"UPDATE Metadata_ShareManagement SET IS_SHARE_ACTIVE = '{isactive}', CREATE_DATE = CURRENT_DATE(), SHARE_NAME = '{share_list}', OBJECT_TYPE = '{objectype}', OBJECT_NAME = '{db}.{schema_namelist}.{table_namelist}' WHERE SHARE_MANAGEMENT_KEY = {unique_id};"

                            cur.execute(sql_update)
                            con.commit()  
                            st.success("Record updated successfully.")
                        except Exception as e:
                            st.error(f"Error occurred while adding record: {e}")
                    elif objectype =="VIEW" and share_list and view_namelist and schema_namelist:
                        try:
                            sql_update = f"UPDATE Metadata_ShareManagement SET IS_SHARE_ACTIVE = '{isactive}', CREATE_DATE = CURRENT_DATE(), SHARE_NAME = '{share_list}', OBJECT_TYPE = '{objectype}', OBJECT_NAME = '{db}.{schema_namelist}.{view_namelist}' WHERE SHARE_MANAGEMENT_KEY = {unique_id};"

                            cur.execute(sql_update)
                            con.commit()  
                            st.success("Record updated successfully.")
                        except Exception as e:
                            st.error(f"Error occurred while adding record: {e}")
                    else:
                        st.write("Select any object type")

            if "df_value" not in st.session_state:
                st.session_state.df_value = qryres6

            outputres = ""

                        
            def updatechkval(edited_df, uniqueid):
                for uniqueval in uniqueid:
                    is_active = edited_df.loc[edited_df['Row Id'] == uniqueval, 'Is_Share_Active'].values[0]
                    
                    activate_container = st.empty()
                    deactivate_container = st.empty()
                    
                    if is_active:  # Check if the checkbox is selected
                        # Activate the share
                        sql_update = f"UPDATE METADATA_SHAREMANAGEMENT SET IS_SHARE_ACTIVE = TRUE, DEACTIVATED_DATE = NULL WHERE SHARE_MANAGEMENT_KEY = {uniqueval};"
                        cur.execute(sql_update)
                        cur.execute(f"SELECT SHARE_NAME, OBJECT_TYPE, OBJECT_NAME from METADATA_SHAREMANAGEMENT WHERE SHARE_MANAGEMENT_KEY = {uniqueval};")
                        jobres = cur.fetchone()
                        if jobres:
                            shlist, obtype, obname = jobres
                            activate_output = f"Shares {shlist} scheduled {obtype} and {obname} is activated successfully."
                            activate_container.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: green'>"+ activate_output +"</div>", unsafe_allow_html=True)
                            time.sleep(2)
                            activate_container.empty()
                            st.experimental_rerun()
                        else:
                            st.write("Error fetching warehouse details.")
                    else:
                        # Deactivate the share
                        sql_update = f"UPDATE METADATA_SHAREMANAGEMENT SET IS_SHARE_ACTIVE = FALSE, DEACTIVATED_DATE = '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' WHERE SHARE_MANAGEMENT_KEY = {uniqueval};"
                        cur.execute(sql_update)
                        cur.execute(f"SELECT SHARE_NAME, OBJECT_TYPE, OBJECT_NAME from METADATA_SHAREMANAGEMENT WHERE SHARE_MANAGEMENT_KEY = {uniqueval};")
                        jobres = cur.fetchone()
                        if jobres:
                            shlist, obtype, obname = jobres
                            deactivate_output = f"Shares {shlist} scheduled {obtype} and {obname} has been deactivated."
                            deactivate_container.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>"+ deactivate_output +"</div>", unsafe_allow_html=True)
                            time.sleep(2)
                            activate_container.empty()
                            st.experimental_rerun()
                        else:
                            st.write("Error fetching warehouse details.")
    st.markdown('<span style="color:#6b6f76 ; font-weight:bold;" ><i class="fas fa-info-circle"></i> Please select one share at a time for update. Select appropriate options from the drop down menus to add/update a share</span>',unsafe_allow_html=True)
    #edited_df = st.data_editor(qryres6,height=int((len(qryres6.reset_index(drop=True))+1) * 35.5), hide_index=True, use_container_width=True)

    def split_frame(input_df, rows):
        df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
        return df   
            
    with st.container():
        # ----------pagination-------------
        if len(qryres6) == 0:
            st.write("")
        else:
            bottom_menu = st.columns((4, 1, 1))
            with bottom_menu[2]:
                data_length = len(qryres6)
                max_batch_size = int(data_length * 0.1)
                max_batch_size = max(max_batch_size, 10)
                max_batch_size = ((max_batch_size + 4) // 5) * 5
                batch_size_options = [i for i in range(5, max_batch_size + 1, 5)]
                batch_size = st.selectbox("Records per page", options=batch_size_options)
            with bottom_menu[1]:
                total_pages = (len(qryres6) + batch_size - 1) // batch_size
                current_page = st.number_input(
                    "Page", min_value=1, max_value=total_pages, step=1
                )

            with bottom_menu[0]:
                st.markdown(f"Page **{current_page}** of **{total_pages}** ")
            


            if total_pages > 0:
                edited_df = None 
                pages = split_frame(qryres6, batch_size)
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
                        generate_update_statements(qryres6, rows_to_update)
                        time.sleep(2)
                        st.experimental_rerun()
                    thread = threading.Thread(target=updatechkval)
                    thread.start()
                else:
                    if update_clicked:
                        st.markdown(f"<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select at least one warehouse to update.</div>", unsafe_allow_html=True)

            else:
                st.write("No data available")


            filtered_qryres_df = qryres6[qryres6.index.isin(edited_df.index)]
           
            if edited_df is not None and not edited_df.equals(st.session_state["df_value"]):
                seluniqueid = edited_df.iloc[(filtered_qryres_df["Is_Share_Active"] != edited_df["Is_Share_Active"]).values]["Row Id"].tolist()
               
                updatechkval(edited_df, seluniqueid)
               
            st.session_state["df_value"] = edited_df 

            
        except Exception as e:
            st.write('')
    # -----------pagination-------------

        



   


    








