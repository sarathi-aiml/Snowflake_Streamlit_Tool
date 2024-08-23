# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 13:23:06 2023

@author: User
"""

import threading
import time
import streamlit as st
import json
import pandas as pd
import  snowflake.connector
import re
from PIL import Image


st.set_page_config(page_title="Create Rule - TAGS", layout="wide", page_icon='snowflakelogo.png')



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

st.markdown(" ### :card_file_box: **Rule Generation**")

def fetch_data(): 
    try:
        cur = con.cursor()
        sql = "select * from Metadata_TagManagement"
        cur.execute(sql)
        results = cur.fetchall()
        if not results:
            return None  # Return None if no data is fetched
        df = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])
        return df
    except Exception as e:
        st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>The 'Metadata_Management' table does not exist.</div>", unsafe_allow_html=True) 
        return 

# Main Streamlit app logic
df = fetch_data()

if df is not None:
    display_df = df.drop(columns=['CREATEDDATE', 'LASTRUNDATETIME', 'LASTRUNSTATUS', 'LASTRUNERROR']).copy(deep=True)

    display_df = display_df.rename(columns={
        'UNIQUEID' :'Row Id',
        'DBNAME': 'Database',
        'SCHEMANAME': 'Schema',
        'OBJECTTYPE': 'Object Type',
        'NAMEPATTERN': 'Name Pattern',
        'PATTERNVALUE': 'Pattern Value',
        'TAGNAME': 'Tag Name',
        'TAGVALUE': 'Tag Value',
        'ISACTIVE' :'Is active'
})
    
else:
    display_df = pd.DataFrame()

with st.sidebar:
    sql = "show databases"
    cur.execute(sql)
    results = cur.fetchall()
    dblist = []
    for row in results:
        qryres = str(row[1])
        dblist.append(qryres)

    default_ix = dblist.index('SNOWFLAKE')


    def db_changed():
        st.session_state.name = st.session_state.name
        st.session_state.tbllist =[]


    if 'name' in st.session_state:
        default_ix = dblist.index(st.session_state['name'])

    option = st.selectbox(
        'Select Database',
        options=dblist,
        index=default_ix,
        key='name',
        on_change=db_changed
    )

    dbname = st.session_state.name

    schsql = 'select SCHEMA_NAME from ' + dbname + '.information_Schema.schemata;'
    cur.execute(schsql)
    schresults = cur.fetchall()
    schemalist = []

    for row in schresults:
        schqryres = str(row[0])
        schemalist.append(schqryres)


    def schema_changed():
        st.session_state.schname = st.session_state.schname


    schemaoption = st.selectbox(
        'Select Schema',
        options=schemalist,
        key='schname',
        on_change=schema_changed
    )

    schemaname = st.session_state.schname

    def objtype_changed():
        st.session_state.objtype = st.session_state.objtype
        
    table2 = st.selectbox(
        'Select Type',
        options=('Table', 'View', 'Column'),
        key='objtype',
        on_change = objtype_changed)
    
    objecttype = st.session_state.objtype 


output = "" 

with st.container():
    
    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        def table_pattern():
            st.session_state.tblpatt = st.session_state.tblpatt
        if objecttype != 'Column':
            options=('Starts with', 'Ends with', 'Contains', 'Has role', 'Created by user')

            tablepatterns = st.selectbox(
               'Name pattern',
               options,
               key='tblpatt',
               on_change=table_pattern)
            
        else:
            options=('Starts with', 'Ends with', 'Contains')
            table3 = st.selectbox(
               'Name pattern',
               options,
               key='tblpatt',
               on_change=table_pattern)
            
        tablepattern = st.session_state.tblpatt
    with col2:
        st.text_input('Type pattern Eg : cust_tbl', key='patname')
        patternvalue = st.session_state.patname


    with col3:
        st.write("")

with st.container():
    col1, col2, col3 = st.columns([2, 2, 2])

    with col1:
       sql1=f"SHOW TAGS IN SCHEMA {dbname}.{schemaname}"  
       cur.execute(sql1)    
       results1 = cur.fetchall()    
       taglist = []
       
       for row in results1:
           qryres1 = str(row[1])            
           taglist.append(qryres1)
           
                   
       def tag_changed():
           st.session_state.taglist_name = st.session_state.taglist_name
           

       if taglist:
           tagoption = st.selectbox(
               'Select Tag',
               options=taglist,
               key='taglist_name',
               on_change=tag_changed,
               placeholder=""
           )
       else:
           tagoption = st.selectbox(
               'Select Tag',
               options=taglist,
               key='taglist_name',
               index=None, 
               on_change=tag_changed,
               placeholder="No tags available"
           )
       
       tagname = st.session_state.taglist_name

    with col2:
        if tagoption:
            try:
                sql2 = "select system$get_tag_allowed_values('" + dbname + "." + schemaname + "." + tagname + "');"
                cur.execute(sql2)
                results2 = cur.fetchall()
                res = []
                for row in results2:
                    qryres2 = str(row[0])
                    res.extend(re.findall(r'"([^"]*)"', qryres2))
                
                def tagvalue_changed():
                       st.session_state.tagvalue = st.session_state.tagvalue

                tagvalueoption = st.selectbox(
                    'Select Tag Value',
                    options=res,
                    key='tagvalue',
                    on_change=tagvalue_changed
                )

                tagvalues = st.session_state.tagvalue 
            except snowflake.connector.errors.ProgrammingError as e:
                st.warning(f"Error: {e}")

    with col3:
        def defaultonload():
            if patternvalue and tagoption and tagvalues:  # Check if all required fields are filled
                try:
                    if add_record_button:
                        # Validate required fields
                        if not dbname:
                            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select Database field</div>", unsafe_allow_html=True)
                        elif not schemaname:
                            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please fill Schema field</div>", unsafe_allow_html=True)
                        elif not objecttype:
                            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please fill Type field</div>", unsafe_allow_html=True)
                        elif not tablepattern:
                            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select NamePattern field</div>", unsafe_allow_html=True)
                        elif not patternvalue:
                            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please fill Type pattern field</div>", unsafe_allow_html=True)
                        elif not tagname:
                            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select tag field</div>", unsafe_allow_html=True)
                        elif not tagvalues:
                            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Please select Tag Value field</div>", unsafe_allow_html=True)
                        else:
                            test = cur.execute("INSERT INTO Metadata_TagManagement(DBName, SchemaName, ObjectType, NamePattern, PatternValue, TagName, TagValue, CreatedDate, IsActive) VALUES  (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), True);", (dbname, schemaname, objecttype, tablepattern, patternvalue, tagname, tagvalues))
                            con.commit()
                            st.markdown("<div style='text-align: center;font-size:20px;font-weight: bold;color: green'>" + "Rule Created Successfully." + "</div>", unsafe_allow_html=True)
                            time.sleep(1) 
                            st.experimental_rerun() 
                    else:
                        st.write("")
                except Exception as e:
                    st.error(f"Error occurred while adding rule: {e}")
            else:
                st.markdown("<div style='text-align: center;font-size:18px;font-weight: bold;color: red'>"+ "One or more required fields are empty. Please fill in all required fields." +"</div>", unsafe_allow_html=True)

            return df
            
        add_record_button = st.button(":heavy_check_mark: Create Rule", on_click=defaultonload, use_container_width=True)

        qryres6 = defaultonload() 

        if qryres6 is not None:
            if "df_value" not in st.session_state:
                st.session_state.df_value = qryres6




with st.container():    
     
        
    st.text("")
    st.text("")
    st.text("Existing rules (all)")



    def updatechkval(edited_display_df, uniqueid):
        
        for uniqueval in uniqueid:
            is_active = edited_display_df.loc[edited_display_df['Row Id'] == uniqueval, 'Is active'].iloc[0]
            
            # Initialize outputres variable
            outputres = ""
            if is_active:
                sql_update = f"UPDATE Metadata_TagManagement SET ISACTIVE = TRUE WHERE UniqueID = {uniqueval};"
                outputres = f"{uniqueval} activated Successfully."
            else:
                sql_update = f"UPDATE Metadata_TagManagement SET ISACTIVE = FALSE WHERE UniqueID = {uniqueval};"
                outputres = f"{uniqueval} deactivated Successfully."

            cur.execute(sql_update)

            # Execute the update statement using your database cursor (e.g., cur.execute(sql_update))
            # Assuming you also want to fetch and display the results
            cur.execute(f"SELECT * FROM Metadata_TagManagement WHERE UniqueID = {uniqueval};")
            jobres = cur.fetchall()
            jobres_df = pd.DataFrame(jobres, columns=[desc[0] for desc in cur.description])
            
            for index, row in jobres_df.iterrows():
                database_name = row['DBNAME']
                object_typevalue = row['OBJECTTYPE']
                name_pattern_value = row['NAMEPATTERN']
                pattern = row['PATTERNVALUE']
                
            outputres = f"{database_name}, {object_typevalue}, {name_pattern_value}, {pattern} {'' if is_active else 'de'}activated Successfully."
            
            # Display the output message using Streamlit
            if outputres != "":
                color = "green" if is_active else "red"
                st.markdown(f"<div style='text-align: center;font-size:20px;font-weight: bold;color: {color}'>{outputres}</div>", unsafe_allow_html=True)
                time.sleep(2)
                st.experimental_rerun()
            thread = threading.Thread(target=updatechkval)
            thread.start()

            
 

with st.container():
    if display_df.empty:
        st.write("")
    else:
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

            # -----------pagination-------------
        filtered_qryres_df = display_df[display_df.index.isin(edited_df.index)]
        if edited_df is not None and not edited_df.equals(st.session_state["df_value"]):
                seluniqueid = edited_df.iloc[(filtered_qryres_df["Is active"] != edited_df["Is active"]).values]["Row Id"].tolist()
                updatechkval(edited_df, seluniqueid)
               
        st.session_state["df_value"] = edited_df 
        
