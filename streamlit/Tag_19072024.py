# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 16:41:47 2023

@author: User
"""

import time
import streamlit as st
import pandas as pd
import numpy as np
import json
import snowflake.connector
import re
import base64
from st_pages import show_pages_from_config
import json
from PIL import Image
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode


show_pages_from_config()

st.set_page_config(page_title="TAGS", layout="wide", page_icon='snowflakelogo.png')

header_style = '''
    <style>
    
        thead, tbody, tfoot, tr, td, th{
            background-color:#e9e9f2;
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
custom_css = {
    "#gridToolBar" :{
        "display":" none !important;"
    },
    ".ag-root.ag-unselectable.ag-layout-normal": {
        "font-size": "13px !important",
        "font-family": "Roboto, sans-serif !important;",
        "background-color": "#ffffff !important;"  # Background color for the grid
    },
    ".ag-header-cell": {
        "background-color": "#29B5E8 !important",  # Header background color
        "color": "#fff !important;"
    },
    ".ag-cell": {
        "color": "#29B5E8 !important;"
    },
    ".ag-row-odd": {
        "background": "#ffffff !important;",
        "border": "1px solid #eee !important;"
    },
    ".ag-row-even": {
        "border-bottom": "1px solid #eee !important;"
    },
    ".ag-theme-light button": {
        "font-size": "0 !important;",
        "width": "auto !important;",
        "height": "24px !important;",
        "border": "1px solid #eee !important;",
        "margin": "4px 2px !important;",
        "background": "#29B5E8 !important;",
        "color": "#fff !important;",
        "border-radius": "3px !important;"
    },
    ".ag-theme-light button:before": {
        "content": "'Confirm' !important",
        "position": "relative !important",
        "z-index": "1000 !important",
        "top": "0 !important",
        "font-size": "12px !important",
        "left": "0 !important",
        "padding": "4px !important"
    }
    }

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

config_database_name = con.database
config_schema_name = con.schema

cur = con.cursor()


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

    db = st.session_state.name

    schsql = 'select SCHEMA_NAME from ' + db + '.information_Schema.schemata;'
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

    schema = st.session_state.schname


# Assuming you have a function from managetag.py
def managetag_tab():
    results7 = ""

    with st.container():
        col1, col2, col3 = st.columns(3)

        with col1:
            sql1=f"SHOW TAGS IN SCHEMA {db}.{schema}" 
            cur.execute(sql1)
            results1 = cur.fetchall()
            taglist = []
            for row in results1:
                qryres1 = str(row[1])
                taglist.append(qryres1)

            def tag_changed():
                st.session_state.tagname1 = st.session_state.tagname1

            if taglist:  
                tagoption = st.selectbox(
                    'Select Tag',
                    options=taglist,
                    key='tagname1',
                    on_change=tag_changed,
                    placeholder=""
                )
            else:
                tagoption = st.selectbox(
                    'Select Tag',
                    options=taglist,
                    key='tagname1',
                    index=None,
                    on_change=tag_changed,
                    placeholder="No tags available"
                )

            tag = st.session_state.tagname1

        with col2:
            st.text("")

        with col3:
            def update():
                st.session_state.name = st.session_state.name
                st.session_state.schname = st.session_state.schname
                st.session_state.tagname1 = st.session_state.tagname1
            if taglist:  # Check if taglist is not empty
                viewobjectsbtn = st.button(":eye: View Objects", on_click=update, use_container_width=True)

                if viewobjectsbtn:
                  
                    if tagoption:
                        sql7 = "select OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, TAG_NAME, TAG_VALUE, DOMAIN from snowflake.account_usage.tag_references where tag_name= "" '" + tag + "' "" and OBJECT_DATABASE= "" '" + db + "' "" and OBJECT_SCHEMA= "" '" + schema + "' "" ;"
                        cur.execute(sql7)
                        results7 = cur.fetchall()

                        mgwithtagdf = pd.DataFrame(results7, columns=[desc[0] for desc in cur.description])
                        mgwithtagdf = mgwithtagdf.rename(columns={
                            'OBJECT_DATABASE' :'Object Database',
                            'OBJECT_SCHEMA' :'Object Schema',
                            'OBJECT_NAME' : 'Object Name',
                            'TAG_NAME' : 'Tag Name',
                            'TAG_VALUE' :'Tag Value',
                            'DOMAIN':'Domain' ,
                            'COLUMN_NAME' : 'Column Name'

                        })
                        alltag = mgwithtagdf["Tag Name"]
                        alldb = mgwithtagdf["Object Database"]
                        alltable = mgwithtagdf["Object Name"]
                        allschema = mgwithtagdf["Object Schema"]
                        alldomain = mgwithtagdf["Domain"]
                        qryresults8 = []
                        for taglist, dblist, taablelist, schemalist, domainlist in zip(alltag, alldb, alltable,
                                                                                       allschema,
                                                                                       alldomain):
                            taglist = taglist.upper()
                            dblist = dblist.upper()
                            taablelist = taablelist.upper()
                            schemalist = schemalist.upper()
                            domainlist = domainlist.upper()
                            qryres8 = ''
                            try:
                                if domainlist == "COLUMN":
                                    sql8 = (
                                            "select COLUMN_NAME from snowflake.account_usage.tag_references where OBJECT_DATABASE= "" '" + db + "' "" and OBJECT_SCHEMA= "" '" + schema + "' "" and object_name="" '" + taablelist + "' "" and tag_name="" '" + taglist + "' "" and domain ='COLUMN'")
                                    cur.execute(sql8)
                                    results8 = cur.fetchall()

                                    if len(results8) == 0:
                                        qryresult = "-"
                                        qryresults8.append(qryresult)
                                    else:
                                        for roww in results8:
                                            qryres8 += str(roww[0]) + ','

                                        qryresult = qryres8.rstrip(',')
                                        qryresults8.append(qryresult)
                                else:
                                    qryresult = "-"
                                    qryresults8.append(qryresult)


                            except:
                                qryresult = "-"
                                qryresults8.append(qryresult)
                                pass
                        columndf1 = pd.DataFrame(qryresults8, columns=['COLUMN_NAME'])
                        columndf1 = columndf1.rename(columns={
                            'COLUMN_NAME' : 'Column Name'

                        })

            st.text("")
            st.text("")

    def split_frame(input_df, rows):
        df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
        return df

    with st.container():
        if results7 != "":
            viewobjtagdf = mgwithtagdf.join(columndf1)
            viewobjtagdf = viewobjtagdf.drop_duplicates(
                subset=["Object Database", "Object Schema", "Object Name", "Tag Name", "Tag Value",
                        "Column Name"])
            if len(viewobjtagdf) == 0:
               st.write("")
               st.write("")
               st.write("")
               st.markdown(f"<div style='text-align: center;font-size:20px;font-weight: bold;color: red'>Tag not set to any objects.</div>", unsafe_allow_html=True)

            else:
                bottom_menu = st.columns((4, 1, 1))
                with bottom_menu[2]:
                    # Pagination
                    data_length = len(viewobjtagdf)
                    max_batch_size = int(data_length * 0.1)
                    max_batch_size = max(max_batch_size, 10)
                    max_batch_size = ((max_batch_size + 4) // 5) * 5
                    batch_size_options = [i for i in range(5, max_batch_size + 1, 5)]
                    batch_size = st.selectbox("Records per page", options=batch_size_options)
                with bottom_menu[1]:
                    total_pages = (data_length + batch_size - 1) // batch_size
                    current_page = st.number_input("Page", min_value=1, max_value=total_pages, step=1)
                with bottom_menu[0]:
                    st.markdown(f"Page **{current_page}** of **{total_pages}** ")

                if total_pages > 0:
                    pages = split_frame(viewobjtagdf, batch_size)
                    pageframe = pages[current_page - 1]
                    st.data_editor(pageframe, use_container_width=True, key=f"data_editor_{current_page}", hide_index=True)



def tag_tab():
    output = ""
    with st.container():
        col1, col2, col3 = st.columns(3)

        with col1:
            sql1=f"SHOW TAGS IN SCHEMA {db}.{schema}" 
            try:
                cur.execute(sql1)
                results1 = cur.fetchall()
                taglist = []

                for row in results1:
                    qryres1 = str(row[1])
                    taglist.append(qryres1)
                     
            except Exception as e:
                st.error(f"Error fetching tag list: {e}")
                st.stop()


            def tag_changed():
                st.session_state.tagname = st.session_state.tagname

            if taglist:  
                    tagoption = st.selectbox(
                        'Select Tag',
                        options=taglist,
                        key='tagname',
                        on_change=tag_changed,
                        placeholder=""
                    )
            else:
                tagoption = st.selectbox(
                    'Select Tag',
                    options=taglist,
                    key='tagname',
                    index=None,
                    on_change=tag_changed,
                    placeholder="No tags available"
                )

            tag = st.session_state.tagname

        with col2:
            if not tagoption:
                col1.empty()
                col2.empty()
                col3.empty()
                st.stop()
            try:
                sql2 = "select system$get_tag_allowed_values('" + db + "." + schema + "." + tag + "');"
                cur.execute(sql2)
                results2 = cur.fetchall()
                for row in results2:
                    qryres2 = str(row[0])
                    res = re.findall(r'"([^"]*)"', qryres2)
                    
            except Exception as e:
                    if "002003" in str(e):  # Check if the error code matches the specific SQL compilation error
                        st.error("The specified tag does not exist or you don't have permission to access it.")
                    else:
                        st.error(f"Error fetching tag allowed values: {e}")
                    st.stop()

            def tagvalue_changed():
                st.session_state.tagvalue = st.session_state.tagvalue

            tagvalueoption = st.selectbox(
                'Select Tag Value',
                options=res,
                key='tagvalue',
                on_change=tagvalue_changed
            )

            tagvalue = st.session_state.tagvalue

        with col3:
            st.text("")



    with st.container():

        col1, col2, col3 = st.columns(3)

        with col1:
            def objtype_changed():
                st.session_state.objtype = st.session_state.objtype

            table2 = st.selectbox(
                'Select Type',
                options=('Table', 'View', 'Column'),
                key='objtype',
                on_change=objtype_changed)

            obj_type = st.session_state.objtype

        with col2:
            table3 = st.selectbox(
                'Name pattern',
                options=('Starts with', 'Ends with', 'Contains'),
                key='tblpatt')

            table_pattern = st.session_state.tblpatt

        with col3:
            table4 = st.text_input('Type pattern Eg: cust_tbl', key='name1')
            st.markdown("""<div style='width:80%;'><p style='color:#6b6f76 ; font-weight:bold;'><i class="fas fa-info-circle"></i> Please enter a value for pattern to search and press Enter key</p></div>""",unsafe_allow_html=True)
            pattern_value = st.session_state.name1

    with st.container():
        if 'tag_applied_successfully' not in st.session_state:
            st.session_state.tag_applied_successfully = False
        if not tagoption:
            col1.empty()
            col2.empty()
            col3.empty()
            st.stop()


        def execute_query(db, schema, obj_type, table_pattern, pattern_value):

            if obj_type.lower() == 'table':
                table_type_condition = "table_type='BASE TABLE'"
                table_pattern = table_pattern.replace(' ', '')
                search_query = f"SELECT table_name FROM {db}.information_schema.tables WHERE table_schema='{schema}' AND {table_type_condition} AND {table_pattern}(LOWER(table_name), '{pattern_value}');"
                cur.execute(search_query)
                searchresults = cur.fetchall()
                getalltableNames = pd.DataFrame(searchresults, columns=[desc[0] for desc in cur.description])
                
                getalltableNames = getalltableNames.rename(columns={
                'TABLE_NAME' :'Table Name',
                })
            elif obj_type.lower() == 'view':
                table_type_condition = "table_type='VIEW'"
                table_pattern = table_pattern.replace(' ', '')
                search_query = f"SELECT table_name FROM {db}.information_schema.tables WHERE table_schema='{schema}' AND {table_type_condition} AND {table_pattern}(LOWER(table_name), '{pattern_value}');"
                cur.execute(search_query)
                searchresults = cur.fetchall()
                getalltableNames = pd.DataFrame(searchresults, columns=[desc[0] for desc in cur.description])
                getalltableNames = getalltableNames.rename(columns={
                'TABLE_NAME' :'Table Name',
                })
            elif obj_type.lower() == 'column':
                table_pattern = table_pattern.replace(' ', '')
                search_query = f"SELECT table_name,column_name FROM {db}.information_schema.columns WHERE table_schema='{schema}' AND {table_pattern}(LOWER(column_name), '{pattern_value}');"
                cur.execute(search_query)
                searchresults = cur.fetchall()
                getalltableNames = pd.DataFrame(searchresults, columns=[desc[0] for desc in cur.description])
                getalltableNames = getalltableNames.rename(columns={
                'TABLE_NAME' :'Table Name',
                'COLUMN_NAME' : 'Column Name'
                })

            if getalltableNames.empty:
                html_string = "<h4 style='color:#000;text-align: center;'>No objects available in applied tag.</h4>"
                st.markdown(html_string, unsafe_allow_html=True)
                return 'None'

            else:
                selected_type = st.session_state.objtype

                if selected_type.lower() == 'table' or selected_type.lower() == 'view':
                    alltables = getalltableNames["Table Name"]
                    qryresults8 = []
                    for tbl in alltables:
                        tbl = tbl.upper()
                        qryres8 = ''
                        try:
                            sql8 = ("SELECT TAG_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.tag_references WHERE object_database='" + db + "' AND object_Schema='" + schema + "' AND object_name = '" + tbl + "'")                                                   
                            cur.execute(sql8)                        
                            results8 = cur.fetchall()
                            if len(results8) == 0:
                                qryresult = "-"
                                qryresults8.append(qryresult)
                            else:
                                for roww in results8:
                                    qryres8 += str(roww[0]) + ','

                                qryresult = qryres8.rstrip(',')
                                qryresults8.append(qryresult)

                        except Exception as e:
                            st.error(f"Error fetching : {e}")
                            pass
                    df1 = pd.DataFrame(qryresults8, columns=['Tag Applied'])

                    
                elif selected_type.lower() == 'column':
                    alltables = getalltableNames["Table Name"]
                    allcolumns = getalltableNames["Column Name"]
                    qryresults8 = []
                    for tbl, clm in zip(alltables, allcolumns):
                        tbl = tbl.upper()
                        clm = clm.upper()
                        qryres8 = ''
                        try:
                            sql8 = ("SELECT TAG_NAME FROM snowflake.account_usage.tag_references where object_database='"+ db +"' AND object_Schema='"+ schema +"' and column_name='"+ clm +"'")
                            cur.execute(sql8)
                            results8 = cur.fetchall()

                            if len(results8) == 0:
                                qryresult = "-"
                                qryresults8.append(qryresult)
                            else:
                                for roww in results8:
                                    qryres8 += str(roww[0]) + ','

                                qryresult = qryres8.rstrip(',')
                                qryresults8.append(qryresult)

                        except Exception as e:
                            st.error(f"Error fetching : {e}")
                            pass
                    df1 = pd.DataFrame(qryresults8, columns=['Tag Applied'])

                searchtagdf = getalltableNames.join(df1)

                searchdf_with_selections = searchtagdf.copy()
                searchdf_with_selections.insert(0, "Select", False)


                edited_df = st.data_editor(
                    searchdf_with_selections,
                    hide_index=True,
                    column_config={"Select": st.column_config.CheckboxColumn(required=True)},
                    disabled=searchtagdf.columns,
                    use_container_width=True
                )
                selected_indices = list(np.where(edited_df.Select)[0])
                selected_table_names = []
                selected_column_names = []

                for row_index in selected_indices:
                    # Access the row using integer index
                    row = searchdf_with_selections.iloc[row_index]
                    
                    # Check if both TABLE_NAME and COLUMN_NAME are in the columns
                    if 'TABLE_NAME' in searchdf_with_selections.columns and 'COLUMN_NAME' in searchdf_with_selections.columns:
                        selected_table_name = row['TABLE_NAME']
                        selected_column_name = row['COLUMN_NAME']  # Use .get() to avoid KeyError if COLUMN_NAME is missing
                    else:
                        selected_table_name = row['Table Name']
                        selected_column_name = ''
                    selected_table_names.append(selected_table_name)
                    selected_column_names.append(selected_column_name)
                selected_tables_and_columns = list(zip(selected_table_names, selected_column_names))
                st.session_state.tbllist = selected_tables_and_columns
                
                return st.session_state.tbllist
            pass
        
        if pattern_value != '':
            
            tagvalue = st.session_state.tagvalue
            tag = st.session_state.tagname
            qryres6 = execute_query(db, schema, obj_type, table_pattern.lower(), pattern_value.lower())
            if qryres6 != 'None':
                tableitem =''
                columnitem=''
                for tableitem, columnitem in st.session_state.tbllist:
                    pass
                if tableitem !='' and columnitem !='':
                    table_column_string = ", ".join([f"{table} - {column}" for table, column in st.session_state.tbllist])
                    st.write(f"You have selected tag {tag} and tag value is {tagvalue} and selected table(s) and column(s) {table_column_string}.")
                elif tableitem !='' and columnitem =='':
                    table_string = ", ".join([table[0] for table in st.session_state.tbllist])
                    st.write(f"You have selected tag {tag} and tag value is {tagvalue} and selected table(s) {table_string}.")

                with st.container(): 
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.text("")
                    with col2:
                        st.text("")
                    with col3: 
                        def update():
                            st.session_state.name = st.session_state.name
                            st.session_state.schname = st.session_state.schname
                            st.session_state.tagname = st.session_state.tagname
                            st.session_state.tagvalue = st.session_state.tagvalue
                    
                        applytagbtn = st.button(":heavy_plus_sign: Apply Tag", on_click=update, use_container_width=True)
                        
                        if applytagbtn:
                            if st.session_state.tbllist:
                                for table, columnvalues in st.session_state.tbllist:
                                    try:
                                        applyqury = f"CALL {config_database_name}.{config_schema_name}.TAG_Management_ApplyTag('{db}', '{schema}', '{table}', '{tag}', '{tagvalue}', '{obj_type}', '{columnvalues}')"
                                        cur.execute(applyqury)
                                        output = cur.fetchone()
                                        
                                    except Exception as e:
                                            st.error(f"Error fetching : {e}")
                            else:
                                st.markdown("<div style='text-align: center;font-size:16px;font-weight: bold;color: red'>Please select atleast one object to apply tag</div>",unsafe_allow_html=True)
                                time.sleep(1)
                                st.experimental_rerun()
                    if output and output[0]:
                        st.session_state.tag_applied_successfully = True
                        st.rerun()
                    
                    # Check if the tag was applied successfully and display a message accordingly
                    if st.session_state.tag_applied_successfully:
                        st.markdown("<div style='text-align: center;font-size:25px;font-weight: bold;color: green'>Tag applied successfully</div>",
                                    unsafe_allow_html=True) 
                        st.session_state.tag_applied_successfully = False
                        time.sleep(1)
                        st.experimental_rerun()           

def main():
    tab1, tab2 = st.tabs(["📑 Manage Tag", "🏷️ Apply Tag"])

    with tab1:
        st.markdown(" ### :bookmark_tabs: **Manage Tags**")
        st.markdown("""<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">""", unsafe_allow_html=True)
        st.markdown('<span style="color:#6b6f76 ; font-weight:bold;" ><i class="fas fa-info-circle"></i> Select a database and schema on your left-hand side to manage tags</span>',unsafe_allow_html=True)
        managetag_tab()

    with tab2:
        st.markdown(" ### :label: **Apply Tags**")
        st.markdown('<span style="color:#6b6f76 ; font-weight:bold;" ><i class="fas fa-info-circle"></i> Select a database and schema on your left-hand side to apply tags</span>',unsafe_allow_html=True)
        tag_tab()


if __name__ == "__main__":
    main()