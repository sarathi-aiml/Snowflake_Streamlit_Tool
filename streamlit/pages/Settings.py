import streamlit as st
import json
import os
import snowflake.connector
import pandas as pd

st.set_page_config(page_title="App Settings", layout="wide", page_icon='snowflakelogo.png')

header_style = '''
    <style> 
        [data-testid='stSidebarNav'] > ul {
            min-height: 38vh;
        } 
        [data-testid="stSidebar"] {
                    background-image: url('https://pbsinfosystems.com/applogo.png');
                    background-repeat: no-repeat;
                    padding-top: 30px;
                    background-position: 20px 20px;
                }
    .st-emotion-cache-xujc5b p {
    font-size: 20px;
    font-weight: bold;
    color: #29B5E8;
}
    </style>
'''
st.markdown(header_style, unsafe_allow_html=True)
st.markdown("""<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">""", unsafe_allow_html=True)

# Function to load data from JSON file
def load_from_json(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return {}


# Function to connect to Snowflake using credentials from JSON file
def connect_to_snowflake(creds):
    try:
        conn = snowflake.connector.connect(
            user=creds["user"],
            password=creds["password"],
            account=creds["account"],
            warehouse=creds["warehouse"],
            database=creds["database"],
            schema=creds["schema"],
        )
        return conn
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None


# Function to save data to JSON file
def save_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)


# Streamlit page layout
st.title('App Configuration')

# Try to load existing data
existing_data = load_from_json('app_config.json')

# Initialize the snowflake_config and app_settings
snowflake_config = existing_data.get("snowflake", {})
app_settings = existing_data.get("app_settings", {})

# Flags to check if forms are submitted
snowflake_form_submitted = False
app_settings_form_submitted = False

# Form for Snowflake configuration
with st.form("snowflake_form"):
    st.subheader("Snowflake Configuration")
    with st.container():
        col1, col2, col3 = st.columns(3, gap='large')
        with col1:
            user = st.text_input("Username", value=snowflake_config.get("user", ""))
        with col2:
            password = st.text_input("Password", type="password", value=snowflake_config.get("password", ""))
        with col3:
            account = st.text_input("Account", value=snowflake_config.get("account", ""))
    with st.container():
        col4, col5, col6 = st.columns(3, gap='large')
        with col4:
            warehouse = st.text_input("Warehouse", value=snowflake_config.get("warehouse", ""))
        with col5:
            #database = st.selectbox("Database", ["TRIAL_123", "DB_STREAMLIT"], index=0)  # Example dropdown options
            database = st.text_input("Database", value=snowflake_config.get("database", ""))
        with col6:
            #schema = st.selectbox("Schema", [ "PUBLIC","SC_STREAMLIT"], index=0)  # Example dropdown options
            schema = st.text_input("Schema", value=snowflake_config.get("schema", ""))
    # Submit button for the form
    snowflake_form_submitted = st.form_submit_button("Submit")

# Update snowflake_config only if the form was submitted
if snowflake_form_submitted:
    if not user.strip():
        st.error("Please fill Username.")
    elif not password.strip():
        st.error("Please fill Password.")
    elif not account.strip():
        st.error("Please fill Account.")
    elif not warehouse.strip():
        st.error("Please fill Warehouse.")
    elif not database.strip():
        st.error("Please fill Database.")
    elif not schema.strip():
        st.error("Please fill Schema.")
    else:
        snowflake_config = {
            "user": user,
            "password": password,
            "account": account,
            "warehouse": warehouse,
            "database": database,
            "schema": schema
        }
        # Proceed with your Snowflake configuration or any other action
        # You can print or use the snowflake_config dictionary as needed
        st.success("Snowflake configuration saved successfully.")

# Form for App Settings
with st.form("app_settings_form"):
    st.subheader("App Settings")
    with st.container():
        col1, col2, col3 = st.columns(3, gap='large')
        with col1:
            metadata_table_name = st.text_input("Metadata Table Name",
                                                value=app_settings.get("metadata_table_name", ""))
        with col2:
            refresh_interval_hrs = st.number_input("Refresh Interval (hrs)", min_value=0,
                                                   value=app_settings.get("refresh_interval_hrs", 2), step=1)
        with col3:
            st.text("")
    # Submit button for the form
    app_settings_form_submitted = st.form_submit_button("Save Settings")

# Update app_settings only if the form was submitted
if app_settings_form_submitted:
    app_settings = {
        "metadata_table_name": metadata_table_name,
        "refresh_interval_hrs": refresh_interval_hrs
    }

# Combine configurations and save to JSON file if any form is submitted
if snowflake_form_submitted or app_settings_form_submitted:
    combined_config = {
        "snowflake": snowflake_config,
        "app_settings": app_settings
    }
    save_to_json(combined_config, 'app_config.json')
    st.success("Configuration saved to app_config.json")

# # Schedule History
# st.subheader("Schedule History")
# file_path = 'jobs_data.csv'  # Update this to the path of your CSV file
# df = pd.read_csv(file_path)
# st.dataframe(df, hide_index=True, use_container_width=True)

#------------------------------------------metadata--------------------------------------------------------#
# Display the metadata table code
st.write("Metadata table script & sample row")

MetaDatacode = """
Create or replace table sch_metadata (
    id int,
    job_name varchar(255),
    job_frequency varchar(255),
    day_of_week int,
    day_of_month int,
    daily_time varchar(25),
    proc_to_call varchar(255),
    lastrundatetime DATETIME,
    is_active boolean
);

insert into sch_metadata values(
    1, -- Row ID
    'apply tag', -- Job Name
    'every', -- Job Frequency : every, daily, weekly, monthly
    2, -- Day of the week. Monday =1... Sunday =7. Not Applicable =0
    8,--day of the month : calendar date of the month. 0- Not Applicable
    '21:30', -- Time to run HH:MM
    'DB_STREAMLIT.SC_STREAMLIT.SPU_WAREHOUSE_MANAGEMENT',-- Stored proc to run at the schduled time ,
    CURRENT_TIMESTAMP(), -- Assuming you want to set this to the current timestamp,
    1 -- Active records indicator 
);
"""
st.code(MetaDatacode, language='sql')

if st.button("Run Metadata table SQL"):
    # Load database credentials
    db_config = load_from_json('app_config.json').get("snowflake", {})
    if db_config:
        # Connect to the database
        conn = connect_to_snowflake(db_config)
        if conn:
            try:
                # Split the SQL code into individual statements
                statements = MetaDatacode.split(';')
                with conn.cursor() as cur:
                    for statement in statements:
                        # Trim whitespace and skip empty statements
                        trimmed_statement = statement.strip()
                        if trimmed_statement:
                            cur.execute(trimmed_statement)
                    st.success("SQL executed successfully.")
            except Exception as e:
                st.error(f"SQL execution error: {e}")
            finally:
                conn.close()
    else:
        st.error("Database configuration is missing.")
# --------------------------------------------------------------------------------------------------------#


st.write("Run Alert Management table SQL")

MetaDatacode = """
create or replace TABLE Metadata_AlertManagement(
    Alert_Management_Key int identity(1,1) COMMENT 'KEY COLUMN RUNNING NUMBER',
    ALERT_NAME VARCHAR(100),
    SQL_QUERY VARCHAR(100000) COLLATE 'en-ci',
    FREQUENCY VARCHAR(50) COLLATE 'en-ci',--Weekly_5_16:30 (Weekly_=Weekly_,5=Dayof the week,16:60=hh:mm) OR Daily_16:30 (Daily_ = Daily_, 16:30 = hh:mm) OR Every_2 (Every_= Every_,2 = Duraton of exeuton)
    FREQUENCY_DAY int,
    EMAIL VARCHAR(500) COLLATE 'en-ci',
    ACTIVATE_DAY VARCHAR(500),
    SUBJECT VARCHAR(500)  COLLATE 'en-ci',  
    ISACTIVE BOOLEAN,
    LASTRUNDATETIME TIMESTAMP_NTZ(9),-- edn date time
    LASTRUNSTATUS BOOLEAN,
    LASTRUNERROR VARCHAR(1000) COLLATE 'en-ci',
    LASTSTARTDATETIME VARCHAR(1000) COLLATE 'en-ci',
    COLUMNNAME VARCHAR(1000) COLLATE 'en-ci',
    IsRunning BOOLEAN    
);
"""

st.code(MetaDatacode, language='sql')

if st.button("Run AlertManagement table SQL"):
    # Load database credentials
    db_config = load_from_json('app_config.json').get("snowflake", {})
    if db_config:
        # Connect to the database
        conn = connect_to_snowflake(db_config)
        if conn:
            try:
                # Split the SQL code into individual statements
                statements = MetaDatacode.split(';')
                with conn.cursor() as cur:
                    for statement in statements:
                        # Trim whitespace and skip empty statements
                        trimmed_statement = statement.strip()
                        if trimmed_statement:
                            cur.execute(trimmed_statement)
                    st.success("SQL executed successfully.")
            except Exception as e:
                st.error(f"SQL execution error: {e}")
            finally:
                conn.close()
    else:
        st.error("Database configuration is missing.")

# ----------------------------------- Tag management tab-------------------------------------------------#


def run_tags_management():
    # Functionality for "Run Tags Management table SQL"
    st.write('Run Tags Management table SQL')
    db_config = load_from_json('app_config.json').get("snowflake", {})
    if db_config:
        # Connect to the database
        conn = connect_to_snowflake(db_config)
        if conn:
            try:
                cur = conn.cursor()

                create_database_query = f"CREATE DATABASE IF NOT EXISTS {snowflake_config['database']};"
                create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {snowflake_config['database']}.{snowflake_config['schema']};"

                create_tag_table_query = f"""
                create or replace TABLE {snowflake_config["database"]}.{snowflake_config["schema"]}.METADATA_TAGMANAGEMENT (
                UNIQUEID NUMBER(38,0) autoincrement start 1 increment 1 order,
                DBNAME VARCHAR(30),
                SCHEMANAME VARCHAR(20),
                OBJECTTYPE VARCHAR(10),
                NAMEPATTERN VARCHAR(20),
                PATTERNVALUE VARCHAR(50),
                TAGNAME VARCHAR(20),
                TAGVALUE VARCHAR(30),
                CREATEDDATE TIMESTAMP_LTZ(9),
                ISACTIVE BOOLEAN,
                LASTRUNDATETIME TIMESTAMP_NTZ(9),
                LASTRUNSTATUS BOOLEAN,
                LASTRUNERROR VARCHAR(8000)
                )"""

                create_list_tag_proc_query = """
                CREATE OR REPLACE PROCEDURE """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.SPU_TAG_MANAGEMENT_APPLYTAG_SCHEDULE()
                RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
execute as caller
AS '
                try
                {       
                    var Query_Get_Metadata="",ResultSet_MetaData="",uniqueid="",dbname="",schemaname="",objecttype="",namepattern="",patternvalue="",tagname="";    
                    var tagvalue="",rulestatus="",isexecuted="";    
                    var Query_SchemaInfo="",stmt_SchemaInfo="",ResultSet_SchemaInfo="";
                    var stmt_metadata="",ResultSet_MetaData="";
                    
                    Query_Get_Metadata +=" select uniqueid,dbname,schemaname,objecttype,namepattern,patternvalue,tagname,tagvalue, ";
                    Query_Get_Metadata +=" ifnull(isactive,false) as rulestatus ";
                    Query_Get_Metadata +=" from """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_TagManagement";    
                     
                    stmt_metadata = snowflake.createStatement({sqlText:Query_Get_Metadata});
                    ResultSet_MetaData = stmt_metadata.execute(); 
                 
                    if(!ResultSet_MetaData.next())
                    {
                        return "No records found in metadata table to do tag action";
                    } 
                    var querytext="";
                    for (var i = 1; i <= stmt_metadata.getRowCount(); i++) 
                    {
                        uniqueid = ResultSet_MetaData.getColumnValue(1);                       
                        dbname = ResultSet_MetaData.getColumnValue(2);                       
                        schemaname = ResultSet_MetaData.getColumnValue(3);                       
                        objecttype = ResultSet_MetaData.getColumnValue(4);                       
                        namepattern = ResultSet_MetaData.getColumnValue(5);                       
                        patternvalue = ResultSet_MetaData.getColumnValue(6);                       
                        tagname = ResultSet_MetaData.getColumnValue(7);                       
                        tagvalue = ResultSet_MetaData.getColumnValue(8);     
                        rulestatus = ResultSet_MetaData.getColumnValue(9);                               
                
                        var tablename_schema ="",columnname_schema="",fun_columnname="";
                    
                        if(objecttype.toUpperCase() == "TABLE")
                        {
                            tablename_schema ="information_schema.tables";
                            columnname_schema="table_catalog,table_schema,table_name";
                            fun_columnname="Table_name";
                            objecttype = "BASE TABLE";           
                        }
                        else if (objecttype.toUpperCase() == "VIEW")
                        {
                            fun_columnname="Table_name";
                            tablename_schema ="information_schema.tables";
                            columnname_schema="table_catalog,table_schema,table_name";
                            objecttype = "VIEW";
                        }
                        else if (objecttype.toUpperCase() == "COLUMN")
                        {
                            fun_columnname="column_name";
                            columnname_schema="table_catalog,table_schema,table_name,column_name";
                            tablename_schema ="information_schema.columns";
                            objecttype = "COLUMN";
                        }                
                        
                        namepattern = namepattern.toUpperCase();
                        patternvalue = patternvalue.toUpperCase();
                        
                        var sWhereclause="";
                        
                        if(namepattern == "STARTS WITH")
                        {
                            sWhereclause =" STARTSWITH("+ fun_columnname +", ''"+ patternvalue +"'')";            
                        }
                        else if (namepattern == "ENDS WITH")
                        {
                            sWhereclause =" ENDSWITH("+ fun_columnname +", ''"+ patternvalue +"'')";
                        }
                        else if (namepattern == "CONTAINS")
                        {
                            sWhereclause =" CONTAINS("+ fun_columnname +", ''"+ patternvalue +"'')";            
                        }
                        else if (namepattern == "CREATED BY USER")
                        {
                            sWhereclause =" UPPER(LAST_DDL_BY) =''"+ patternvalue +"''";                        
                        }
                        else if (namepattern == "HAS ROLE")
                        {
                            sWhereclause =" UPPER(table_owner) =''"+ patternvalue +"''";                        
                        }
                        else
                        {
                            Query_SchemaInfo="";
                        }           
                           
                
                        Query_SchemaInfo = "select "+ columnname_schema +" from "+ dbname +"."+ tablename_schema +" where "+sWhereclause;        
                        Query_SchemaInfo += " and UPPER(table_schema)=''"+ schemaname +"'' ";
                        Query_SchemaInfo += " and UPPER(table_catalog)=''"+ dbname +"'' " ; 
                        if(objecttype.toUpperCase() == "TABLE" || objecttype.toUpperCase() == "VIEW")
                        {
                            Query_SchemaInfo +=" and UPPER(table_type)=''"+ objecttype +"'' ";
                        }       
                       
                        var stmt_tag = snowflake.createStatement({sqlText:Query_SchemaInfo});
                        var ResultSet_tag = stmt_tag.execute();      
                        ResultSet_tag.next();
                        
                        for (var j = 1; j <= stmt_tag.getRowCount(); j++) 
                        {
                            var table_catalog = ResultSet_tag.getColumnValue(1);  
                            var table_schema = ResultSet_tag.getColumnValue(2);  
                            var table_name = ResultSet_tag.getColumnValue(3);
                            var query_apply_tag="";
                            
                            if(objecttype.toUpperCase() == "COLUMN")
                            {
                                var column_name = ResultSet_tag.getColumnValue(4); 
                                if(rulestatus == true)
                                {
                                    query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" ";
                                    query_apply_tag +=" modify column "+ column_name +" SET TAG "+tagname +" = ''"+ tagvalue +"''";
                                }
                                else
                                {
                                    query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" ";
                                    query_apply_tag +="modify column "+ column_name +" UNSET TAG "+tagname +" ";
                                    
                                }                  
                            }
                            else
                            {
                                if(rulestatus == true)
                                {
                                     query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" SET TAG "+tagname +" = ''"+ tagvalue +"''";
                                }
                                else
                                {
                                    query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" UNSET TAG "+tagname +" ";
                                }            
                            }   
                            //return query_apply_tag;

                            var qry_get_curr_db ="SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();";
                            var stmt_get_curr_db = snowflake.createStatement({sqlText:qry_get_curr_db});
                            var ResultSet_get_curr_db = stmt_get_curr_db.execute();      
                            ResultSet_get_curr_db.next();

                            var current_catalog = ResultSet_tag.getColumnValue(1);  
                            var current_schema = ResultSet_tag.getColumnValue(2);                              

                            var qry_usedb ="use "+ table_catalog +"."+ table_schema +"";  
                            
                            var stmt_qry_usedb = snowflake.createStatement({sqlText:qry_usedb});
                            var ResultSet_qry_usedb = stmt_qry_usedb.execute();             

                             
                            var stmt_apply_tag = snowflake.createStatement({sqlText:query_apply_tag});                           
                            var ResultSet_apply_tag = stmt_apply_tag.execute();                                        
                            ResultSet_tag.next();  

                            var qry_usedb_curr ="use "+ current_catalog +"."+ current_schema +""; 
                            //return qry_usedb_curr;
                            var stmt_qry_usedb_curr = snowflake.createStatement({sqlText:qry_usedb_curr});
                            var ResultSet_qry_usedb_curr = stmt_qry_usedb_curr.execute();  
                            
                        }
                
                        var query_update_result ="";
                        query_update_result="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_TagManagement set lastrunstatus=true,lastrundatetime=CURRENT_TIMESTAMP(),";
                        //return query_update_result;
                        query_update_result +=" lastrunerror='''' where uniqueid ="+uniqueid;        
                        var stmt_result_update = snowflake.createStatement({sqlText:query_update_result});
                        var result_update = stmt_result_update.execute();
                        
                        ResultSet_MetaData.next();  
                    }
                    
                    return "success";
                    
                     
                }    
                catch(err)
                {
                    var errstr;
                    errstr = "Failed: Code: " + err.code + "  State: " + err.state;
                    errstr += "  Message: " + err.message;
                    errstr += " Stack Trace:" + err.stackTraceTxt;
                    errstr = errstr.replaceAll("''"," ");
                
                    var query_update_result ="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_TagManagement set"; 
                    query_update_result +=" lastrunstatus=false,lastrundatetime=CURRENT_TIMESTAMP(),";
                    query_update_result +=" lastrunerror=''"+errstr+"'' where uniqueid="+uniqueid;
                
                    //return query_update_result;
                    var stmt_result_update = snowflake.createStatement({sqlText:query_update_result});
                    var result_update = stmt_result_update.execute();    
                    return "failure";
                }
                ';"""

                create_task_tag_objects_proc_query = """
                     create or replace task """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.TK_TAGMANAGEMENT
                        warehouse=COMPUTE_WH
                        schedule='USING CRON 0 22 * * * UTC'
                        as Call """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.SPU_TAG_MANAGEMENT_APPLYTAG_SCHEDULE();
                """

                create_tag_applytag = """
                CREATE OR REPLACE PROCEDURE """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.TAG_Management_ApplyTag(DBNAME VARCHAR,SCHEMANAME VARCHAR,OBJECTNAME VARCHAR,TAGNAME VARCHAR,TAGVALUE VARCHAR,OBJECTTYPE VARCHAR,COLUMNNAME VARCHAR)
                RETURNS STRING
                LANGUAGE JAVASCRIPT
                EXECUTE AS CALLER
                AS
                $$
                    try 
                    {
                        var local_dbname = DBNAME;  
                        var local_schemaname = SCHEMANAME;  
                        var local_objectname = OBJECTNAME;  
                        var local_tagname = TAGNAME;  
                        var local_tagvalue = TAGVALUE;  
                        var local_objecttype = OBJECTTYPE;  
                        var local_columnname = COLUMNNAME; 

                        if(local_objecttype.toUpperCase() != "TABLE" && local_objecttype.toUpperCase() != "VIEW" && local_objecttype.toUpperCase() != "COLUMN")
                        {
                            return 'Object type parameter value should be table or view or column. Please check the object type parameter value';
                        }
                        
                        if(local_objecttype.toUpperCase() == "COLUMN")
                        {
                            if(local_dbname == "" || local_schemaname =="" || local_objectname =="" || local_tagname == "" || local_tagvalue == "" || local_columnname == 
                            "")
                            {
                                return 'Procedure parameters value should not be empty. Please check all parameters values';
                            }
                        }
                        else
                        {
                            if(local_dbname == "" || local_schemaname =="" || local_objectname =="" || local_tagname == "" || local_tagvalue == "")
                            {
                                return 'Procedure parameters value should not be empty. Please check all parameters values';
                            }
                        }

                        if(local_objecttype.toUpperCase() == "COLUMN")
                        {            
                            query_apply_tag ="ALTER TABLE IF EXISTS "+local_dbname+"."+local_schemaname+"."+local_objectname+" ";
                            query_apply_tag +=" modify column "+ local_columnname +" SET TAG "+local_tagname +" = '"+ local_tagvalue +"'";            
                        }
                        else
                        {           
                            query_apply_tag ="ALTER "+ local_objecttype +" IF EXISTS "+local_dbname+"."+local_schemaname+"."+local_objectname+" ";
                            query_apply_tag +=" SET TAG "+local_tagname +" = '"+ local_tagvalue  +"'";           
                        }
                        //return query_apply_tag;
                        var qry_get_curr_db ="SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();";
                        var stmt_get_curr_db = snowflake.createStatement({sqlText:qry_get_curr_db});
                        var ResultSet_get_curr_db = stmt_get_curr_db.execute();      
                        ResultSet_get_curr_db.next();

                        var current_catalog = ResultSet_get_curr_db .getColumnValue(1);  
                        var current_schema = ResultSet_get_curr_db .getColumnValue(2);                              

                        var qry_usedb ="use "+ local_dbname +"."+ local_schemaname +"";  
                        
                        var stmt_qry_usedb = snowflake.createStatement({sqlText:qry_usedb});
                        var ResultSet_qry_usedb = stmt_qry_usedb.execute();    

                        
                        var stmt_apply_tag = snowflake.createStatement({sqlText:query_apply_tag});
                        var ResultSet_apply_tag = stmt_apply_tag.execute(); 

                        var qry_usedb_curr ="use "+ current_catalog +"."+ current_schema +""; 
                        //return qry_usedb_curr;
                        var stmt_qry_usedb_curr = snowflake.createStatement({sqlText:qry_usedb_curr});
                        var ResultSet_qry_usedb_curr = stmt_qry_usedb_curr.execute(); 
                        return 'Success';
                    }
                    catch (err)  
                    {
                        return "Failed: " + err;  
                    }
                $$;"""

                # Execute queries
                cur.execute(create_database_query)
                cur.execute(create_schema_query)
                cur.execute(create_tag_table_query)
                cur.execute(create_list_tag_proc_query)
                cur.execute(create_task_tag_objects_proc_query)
                cur.execute(create_tag_applytag)

                st.success("All  Tags Management SQL scripts executed successfully.")
            except Exception as e:
                st.error(f"SQL execution error: {e}")
            finally:
                conn.close()
    else:
        st.error("Database configuration is missing.")
    pass

def run_share_management():
    # Functionality for "Run Share Management table SQL"
    st.write('Run Share Management table SQL')
    db_config = load_from_json('app_config.json').get("snowflake", {})
    if db_config:
        # Connect to the database
        conn = connect_to_snowflake(db_config)
        if conn:
            try:
                cur = conn.cursor()

                # Table creation query
                create_shr_table_query = f"""
                create or replace TABLE {snowflake_config["database"]}.{snowflake_config["schema"]}.METADATA_SHAREMANAGEMENT (
                SHARE_MANAGEMENT_KEY NUMBER(38,0) autoincrement start 1 increment 1 order,
                SHARE_NAME VARCHAR(1000),
                OBJECT_TYPE VARCHAR(500),
                OBJECT_NAME VARCHAR(1000),
                IS_SHARE_ACTIVE BOOLEAN DEFAULT TRUE,
                CREATE_DATE DATE DEFAULT CURRENT_DATE(),
                DEACTIVATED_DATE DATE,
                LASTRUNSTATUS BOOLEAN,
                LASTRUNDATETIME TIMESTAMP_NTZ(9),
                LASTRUNERROR VARCHAR(4000)
                );
                """

                # Stored procedure for listing all shares and inserting
                create_list_shares_proc_query = """
                CREATE OR REPLACE PROCEDURE """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.SPU_SHAREMANAGEMENT()
                RETURNS VARCHAR(16777216)
                LANGUAGE JAVASCRIPT
                EXECUTE AS CALLER
                AS '  
                try
                {
                    var is_share_exists = true;
                    var mail_body ="";
                    var shares_query = "SELECT DISTINCT share_name,Is_Share_Active FROM """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement";
                    var shares_query_stmt = snowflake.createStatement({sqlText: shares_query});
                    var shares_result = shares_query_stmt.execute();
            
                    
                    mail_body =''The following shared objects do not exist<br><br>'';
                    mail_body +=''<table  border="1" cellspacing="0" cellpadding="5">'' ;       
                    
                    mail_body +=''<tr>'';
                    mail_body +=''<td>'';
                    mail_body +=''Share Name'';
                    mail_body +=''</td>'';
                    mail_body +=''<td>'';
                    mail_body +=''Object Name'';
                    mail_body +=''</td>'';
                    mail_body +=''<td>'';
                    mail_body +=''Object Type'';
                    mail_body +=''</td>'';
                    mail_body +=''</tr>'';
                    
                    while (shares_result.next()) 
                    {
                        
                        var share_name = shares_result.getColumnValue(1);
                        var share_is_share_active = shares_result.getColumnValue(2);
                
                        var share_exists_query = "show shares like ''" + share_name +"'';";
                        var share_exists_query_stmt = snowflake.createStatement({sqlText: share_exists_query});        
                        var share_exists_query_result = share_exists_query_stmt.execute();
                        
                        if(share_exists_query_result.next())
                        {   
                            is_share_exists = true;                                   
                        }
                        else
                        {
                            is_share_exists = false;
                        }    
                      
                        if(is_share_exists == false && share_is_share_active == false)
                        {
                            continue;
                        }
                        
                        if(is_share_exists == false && share_is_share_active == true)
                        {
                        
                            var create_share_query = "CREATE SHARE " + share_name +";";
                            var create_share_query_stmt = snowflake.createStatement({sqlText: create_share_query});
                            var create_share_query_result = create_share_query_stmt.execute(); 
                            
                        } 
                        
                        var share_objects_query = "SELECT share_management_key,object_type,object_name FROM """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement WHERE share_name = ?";           
                        share_objects_query += " order by object_type desc";
                        
                        var share_objects_stmt = snowflake.createStatement({sqlText: share_objects_query, binds: [share_name]});
                        var share_objects_result = share_objects_stmt.execute();
                        var iSSentMail = false;
                        while (share_objects_result.next()) 
                        {
                            try
                            {
                                var share_management_key = share_objects_result.getColumnValue(1);
                                var metadata_object_type = share_objects_result.getColumnValue(2);
                                var metadata_object_name = share_objects_result.getColumnValue(3);                
                                
                                var metadata_object_name_Arr = metadata_object_name.split(".")
                                
                                var matadata_dbname ="";
                                var matadata_schemaname ="";
                                var matadata_tablename ="";
                                var matadata_viewname ="";
                                
                    
                                var showkey = "";
                                var is_objects_exists_query="";
                                switch(metadata_object_type.toUpperCase()) 
                                {
                                    case ''DATABASE'':
                                        matadata_dbname = metadata_object_name_Arr[0];
                                        matadata_dbname = matadata_dbname.toUpperCase();                                                
                                        is_objects_exists_query = "select database_name from information_schema.databases where upper(database_name)=''"+ matadata_dbname +"''";
                                    break;
                                    case ''SCHEMA'':
                                         matadata_dbname = metadata_object_name_Arr[0];
                                         matadata_schemaname = metadata_object_name_Arr[1];
                                         matadata_dbname = matadata_dbname.toUpperCase();
                                         matadata_schemaname = matadata_schemaname.toUpperCase();                        
                                         
                                         is_objects_exists_query="select * from "+ matadata_dbname +".information_schema.schemata ";
                                         is_objects_exists_query +=" where upper(catalog_name)=''"+ matadata_dbname +"''";
                                         is_objects_exists_query += " and upper(schema_name) =''"+ matadata_schemaname +"''";
                                        break;
                                    case ''TABLE'':
                                         matadata_dbname = metadata_object_name_Arr[0];
                                         matadata_schemaname = metadata_object_name_Arr[1];
                                         matadata_tablename = metadata_object_name_Arr[2];  
                                         matadata_dbname = matadata_dbname.toUpperCase();
                                         matadata_schemaname = matadata_schemaname.toUpperCase();
                                         matadata_tablename = matadata_tablename.toUpperCase();
                                         
                                         is_objects_exists_query="select * from "+ matadata_dbname +".information_schema.tables ";
                                         is_objects_exists_query +=" where upper(table_catalog)=''"+ matadata_dbname +"''";
                                         is_objects_exists_query += " and upper(table_schema) =''"+ matadata_schemaname +"''";
                                         is_objects_exists_query += " and upper(table_type)=''BASE TABLE'' ";
                                         is_objects_exists_query += " and upper(table_name) =''"+ matadata_tablename +"''";
                                         
                                    break;
                                    case ''VIEW'':
                                         matadata_dbname = metadata_object_name_Arr[0];
                                         matadata_schemaname = metadata_object_name_Arr[1];
                                         matadata_viewname = metadata_object_name_Arr[2];
                                         matadata_dbname = matadata_dbname.toUpperCase();
                                         matadata_schemaname = matadata_schemaname.toUpperCase();
                                         matadata_viewname = matadata_viewname.toUpperCase();
                                         
                                         
                
                                         is_objects_exists_query="select * from "+ matadata_dbname +".information_schema.tables ";
                                         is_objects_exists_query +=" where upper(table_catalog)=''"+ matadata_dbname +"''";
                                         is_objects_exists_query +=" and upper(table_schema) =''"+ matadata_schemaname +"''";
                                         is_objects_exists_query +=" and upper(table_type)=''VIEW'' ";
                                         is_objects_exists_query +=" and upper(table_name) =''"+ matadata_viewname +"''";
                                    break;
                                }               
                                
                                var is_objects_exists = false;                
                                var is_objects_exists_stmt = snowflake.createStatement({sqlText: is_objects_exists_query});
                                var is_objects_exists_result = is_objects_exists_stmt.execute(); 
                                if(is_objects_exists_result.next())
                                {
                                    is_objects_exists= true;
                                }
                                else
                                {
                                    is_objects_exists= false;
                                }
                
                                
                                if(is_objects_exists)
                                {
                                    switch(metadata_object_type.toUpperCase()) 
                                    {
                                        case ''DATABASE'':
                                            qry_usedb ="use "+ matadata_dbname ;  
                                            if(is_share_exists == true && share_is_share_active == false)
                                            { 
                                                grant_query = "REVOKE USAGE ON DATABASE " + matadata_dbname + " FROM SHARE " + share_name+ " ";
                                            }
                                            else
                                            {
                                                var desc_share_qry ="desc share "+ share_name +"";
                                                var stmt_desc_share_qry = snowflake.createStatement({sqlText: desc_share_qry});
                                                var result_desc_share_qry = stmt_desc_share_qry.execute();
                                                
                                                while (result_desc_share_qry.next()) 
                                                {
                                                    var share_kind = result_desc_share_qry.getColumnValue(1);
                                                    var db_name = result_desc_share_qry.getColumnValue(2);

                                                    if(share_kind.toUpperCase() == "DATABASE" && db_name.toUpperCase() == matadata_dbname.toUpperCase())
                                                    {
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        grant_query = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name+ " ";    
                                                    }                                                                                                                                       
                                                }    
                                                
                                            }
                                            
                                        break;

                                        case ''SCHEMA'':
                                            qry_usedb ="use "+ matadata_dbname ; 
                                            if(is_share_exists == true && share_is_share_active == false)
                                            { 
                                                grant_query = "REVOKE USAGE ON SCHEMA "+ matadata_dbname +"." + matadata_schemaname + " FROM SHARE " + share_name+ " ";
                                            }
                                            else
                                            {
                                                var desc_share_qry ="desc share "+ share_name +"";
                                                var stmt_desc_share_qry = snowflake.createStatement({sqlText: desc_share_qry});
                                                var result_desc_share_qry = stmt_desc_share_qry.execute();
                                                
                                                while (result_desc_share_qry.next()) 
                                                {
                                                    var share_kind = result_desc_share_qry.getColumnValue(1);
                                                    var db_name = result_desc_share_qry.getColumnValue(2);

                                                    if(share_kind.toUpperCase() == "DATABASE" && db_name.toUpperCase() == matadata_dbname.toUpperCase())
                                                    {
                                                        
                                                    }
                                                    else
                                                    {
                                                        var grant_query_db_sc = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name + " ";
                                                        var grant_stmt_db_sc = snowflake.createStatement({sqlText: grant_query_db_sc});
                                                        var result_grant_stmt_db_sc = grant_stmt_db_sc.execute();  
                                                    } 

                                                    if(share_kind.toUpperCase() == "SCHEMA" && db_name.toUpperCase() == matadata_schemaname.toUpperCase())
                                                    {
                                                        
                                                    }
                                                    else
                                                    {
                                                        var grant_query_db_sc = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name + " ";
                                                        var grant_stmt_db_sc = snowflake.createStatement({sqlText: grant_query_db_sc});
                                                        var result_grant_stmt_db_sc = grant_stmt_db_sc.execute();  
                                                    } 
                                                }     
                                            }
                                        break;
                                        case ''TABLE'':
                                            qry_usedb ="use "+ matadata_dbname +"."+ matadata_schemaname +"";
                                            if(is_share_exists == true && share_is_share_active == false)
                                            { 
                                                grant_query = "REVOKE SELECT ON TABLE "+ matadata_dbname +"."+matadata_schemaname+"." + matadata_tablename + " ";
                                                grant_query += " FROM SHARE " + share_name ;
                                            }
                                            else
                                            {
                                                var desc_share_qry ="desc share "+ share_name +"";
                                                var stmt_desc_share_qry = snowflake.createStatement({sqlText: desc_share_qry});
                                                var result_desc_share_qry = stmt_desc_share_qry.execute();
                                                
                                                while (result_desc_share_qry.next()) 
                                                {
                                                    var share_kind = result_desc_share_qry.getColumnValue(1);
                                                    var db_name = result_desc_share_qry.getColumnValue(2);

                                                    if(share_kind.toUpperCase() == "DATABASE" && db_name.toUpperCase() == matadata_dbname.toUpperCase())
                                                    {
                                                        
                                                    }
                                                    else
                                                    {
                                                        var grant_query_db_sc = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name + " ";
                                                        var grant_stmt_db_sc = snowflake.createStatement({sqlText: grant_query_db_sc});
                                                        var result_grant_stmt_db_sc = grant_stmt_db_sc.execute();  
                                                    } 

                                                    if(share_kind.toUpperCase() == "SCHEMA" && db_name.toUpperCase() == matadata_schemaname.toUpperCase())
                                                    {
                                                        
                                                    }
                                                    else
                                                    {
                                                        var grant_query_db_sc = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name + " ";
                                                        var grant_stmt_db_sc = snowflake.createStatement({sqlText: grant_query_db_sc});
                                                        var result_grant_stmt_db_sc = grant_stmt_db_sc.execute();  
                                                    } 
                                                } 
                                                grant_query = "GRANT SELECT ON TABLE "+ matadata_dbname +"."+matadata_schemaname+"." + matadata_tablename + " ";
                                                grant_query += " TO SHARE " + share_name ;
                                            }
                                        break;
                                        case ''VIEW'':
                                            qry_usedb ="use "+ matadata_dbname +"."+ matadata_schemaname +"";
                                            if(is_share_exists == true && share_is_share_active == false)
                                            { 
                                                grant_query = "REVOKE SELECT ON VIEW "+ matadata_dbname +"."+matadata_schemaname+"." + matadata_viewname + " ";
                                                grant_query += " FROM SHARE " + share_name ;                        
                                            }
                                            else
                                            {
                                                
                                                var grant_query_db_vw = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name ;                                    
                                                var grant_stmt_db_vw = snowflake.createStatement({sqlText: grant_query_db_vw});
                                                var result_grant_stmt_db_vw = grant_stmt_db_vw.execute(); 
                                                                                   
                                                var grant_query_schema_vw = "GRANT USAGE ON SCHEMA " + matadata_dbname + "." + matadata_schemaname + " ";
                                                grant_query_schema_vw += " TO SHARE " + share_name ;
                                                var grant_stmt_schema_vw = snowflake.createStatement({sqlText: grant_query_schema_vw});
                                                var result_grant_stmt_schema_vw = grant_stmt_schema_vw.execute();                                 
                
                                                var grant_query_ref_db ="select distinct referenced_database_name ";
                                                grant_query_ref_db +=" from table(get_object_references(database_name=>''"+ matadata_dbname +"''";
                                                grant_query_ref_db +=", schema_name=>''"+ matadata_schemaname +"'', ";
                                                grant_query_ref_db +=" object_name=>''"+ matadata_viewname +"''));";                               
            
                                               
                                                var grant_stmt_ref_db = snowflake.createStatement({sqlText: grant_query_ref_db});
                                                var result_grant_stmt_ref_db = grant_stmt_ref_db.execute();
                                                
                                                while (result_grant_stmt_ref_db.next()) 
                                                {
                                                    var ref_db_name = result_grant_stmt_ref_db.getColumnValue(1);
                                                    var grant_query_ref_db1 = "GRANT REFERENCE_USAGE  ON DATABASE " + ref_db_name + " TO SHARE " + share_name ;
                                                    
                                                    var grant_stmt_ref_db1 = snowflake.createStatement({sqlText: grant_query_ref_db1});
                                                    var result_grant_stmt_ref_db1 = grant_stmt_ref_db1.execute();                                    
                                                }    
                                                
                                                grant_query = "GRANT SELECT ON VIEW "+ matadata_dbname +"."+matadata_schemaname+"." + matadata_viewname + " ";
                                                grant_query += " TO SHARE " + share_name ;                        
                                            }                            
                                        break;
                                    }        
                                    //return grant_query;
                                    if (grant_query) 
                                    {    
                                       
                                        var query_update_result="";
                                        var grant_stmt = snowflake.createStatement({sqlText: grant_query});
                                        var result = grant_stmt.execute();   
                
                                        query_update_result="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement set";              
                                        query_update_result+=" lastrunstatus=true,lastrundatetime=CURRENT_TIMESTAMP(),";
                                        query_update_result +=" lastrunerror='''' where share_management_key ="+share_management_key;   
                                        
                                        var stmt_result_update = snowflake.createStatement({sqlText:query_update_result});
                                        var result_update = stmt_result_update.execute();                        
                                        
                                      
                                    }
                                    
                                }
                                else
                                {
                                    iSSentMail = true;
                                    mail_body +=''<tr>''
                                    mail_body +=''<td>''
                                    mail_body +=share_name
                                    mail_body +=''</td>''
                                    mail_body +=''<td>''
                                    mail_body +=metadata_object_name
                                    mail_body +=''</td>''
                                    mail_body +=''<td>''
                                    mail_body +=metadata_object_type
                                    mail_body +=''</td>''
                                    mail_body +=''</tr>''
                                    
                                   
                                    //sent mail
                                }  
                            
                            }
                            catch(err)
                            {
                                
                                var errstr="";                           
                                errstr = err.message.replaceAll("''"," ");
                                errstr = errstr.replaceAll("''", "")
                                
                                var query_update_errresult ="";
                                
                                query_update_errresult ="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement set"; 
                                query_update_errresult +=" lastrunstatus=false,lastrundatetime=CURRENT_TIMESTAMP(),";
                                query_update_errresult +=" lastrunerror=''"+ errstr +"'' where share_management_key="+ share_management_key +" ";                    
                                var stmt_result_update = snowflake.createStatement({sqlText:query_update_errresult});
                                var result_update = stmt_result_update.execute(); 
                            }
                                                          
                        }
                       
                        
                    }   
                    
                    mail_body +=''</table>''
                    if(iSSentMail)
                    {
                        var EMAILS  ="parthipan@goleads.com";
                        var EMAILSUBJECT ="Snowflake - Shared object does not exist";        
                        var sqlquery = "call system$send_email(''my_email'',''"+EMAILS+"'',''"+EMAILSUBJECT+"'',''"+ mail_body+"'',''text/html'')";        
                        var sql_statement = snowflake.createStatement({sqlText:sqlquery});
                        sql_statement.execute();
                    }
                    
                    return "success";
                }
                catch (err)  
                {
            
                    var errstr="";
                    errstr = "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
                    errstr += "\\\\n  Message: " + err.message;
                    errstr += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;               
            
                    errstr = errstr.replaceAll("''", "")
                    
                    var query_update_errresult ="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement set lastrunstatus=false,lastrundatetime=CURRENT_TIMESTAMP(),";
                    query_update_errresult +=" lastrunerror=''"+errstr+"'' where share_management_key="+share_management_key;
                    var stmt_result_update = snowflake.createStatement({sqlText:query_update_errresult});
                    var result_update = stmt_result_update.execute();    
                    return "failure";        
                }
            ';"""

                # Stored procedure for checking task share objects availability
                create_task_share_objects_proc_query = """
                create or replace task """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.TK_SHAREMANAGEMENT
                    warehouse=COMPUTE_WH
                    schedule='USING CRON 0 22 * * * UTC'
                    as Call """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.spu_shareManagement();
                """

                # Execute queries
                cur.execute(create_shr_table_query)
                cur.execute(create_list_shares_proc_query)
                cur.execute(create_task_share_objects_proc_query)



                st.success("All Share Management SQL scripts executed successfully.")
            except Exception as e:
                st.error(f"SQL execution error: {e}")
            finally:
                conn.close()
    else:
        st.error("Database configuration is missing.")
    pass

def run_warehouse_management():
    # Functionality for "Run Warehouse Management table SQL"
    cdatabase_name = snowflake_config["database"]
    cschema_name = snowflake_config["schema"]
    # st.write(sdatabase)
    # st.write(schemas)
    st.write('Run Warehouse Management table SQL')
    db_config = load_from_json('app_config.json').get("snowflake", {})
    if db_config:
        # Connect to the database
        conn = connect_to_snowflake(db_config)
        if conn:
            try:
                cur = conn.cursor()

                # Table creation query
                create_wh_table_query = f"""
                CREATE OR REPLACE TABLE {snowflake_config["database"]}.{snowflake_config["schema"]}.Warehouse_Management
                (
                    UNIQUEID NUMBER(38,0) autoincrement start 1 increment 1 order,
                    WAREHOUSE_NAME VARCHAR(16777216),
                    FREQUENCY VARCHAR(16777216),
                    DATE DATE,
                    ACTIVATE_DAY VARCHAR(16777216),
                    ACTIVATE_TIME TIME(9),
                    STATUS VARCHAR(16777216),
                    SIZE VARCHAR(16777216),
                    ISACTIVE BOOLEAN
                )
                """

                # Stored procedure for listing all warehouses and inserting
                create_list_wh_proc_query = f"""
                CREATE OR REPLACE PROCEDURE {cdatabase_name}.{cschema_name}.SPU_WAREHOUSE_MANAGEMENT()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS $$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, current_date, current_time, left, replace
from datetime import datetime
import calendar

def check_warehouse_status(session, warehouse_name):
    query = f"SHOW WAREHOUSES LIKE '{{warehouse_name}}'"
    result = session.sql(query).collect()
    warehouse_state = result[0]['state']
    return warehouse_state

def get_day_of_week():
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    today = datetime.today()
    day_of_week_str = days_of_week[today.weekday()]
    return day_of_week_str

def main(session: snowpark.Session):
    currenttime = datetime.now().strftime('%H:%M')
    day_of_week = get_day_of_week()

    table = '{cschema_name}.warehouse_Management'
    Conditioncheck = (col("isactive") == 1)
    TimeCheck = (col("Activate_time") == currenttime)
    DayCheck = (col("ACTIVATE_DAY").isNull() | col("ACTIVATE_DAY").like(f'%{{day_of_week}}%') | (col("ACTIVATE_DAY") == '') | (col("ACTIVATE_DAY") == ' '))
    dataframe = session.table(table).filter(Conditioncheck & TimeCheck & DayCheck)

    Statusupdate = dataframe.filter((col("STATUS").isin("Suspend","Resume"))).collect()

    for row in Statusupdate:
        name = row["WAREHOUSE_NAME"]
        Status = row["STATUS"]
        warehouse_status = check_warehouse_status(session, name)
        warehouse_status = warehouse_status.replace('STARTED', 'Resume').replace('SUSPENDED', 'Suspend')

        if warehouse_status != Status:
            session.sql(f'ALTER WAREHOUSE {{name}} {{Status}};').collect()

    Sizeupdate = dataframe.filter(col("SIZE").isin("X-Small","Small","Medium","Large","X-Large","2X-Large","3X-Large","4X-Large")).collect()

    for row in Sizeupdate:
        name = row["WAREHOUSE_NAME"]
        size = row["SIZE"]
        session.sql(f"ALTER WAREHOUSE {{name}} SET WAREHOUSE_SIZE='{{size}}';").collect()

    return "Procedure Completed Successfully"
$$;;

                """
                create_task_warehouse_proc_query = """
                create or replace task """ + snowflake_config["schema"] + """.TK_WAREMANAGEMENT
                    warehouse=COMPUTE_WH
                    schedule='USING CRON 0 22 * * * UTC'
                    as Call """ + snowflake_config["schema"] + """.SPU_WAREHOUSE_MANAGEMENT();
                """

                create_task_warehouse_alter ="""Alter task """ + snowflake_config["schema"] + """.TK_WAREMANAGEMENT resume;"""
                # Execute queries
                cur.execute(create_wh_table_query)
                cur.execute(create_list_wh_proc_query)
                cur.execute(create_task_warehouse_proc_query)
                cur.execute(create_task_warehouse_alter)


                st.success("All Warehouse Management SQL scripts executed successfully.")
            except Exception as e:
                st.error(f"SQL execution error: {e}")
            finally:
                conn.close()
    else:
        st.error("Database configuration is missing.")

    pass

# Main function to run when the combined button is clicked
def run_all():
    run_tags_management()
    run_share_management()
    run_warehouse_management()

# UI
if st.button("Run All Management SQL Scripts"):
    run_all()


#st.write('Tag Management Rules script')
with st.expander("**Tag Management Rules script**"):
    #st.markdown("""<p style='color:#29B5E8 ;font-size:20px; font-weight:bold;margin-bottom:0px;'><i class="fas fa-info-circle"></i> Tag Management Rules script</p>""",unsafe_allow_html=True)

    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is for create METADATA table for TAG MANAGEMENT</h4>""",unsafe_allow_html=True)

    create_tag_table_query = f"""
    create or replace TABLE {snowflake_config["database"]}.{snowflake_config["schema"]}.METADATA_TAGMANAGEMENT (
    UNIQUEID NUMBER(38,0) autoincrement start 1 increment 1 order,
    DBNAME VARCHAR(30),
    SCHEMANAME VARCHAR(20),
    OBJECTTYPE VARCHAR(10),
    NAMEPATTERN VARCHAR(20),
    PATTERNVALUE VARCHAR(50),
    TAGNAME VARCHAR(20),
    TAGVALUE VARCHAR(30),
    CREATEDDATE TIMESTAMP_LTZ(9),
    ISACTIVE BOOLEAN,
    LASTRUNDATETIME TIMESTAMP_NTZ(9),
    LASTRUNSTATUS BOOLEAN,
    LASTRUNERROR VARCHAR(8000)
    )"""
    st.code(create_tag_table_query, language='sql')

    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is to create PROCEDURE for TAG MANAGEMENT to apply tag schedule</h4>""",unsafe_allow_html=True)
    create_list_tag_proc_query = """
    CREATE OR REPLACE PROCEDURE """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.SPU_TAG_MANAGEMENT_APPLYTAG_SCHEDULE()
    RETURNS VARCHAR(16777216)
    LANGUAGE JAVASCRIPT
    execute as caller
    AS '
                try
                {       
                    var Query_Get_Metadata="",ResultSet_MetaData="",uniqueid="",dbname="",schemaname="",objecttype="",namepattern="",patternvalue="",tagname="";    
                    var tagvalue="",rulestatus="",isexecuted="";    
                    var Query_SchemaInfo="",stmt_SchemaInfo="",ResultSet_SchemaInfo="";
                    var stmt_metadata="",ResultSet_MetaData="";
                    
                    Query_Get_Metadata +=" select uniqueid,dbname,schemaname,objecttype,namepattern,patternvalue,tagname,tagvalue, ";
                    Query_Get_Metadata +=" ifnull(isactive,false) as rulestatus ";
                    Query_Get_Metadata +=" from """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_TagManagement";    
                     
                    stmt_metadata = snowflake.createStatement({sqlText:Query_Get_Metadata});
                    ResultSet_MetaData = stmt_metadata.execute(); 
                 
                    if(!ResultSet_MetaData.next())
                    {
                        return "No records found in metadata table to do tag action";
                    } 
                    var querytext="";
                    for (var i = 1; i <= stmt_metadata.getRowCount(); i++) 
                    {
                        uniqueid = ResultSet_MetaData.getColumnValue(1);                       
                        dbname = ResultSet_MetaData.getColumnValue(2);                       
                        schemaname = ResultSet_MetaData.getColumnValue(3);                       
                        objecttype = ResultSet_MetaData.getColumnValue(4);                       
                        namepattern = ResultSet_MetaData.getColumnValue(5);                       
                        patternvalue = ResultSet_MetaData.getColumnValue(6);                       
                        tagname = ResultSet_MetaData.getColumnValue(7);                       
                        tagvalue = ResultSet_MetaData.getColumnValue(8);     
                        rulestatus = ResultSet_MetaData.getColumnValue(9);                               
                
                        var tablename_schema ="",columnname_schema="",fun_columnname="";
                    
                        if(objecttype.toUpperCase() == "TABLE")
                        {
                            tablename_schema ="information_schema.tables";
                            columnname_schema="table_catalog,table_schema,table_name";
                            fun_columnname="Table_name";
                            objecttype = "BASE TABLE";           
                        }
                        else if (objecttype.toUpperCase() == "VIEW")
                        {
                            fun_columnname="Table_name";
                            tablename_schema ="information_schema.tables";
                            columnname_schema="table_catalog,table_schema,table_name";
                            objecttype = "VIEW";
                        }
                        else if (objecttype.toUpperCase() == "COLUMN")
                        {
                            fun_columnname="column_name";
                            columnname_schema="table_catalog,table_schema,table_name,column_name";
                            tablename_schema ="information_schema.columns";
                            objecttype = "COLUMN";
                        }                
                        
                        namepattern = namepattern.toUpperCase();
                        patternvalue = patternvalue.toUpperCase();
                        
                        var sWhereclause="";
                        
                        if(namepattern == "STARTS WITH")
                        {
                            sWhereclause =" STARTSWITH("+ fun_columnname +", ''"+ patternvalue +"'')";            
                        }
                        else if (namepattern == "ENDS WITH")
                        {
                            sWhereclause =" ENDSWITH("+ fun_columnname +", ''"+ patternvalue +"'')";
                        }
                        else if (namepattern == "CONTAINS")
                        {
                            sWhereclause =" CONTAINS("+ fun_columnname +", ''"+ patternvalue +"'')";            
                        }
                        else if (namepattern == "CREATED BY USER")
                        {
                            sWhereclause =" UPPER(LAST_DDL_BY) =''"+ patternvalue +"''";                        
                        }
                        else if (namepattern == "HAS ROLE")
                        {
                            sWhereclause =" UPPER(table_owner) =''"+ patternvalue +"''";                        
                        }
                        else
                        {
                            Query_SchemaInfo="";
                        }           
                           
                
                        Query_SchemaInfo = "select "+ columnname_schema +" from "+ dbname +"."+ tablename_schema +" where "+sWhereclause;        
                        Query_SchemaInfo += " and UPPER(table_schema)=''"+ schemaname +"'' ";
                        Query_SchemaInfo += " and UPPER(table_catalog)=''"+ dbname +"'' " ; 
                        if(objecttype.toUpperCase() == "TABLE" || objecttype.toUpperCase() == "VIEW")
                        {
                            Query_SchemaInfo +=" and UPPER(table_type)=''"+ objecttype +"'' ";
                        }       
                       
                        var stmt_tag = snowflake.createStatement({sqlText:Query_SchemaInfo});
                        var ResultSet_tag = stmt_tag.execute();      
                        ResultSet_tag.next();
                        
                        for (var j = 1; j <= stmt_tag.getRowCount(); j++) 
                        {
                            var table_catalog = ResultSet_tag.getColumnValue(1);  
                            var table_schema = ResultSet_tag.getColumnValue(2);  
                            var table_name = ResultSet_tag.getColumnValue(3);
                            var query_apply_tag="";
                            
                            if(objecttype.toUpperCase() == "COLUMN")
                            {
                                var column_name = ResultSet_tag.getColumnValue(4); 
                                if(rulestatus == true)
                                {
                                    query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" ";
                                    query_apply_tag +=" modify column "+ column_name +" SET TAG "+tagname +" = ''"+ tagvalue +"''";
                                }
                                else
                                {
                                    query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" ";
                                    query_apply_tag +="modify column "+ column_name +" UNSET TAG "+tagname +" ";
                                    
                                }                  
                            }
                            else
                            {
                                if(rulestatus == true)
                                {
                                     query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" SET TAG "+tagname +" = ''"+ tagvalue +"''";
                                }
                                else
                                {
                                    query_apply_tag ="ALTER TABLE IF EXISTS "+table_catalog+"."+table_schema+"."+table_name+" UNSET TAG "+tagname +" ";
                                }            
                            }   
                            //return query_apply_tag;

                            var qry_get_curr_db ="SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();";
                            var stmt_get_curr_db = snowflake.createStatement({sqlText:qry_get_curr_db});
                            var ResultSet_get_curr_db = stmt_get_curr_db.execute();      
                            ResultSet_get_curr_db.next();

                            var current_catalog = ResultSet_tag.getColumnValue(1);  
                            var current_schema = ResultSet_tag.getColumnValue(2);                              

                            var qry_usedb ="use "+ table_catalog +"."+ table_schema +"";  
                            
                            var stmt_qry_usedb = snowflake.createStatement({sqlText:qry_usedb});
                            var ResultSet_qry_usedb = stmt_qry_usedb.execute();             

                             
                            var stmt_apply_tag = snowflake.createStatement({sqlText:query_apply_tag});                           
                            var ResultSet_apply_tag = stmt_apply_tag.execute();                                        
                            ResultSet_tag.next();  

                            var qry_usedb_curr ="use "+ current_catalog +"."+ current_schema +""; 
                            //return qry_usedb_curr;
                            var stmt_qry_usedb_curr = snowflake.createStatement({sqlText:qry_usedb_curr});
                            var ResultSet_qry_usedb_curr = stmt_qry_usedb_curr.execute();  
                            
                        }
                
                        var query_update_result ="";
                        query_update_result="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_TagManagement set lastrunstatus=true,lastrundatetime=CURRENT_TIMESTAMP(),";
                        //return query_update_result;
                        query_update_result +=" lastrunerror='''' where uniqueid ="+uniqueid;        
                        var stmt_result_update = snowflake.createStatement({sqlText:query_update_result});
                        var result_update = stmt_result_update.execute();
                        
                        ResultSet_MetaData.next();  
                    }
                    
                    return "success";
                    
                     
                }    
                catch(err)
                {
                    var errstr;
                    errstr = "Failed: Code: " + err.code + "  State: " + err.state;
                    errstr += "  Message: " + err.message;
                    errstr += " Stack Trace:" + err.stackTraceTxt;
                    errstr = errstr.replaceAll("''"," ");
                
                    var query_update_result ="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_TagManagement set"; 
                    query_update_result +=" lastrunstatus=false,lastrundatetime=CURRENT_TIMESTAMP(),";
                    query_update_result +=" lastrunerror=''"+errstr+"'' where uniqueid="+uniqueid;
                
                    //return query_update_result;
                    var stmt_result_update = snowflake.createStatement({sqlText:query_update_result});
                    var result_update = stmt_result_update.execute();    
                    return "failure";
                }
                ';"""

    st.code(create_list_tag_proc_query, language='sql')

    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is to create PROCEDURE for TAG MANAGEMENT to apply tag</h4>""",unsafe_allow_html=True)

    create_tag_applytag = """
                    CREATE OR REPLACE PROCEDURE """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.TAG_Management_ApplyTag(DBNAME VARCHAR,SCHEMANAME VARCHAR,OBJECTNAME VARCHAR,TAGNAME VARCHAR,TAGVALUE VARCHAR,OBJECTTYPE VARCHAR,COLUMNNAME VARCHAR)
                    RETURNS STRING
                    LANGUAGE JAVASCRIPT
                    EXECUTE AS CALLER
                    AS
                    $$
                        try 
                        {
                            var local_dbname = DBNAME;  
                            var local_schemaname = SCHEMANAME;  
                            var local_objectname = OBJECTNAME;  
                            var local_tagname = TAGNAME;  
                            var local_tagvalue = TAGVALUE;  
                            var local_objecttype = OBJECTTYPE;  
                            var local_columnname = COLUMNNAME; 

                            if(local_objecttype.toUpperCase() != "TABLE" && local_objecttype.toUpperCase() != "VIEW" && local_objecttype.toUpperCase() != "COLUMN")
                            {
                                return 'Object type parameter value should be table or view or column. Please check the object type parameter value';
                            }
                            
                            if(local_objecttype.toUpperCase() == "COLUMN")
                            {
                                if(local_dbname == "" || local_schemaname =="" || local_objectname =="" || local_tagname == "" || local_tagvalue == "" || local_columnname == 
                                "")
                                {
                                    return 'Procedure parameters value should not be empty. Please check all parameters values';
                                }
                            }
                            else
                            {
                                if(local_dbname == "" || local_schemaname =="" || local_objectname =="" || local_tagname == "" || local_tagvalue == "")
                                {
                                    return 'Procedure parameters value should not be empty. Please check all parameters values';
                                }
                            }

                            if(local_objecttype.toUpperCase() == "COLUMN")
                            {            
                                query_apply_tag ="ALTER TABLE IF EXISTS "+local_dbname+"."+local_schemaname+"."+local_objectname+" ";
                                query_apply_tag +=" modify column "+ local_columnname +" SET TAG "+local_tagname +" = '"+ local_tagvalue +"'";            
                            }
                            else
                            {           
                                query_apply_tag ="ALTER "+ local_objecttype +" IF EXISTS "+local_dbname+"."+local_schemaname+"."+local_objectname+" ";
                                query_apply_tag +=" SET TAG "+local_tagname +" = '"+ local_tagvalue  +"'";           
                            }
                            //return query_apply_tag;
                            var qry_get_curr_db ="SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();";
                            var stmt_get_curr_db = snowflake.createStatement({sqlText:qry_get_curr_db});
                            var ResultSet_get_curr_db = stmt_get_curr_db.execute();      
                            ResultSet_get_curr_db.next();

                            var current_catalog = ResultSet_get_curr_db .getColumnValue(1);  
                            var current_schema = ResultSet_get_curr_db .getColumnValue(2);                              

                            var qry_usedb ="use "+ local_dbname +"."+ local_schemaname +"";  
                            
                            var stmt_qry_usedb = snowflake.createStatement({sqlText:qry_usedb});
                            var ResultSet_qry_usedb = stmt_qry_usedb.execute();    

                            
                            var stmt_apply_tag = snowflake.createStatement({sqlText:query_apply_tag});
                            var ResultSet_apply_tag = stmt_apply_tag.execute(); 

                            var qry_usedb_curr ="use "+ current_catalog +"."+ current_schema +""; 
                            //return qry_usedb_curr;
                            var stmt_qry_usedb_curr = snowflake.createStatement({sqlText:qry_usedb_curr});
                            var ResultSet_qry_usedb_curr = stmt_qry_usedb_curr.execute(); 
                            return 'Success';
                        }
                        catch (err)  
                        {
                            return "Failed: " + err;  
                        }
                    $$;"""



    st.code(create_tag_applytag, language='sql')

    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is to create TASK for TAG MANAGEMENT to apply tag schedule</h4>""",unsafe_allow_html=True)
    create_task_tag_objects_proc_query = """
                     create or replace task """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.TK_TAGMANAGEMENT
                        warehouse=COMPUTE_WH
                        schedule='USING CRON 0 22 * * * UTC'
                        as Call """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.SPU_TAG_MANAGEMENT_APPLYTAG_SCHEDULE();
                """
    st.code(create_task_tag_objects_proc_query, language='sql')            

# ----------------------------------- share management tab-----------------------------------------------#
with st.expander("**Share Management Rules script**"):
#st.write('Share Management Rules script')
#st.markdown("""<p style='color:#29B5E8 ;font-size:20px; font-weight:bold;margin-bottom:0px;'><i class="fas fa-info-circle"></i> Share Management Rules script</p>""",unsafe_allow_html=True)
    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is to create METADATA for SHARE MANAGEMENT</h4>""",unsafe_allow_html=True)

    create_shr_table_query = f"""
    create or replace TABLE {snowflake_config["database"]}.{snowflake_config["schema"]}.METADATA_SHAREMANAGEMENT (
    SHARE_MANAGEMENT_KEY NUMBER(38,0) autoincrement start 1 increment 1 order,
    SHARE_NAME VARCHAR(1000),
    OBJECT_TYPE VARCHAR(500),
    OBJECT_NAME VARCHAR(1000),
    IS_SHARE_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATE_DATE DATE DEFAULT CURRENT_DATE(),
    DEACTIVATED_DATE DATE,
    LASTRUNSTATUS BOOLEAN,
    LASTRUNDATETIME TIMESTAMP_NTZ(9),
    LASTRUNERROR VARCHAR(4000)
    );
    """

    st.code(create_shr_table_query, language='sql')

    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is to create PROCEDURE for SHARE MANAGEMENT</h4>""",unsafe_allow_html=True)

    create_list_shares_proc_query = """
    CREATE OR REPLACE PROCEDURE """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.SPU_SHAREMANAGEMENT()
    RETURNS VARCHAR(16777216)
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS '  
    try
    {
        var is_share_exists = true;
        var mail_body ="";
        var shares_query = "SELECT DISTINCT share_name,Is_Share_Active FROM """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement";
        var shares_query_stmt = snowflake.createStatement({sqlText: shares_query});
        var shares_result = shares_query_stmt.execute();

        
        mail_body =''The following shared objects do not exist<br><br>'';
        mail_body +=''<table  border="1" cellspacing="0" cellpadding="5">'' ;       
        
        mail_body +=''<tr>'';
        mail_body +=''<td>'';
        mail_body +=''Share Name'';
        mail_body +=''</td>'';
        mail_body +=''<td>'';
        mail_body +=''Object Name'';
        mail_body +=''</td>'';
        mail_body +=''<td>'';
        mail_body +=''Object Type'';
        mail_body +=''</td>'';
        mail_body +=''</tr>'';
        
        while (shares_result.next()) 
        {
            
            var share_name = shares_result.getColumnValue(1);
            var share_is_share_active = shares_result.getColumnValue(2);

            var share_exists_query = "show shares like ''" + share_name +"'';";
            var share_exists_query_stmt = snowflake.createStatement({sqlText: share_exists_query});        
            var share_exists_query_result = share_exists_query_stmt.execute();
            
            if(share_exists_query_result.next())
            {   
                is_share_exists = true;                                   
            }
            else
            {
                is_share_exists = false;
            }    
            
            if(is_share_exists == false && share_is_share_active == false)
            {
                continue;
            }
            
            if(is_share_exists == false && share_is_share_active == true)
            {
            
                var create_share_query = "CREATE SHARE " + share_name +";";
                var create_share_query_stmt = snowflake.createStatement({sqlText: create_share_query});
                var create_share_query_result = create_share_query_stmt.execute(); 
                
            } 
            
            var share_objects_query = "SELECT share_management_key,object_type,object_name FROM """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement WHERE share_name = ?";           
            share_objects_query += " order by object_type desc";
            
            var share_objects_stmt = snowflake.createStatement({sqlText: share_objects_query, binds: [share_name]});
            var share_objects_result = share_objects_stmt.execute();
            var iSSentMail = false;
            while (share_objects_result.next()) 
            {
                try
                {
                    var share_management_key = share_objects_result.getColumnValue(1);
                    var metadata_object_type = share_objects_result.getColumnValue(2);
                    var metadata_object_name = share_objects_result.getColumnValue(3);                
                    
                    var metadata_object_name_Arr = metadata_object_name.split(".")
                    
                    var matadata_dbname ="";
                    var matadata_schemaname ="";
                    var matadata_tablename ="";
                    var matadata_viewname ="";
                    
        
                    var showkey = "";
                    var is_objects_exists_query="";
                    switch(metadata_object_type.toUpperCase()) 
                    {
                        case ''DATABASE'':
                            matadata_dbname = metadata_object_name_Arr[0];
                            matadata_dbname = matadata_dbname.toUpperCase();                                                
                            is_objects_exists_query = "select database_name from information_schema.databases where upper(database_name)=''"+ matadata_dbname +"''";
                        break;
                        case ''SCHEMA'':
                                matadata_dbname = metadata_object_name_Arr[0];
                                matadata_schemaname = metadata_object_name_Arr[1];
                                matadata_dbname = matadata_dbname.toUpperCase();
                                matadata_schemaname = matadata_schemaname.toUpperCase();                        
                                
                                is_objects_exists_query="select * from "+ matadata_dbname +".information_schema.schemata ";
                                is_objects_exists_query +=" where upper(catalog_name)=''"+ matadata_dbname +"''";
                                is_objects_exists_query += " and upper(schema_name) =''"+ matadata_schemaname +"''";
                            break;
                        case ''TABLE'':
                                matadata_dbname = metadata_object_name_Arr[0];
                                matadata_schemaname = metadata_object_name_Arr[1];
                                matadata_tablename = metadata_object_name_Arr[2];  
                                matadata_dbname = matadata_dbname.toUpperCase();
                                matadata_schemaname = matadata_schemaname.toUpperCase();
                                matadata_tablename = matadata_tablename.toUpperCase();
                                
                                is_objects_exists_query="select * from "+ matadata_dbname +".information_schema.tables ";
                                is_objects_exists_query +=" where upper(table_catalog)=''"+ matadata_dbname +"''";
                                is_objects_exists_query += " and upper(table_schema) =''"+ matadata_schemaname +"''";
                                is_objects_exists_query += " and upper(table_type)=''BASE TABLE'' ";
                                is_objects_exists_query += " and upper(table_name) =''"+ matadata_tablename +"''";
                                
                        break;
                        case ''VIEW'':
                                matadata_dbname = metadata_object_name_Arr[0];
                                matadata_schemaname = metadata_object_name_Arr[1];
                                matadata_viewname = metadata_object_name_Arr[2];
                                matadata_dbname = matadata_dbname.toUpperCase();
                                matadata_schemaname = matadata_schemaname.toUpperCase();
                                matadata_viewname = matadata_viewname.toUpperCase();
                                
                                

                                is_objects_exists_query="select * from "+ matadata_dbname +".information_schema.tables ";
                                is_objects_exists_query +=" where upper(table_catalog)=''"+ matadata_dbname +"''";
                                is_objects_exists_query +=" and upper(table_schema) =''"+ matadata_schemaname +"''";
                                is_objects_exists_query +=" and upper(table_type)=''VIEW'' ";
                                is_objects_exists_query +=" and upper(table_name) =''"+ matadata_viewname +"''";
                        break;
                    }               
                    
                    var is_objects_exists = false;                
                    var is_objects_exists_stmt = snowflake.createStatement({sqlText: is_objects_exists_query});
                    var is_objects_exists_result = is_objects_exists_stmt.execute(); 
                    if(is_objects_exists_result.next())
                    {
                        is_objects_exists= true;
                    }
                    else
                    {
                        is_objects_exists= false;
                    }

                    
                    if(is_objects_exists)
                    {
                        switch(metadata_object_type.toUpperCase()) 
                        {
                            case ''DATABASE'':
                                            qry_usedb ="use "+ matadata_dbname ;  
                                            if(is_share_exists == true && share_is_share_active == false)
                                            { 
                                                grant_query = "REVOKE USAGE ON DATABASE " + matadata_dbname + " FROM SHARE " + share_name+ " ";
                                            }
                                            else
                                            {
                                                var desc_share_qry ="desc share "+ share_name +"";
                                                var stmt_desc_share_qry = snowflake.createStatement({sqlText: desc_share_qry});
                                                var result_desc_share_qry = stmt_desc_share_qry.execute();
                                                
                                                while (result_desc_share_qry.next()) 
                                                {
                                                    var share_kind = result_desc_share_qry.getColumnValue(1);
                                                    var db_name = result_desc_share_qry.getColumnValue(2);

                                                    if(share_kind.toUpperCase() == "DATABASE" && db_name.toUpperCase() == matadata_dbname.toUpperCase())
                                                    {
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        grant_query = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name+ " ";    
                                                    }                                                                                                                                       
                                                }    
                                                
                                            }
                                            
                                        break;

case ''SCHEMA'':
                                            qry_usedb ="use "+ matadata_dbname ; 
                                            if(is_share_exists == true && share_is_share_active == false)
                                            { 
                                                grant_query = "REVOKE USAGE ON SCHEMA "+ matadata_dbname +"." + matadata_schemaname + " FROM SHARE " + share_name+ " ";
                                            }
                                            else
                                            {
                                                var desc_share_qry ="desc share "+ share_name +"";
                                                var stmt_desc_share_qry = snowflake.createStatement({sqlText: desc_share_qry});
                                                var result_desc_share_qry = stmt_desc_share_qry.execute();
                                                
                                                while (result_desc_share_qry.next()) 
                                                {
                                                    var share_kind = result_desc_share_qry.getColumnValue(1);
                                                    var db_name = result_desc_share_qry.getColumnValue(2);

                                                    if(share_kind.toUpperCase() == "DATABASE" && db_name.toUpperCase() == matadata_dbname.toUpperCase())
                                                    {
                                                        
                                                    }
                                                    else
                                                    {
                                                        var grant_query_db_sc = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name + " ";
                                                        var grant_stmt_db_sc = snowflake.createStatement({sqlText: grant_query_db_sc});
                                                        var result_grant_stmt_db_sc = grant_stmt_db_sc.execute();  
                                                    } 

                                                    if(share_kind.toUpperCase() == "SCHEMA" && db_name.toUpperCase() == matadata_schemaname.toUpperCase())
                                                    {
                                                        
                                                    }
                                                    else
                                                    {
                                                        var grant_query_db_sc = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name + " ";
                                                        var grant_stmt_db_sc = snowflake.createStatement({sqlText: grant_query_db_sc});
                                                        var result_grant_stmt_db_sc = grant_stmt_db_sc.execute();  
                                                    } 
                                                }     
                                            }
                                        break;
case ''TABLE'':
                                            qry_usedb ="use "+ matadata_dbname +"."+ matadata_schemaname +"";
                                            if(is_share_exists == true && share_is_share_active == false)
                                            { 
                                                grant_query = "REVOKE SELECT ON TABLE "+ matadata_dbname +"."+matadata_schemaname+"." + matadata_tablename + " ";
                                                grant_query += " FROM SHARE " + share_name ;
                                            }
                                            else
                                            {
                                                var desc_share_qry ="desc share "+ share_name +"";
                                                var stmt_desc_share_qry = snowflake.createStatement({sqlText: desc_share_qry});
                                                var result_desc_share_qry = stmt_desc_share_qry.execute();
                                                
                                                while (result_desc_share_qry.next()) 
                                                {
                                                    var share_kind = result_desc_share_qry.getColumnValue(1);
                                                    var db_name = result_desc_share_qry.getColumnValue(2);

                                                    if(share_kind.toUpperCase() == "DATABASE" && db_name.toUpperCase() == matadata_dbname.toUpperCase())
                                                    {
                                                        
                                                    }
                                                    else
                                                    {
                                                        var grant_query_db_sc = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name + " ";
                                                        var grant_stmt_db_sc = snowflake.createStatement({sqlText: grant_query_db_sc});
                                                        var result_grant_stmt_db_sc = grant_stmt_db_sc.execute();  
                                                    } 

                                                    if(share_kind.toUpperCase() == "SCHEMA" && db_name.toUpperCase() == matadata_schemaname.toUpperCase())
                                                    {
                                                        
                                                    }
                                                    else
                                                    {
                                                        var grant_query_db_sc = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name + " ";
                                                        var grant_stmt_db_sc = snowflake.createStatement({sqlText: grant_query_db_sc});
                                                        var result_grant_stmt_db_sc = grant_stmt_db_sc.execute();  
                                                    } 
                                                } 
                                                grant_query = "GRANT SELECT ON TABLE "+ matadata_dbname +"."+matadata_schemaname+"." + matadata_tablename + " ";
                                                grant_query += " TO SHARE " + share_name ;
                                            }
                                        break;
case ''VIEW'':
                                            qry_usedb ="use "+ matadata_dbname +"."+ matadata_schemaname +"";
                                            if(is_share_exists == true && share_is_share_active == false)
                                            { 
                                                grant_query = "REVOKE SELECT ON VIEW "+ matadata_dbname +"."+matadata_schemaname+"." + matadata_viewname + " ";
                                                grant_query += " FROM SHARE " + share_name ;                        
                                            }
                                            else
                                            {
                                                
                                                var grant_query_db_vw = "GRANT USAGE ON DATABASE " + matadata_dbname + " TO SHARE " + share_name ;                                    
                                                var grant_stmt_db_vw = snowflake.createStatement({sqlText: grant_query_db_vw});
                                                var result_grant_stmt_db_vw = grant_stmt_db_vw.execute(); 
                                                                                   
                                                var grant_query_schema_vw = "GRANT USAGE ON SCHEMA " + matadata_dbname + "." + matadata_schemaname + " ";
                                                grant_query_schema_vw += " TO SHARE " + share_name ;
                                                var grant_stmt_schema_vw = snowflake.createStatement({sqlText: grant_query_schema_vw});
                                                var result_grant_stmt_schema_vw = grant_stmt_schema_vw.execute();                                 
                
                                                var grant_query_ref_db ="select distinct referenced_database_name ";
                                                grant_query_ref_db +=" from table(get_object_references(database_name=>''"+ matadata_dbname +"''";
                                                grant_query_ref_db +=", schema_name=>''"+ matadata_schemaname +"'', ";
                                                grant_query_ref_db +=" object_name=>''"+ matadata_viewname +"''));";                               
            
                                               
                                                var grant_stmt_ref_db = snowflake.createStatement({sqlText: grant_query_ref_db});
                                                var result_grant_stmt_ref_db = grant_stmt_ref_db.execute();
                                                
                                                while (result_grant_stmt_ref_db.next()) 
                                                {
                                                    var ref_db_name = result_grant_stmt_ref_db.getColumnValue(1);
                                                    var grant_query_ref_db1 = "GRANT REFERENCE_USAGE  ON DATABASE " + ref_db_name + " TO SHARE " + share_name ;
                                                    
                                                    var grant_stmt_ref_db1 = snowflake.createStatement({sqlText: grant_query_ref_db1});
                                                    var result_grant_stmt_ref_db1 = grant_stmt_ref_db1.execute();                                    
                                                }    
                                                
                                                grant_query = "GRANT SELECT ON VIEW "+ matadata_dbname +"."+matadata_schemaname+"." + matadata_viewname + " ";
                                                grant_query += " TO SHARE " + share_name ;                        
                                            }                            
                                        break;
                        }        
                        //return grant_query;
                        if (grant_query) 
                        {    
                            
                            var query_update_result="";
                            var grant_stmt = snowflake.createStatement({sqlText: grant_query});
                            var result = grant_stmt.execute();   

                            query_update_result="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement set";              
                            query_update_result+=" lastrunstatus=true,lastrundatetime=CURRENT_TIMESTAMP(),";
                            query_update_result +=" lastrunerror='''' where share_management_key ="+share_management_key;   
                            
                            var stmt_result_update = snowflake.createStatement({sqlText:query_update_result});
                            var result_update = stmt_result_update.execute();                        
                            
                            
                        }
                        
                    }
                    else
                    {
                        iSSentMail = true;
                        mail_body +=''<tr>''
                        mail_body +=''<td>''
                        mail_body +=share_name
                        mail_body +=''</td>''
                        mail_body +=''<td>''
                        mail_body +=metadata_object_name
                        mail_body +=''</td>''
                        mail_body +=''<td>''
                        mail_body +=metadata_object_type
                        mail_body +=''</td>''
                        mail_body +=''</tr>''
                        
                        
                        //sent mail
                    }  
                
                }
                catch(err)
                {
                    
                    var errstr="";                           
                    errstr = err.message.replaceAll("''"," ");
                    errstr = errstr.replaceAll("''", "")
                    
                    var query_update_errresult ="";
                    
                    query_update_errresult ="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement set"; 
                    query_update_errresult +=" lastrunstatus=false,lastrundatetime=CURRENT_TIMESTAMP(),";
                    query_update_errresult +=" lastrunerror=''"+ errstr +"'' where share_management_key="+ share_management_key +" ";                    
                    var stmt_result_update = snowflake.createStatement({sqlText:query_update_errresult});
                    var result_update = stmt_result_update.execute(); 
                }
                                                
            }
            
            
        }   
        
        mail_body +=''</table>''
        if(iSSentMail)
        {
            var EMAILS  ="parthipan@goleads.com";
            var EMAILSUBJECT ="Snowflake - Shared object does not exist";        
            var sqlquery = "call system$send_email(''my_email'',''"+EMAILS+"'',''"+EMAILSUBJECT+"'',''"+ mail_body+"'',''text/html'')";        
            var sql_statement = snowflake.createStatement({sqlText:sqlquery});
            sql_statement.execute();
        }
        
        return "success";
    }
    catch (err)  
    {

        var errstr="";
        errstr = "Failed: Code: " + err.code + "\\\\n  State: " + err.state;
        errstr += "\\\\n  Message: " + err.message;
        errstr += "\\\\nStack Trace:\\\\n" + err.stackTraceTxt;               

        errstr = errstr.replaceAll("''", "")
        
        var query_update_errresult ="update """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.Metadata_ShareManagement set lastrunstatus=false,lastrundatetime=CURRENT_TIMESTAMP(),";
        query_update_errresult +=" lastrunerror=''"+errstr+"'' where share_management_key="+share_management_key;
        var stmt_result_update = snowflake.createStatement({sqlText:query_update_errresult});
        var result_update = stmt_result_update.execute();    
        return "failure";        
    }
    ';"""
    st.code(create_list_shares_proc_query, language='sql')

    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is to create SCHEDULE for SHARE MANAGEMENT</h4>""",unsafe_allow_html=True)

    create_task_share_objects_proc_query = """
                    create or replace task """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.TK_SHAREMANAGEMENT
                        warehouse=COMPUTE_WH
                        schedule='USING CRON 0 22 * * * UTC'
                        as Call """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.spu_shareManagement();
                    """
    st.code(create_task_share_objects_proc_query, language='sql')

# -------------------------------------warehouse management----------------------------------------------------#
with st.expander("**Warehouse Management Rules script**"):
    #st.markdown("""<p style='color:#29B5E8 ;font-size:20px; font-weight:bold;margin-bottom:0px;'><i class="fas fa-info-circle"></i> Warehouse Management Rules script</p>""",unsafe_allow_html=True)
    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is to create METADATA for WAREHOUSE MANAGEMENT</h4>""",unsafe_allow_html=True)

    create_wh_table_query = f"""
    CREATE OR REPLACE TABLE {snowflake_config["database"]}.{snowflake_config["schema"]}.Warehouse_Management
    (
        UNIQUEID NUMBER(38,0) autoincrement start 1 increment 1 order,
        WAREHOUSE_NAME VARCHAR(16777216),
        FREQUENCY VARCHAR(16777216),
        DATE DATE,
        ACTIVATE_DAY VARCHAR(16777216),
        ACTIVATE_TIME TIME(9),
        STATUS VARCHAR(16777216),
        SIZE VARCHAR(16777216),
        ISACTIVE BOOLEAN
    )
    """
    st.code(create_wh_table_query, language='sql')

    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is to create PROCEDURE for WAREHOUSE MANAGEMENT</h4>""",unsafe_allow_html=True)


    create_list_wh_proc_query = """
    CREATE OR REPLACE PROCEDURE """ + snowflake_config["database"] + "." + snowflake_config["schema"] + """.SPU_WAREHOUSE_MANAGEMENT()
    RETURNS VARCHAR(16777216)
    language python
    runtime_version = 3.11
    packages =('snowflake-snowpark-python')
    handler = 'main'
    as 'import snowflake.snowpark as snowpark
    from snowflake.snowpark.functions import col,current_date,current_time,left,replace
    from datetime import datetime
    import calendar

    def check_warehouse_status(session, warehouse_name):
        query = f"SHOW WAREHOUSES LIKE ''{warehouse_name}''"
        result = session.sql(query).collect()
        warehouse_state = result[0][''state'']
        return warehouse_state
            

    def get_day_of_week():
        days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        today = datetime.today()
        day_of_week_str = days_of_week[today.weekday()]
        return day_of_week_str


    def main(session: snowpark.Session): 
        
        currenttime = datetime.now().strftime(''%H:%M'')
        print(currenttime)
        day_of_week = get_day_of_week()
        print("Today is:", day_of_week)

        
        table = ''""" + snowflake_config["schema"] + """.warehouse_Management''
        Conditioncheck = (col("isactive")==1)
        TimeCheck = (col("Activate_time") == currenttime)
        DayCheck = (col("ACTIVATE_DAY").isNull() | col("ACTIVATE_DAY").like(f''%{day_of_week}%'') |(col("ACTIVATE_DAY") == '''') | (col("ACTIVATE_DAY") == '' '') )
        dataframe = session.table(table).filter(Conditioncheck & TimeCheck & DayCheck)
        dataframe.show()
        
        # Collecting the DataFrame to a list of Rows for iteration
        Statusupdate = dataframe.filter((col("STATUS").isin("Suspend","Resume") )).collect()
            
        for row in Statusupdate:
            name = row["WAREHOUSE_NAME"]
            Status = row["STATUS"]
            warehouse_status = check_warehouse_status(session, name)
            print(warehouse_status)
            warehouse_status = warehouse_status.replace(''STARTED'', ''Resume'').replace(''SUSPENDED'', ''Suspend'')
            print(warehouse_status)
            
            if warehouse_status != Status:
                session.sql(f''ALTER WAREHOUSE {name} {Status};'').collect()
            
        Sizeupdate = dataframe.filter((col("SIZE").isin("X-Small","Small","Medium","Large","X-Large","2X-Large","3X-Large","4X-Large") )).collect()
            
        for row in Sizeupdate:
            name = row["WAREHOUSE_NAME"]
            size = row["SIZE"]
            query=(f"ALTER WAREHOUSE {name} SET warehouse_size=''{size}'';")
            session.sql(query).collect() 

        return "Procedure Completed Successfully"


    ';
    """

    st.code(create_list_wh_proc_query, language='sql')

    st.markdown("""<h4 style='color:#6b6f76 ;font-size:16px; font-weight:bold;'>Below script is to create SCHEDULE for WAREHOUSE MANAGEMENT</h4>""",unsafe_allow_html=True)

    create_task_warehouse_proc_query = """
                    create or replace task """ + snowflake_config["schema"] + """.TK_WAREMANAGEMENT
                        warehouse=COMPUTE_WH
                        schedule='USING CRON 0 22 * * * UTC'
                        as Call """ + snowflake_config["schema"] + """.SPU_WAREHOUSE_MANAGEMENT();
                    """
    st.code(create_task_warehouse_proc_query, language='sql')

    create_task_warehouse_alter ="""Alter task """ + snowflake_config["schema"] + """.TK_WAREMANAGEMENT resume;"""

    st.code(create_task_warehouse_alter, language='sql')