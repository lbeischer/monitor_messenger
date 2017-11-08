# Python script to write event to databases specified in config.ini
#
# Lewis Beischer
# 24/10/2017

# This section imports the required libraries for the messenger

import os, time, datetime, csv, sys # Importing system packages
from dateutil import parser as dateutilparser
import configparser # Importing config parser
import EXASOL
import psycopg2
import re
import xml.etree.ElementTree as ET

def quote_str(string):
    return "'" + str(string) + "'"

def cross_tab(unique_list, cross_list, tag=None):
    output_list = []
    for tool in unique_list:
        number_of_occur = cross_list.count(tool)
        if tag:
            output_list.append([tool, tag, number_of_occur])
        else:
            output_list.append([tool, number_of_occur])
    return output_list

def tag_list_of_list(list_to_tag, tag):
    for row in list_to_tag:
        row.append(tag)

def app_logs_to_exasol(exasol_connection, exasol_cur, schema, table, username, application, event, timestamp, alteryx_logs=None, runtime=None, parsed_workflows=None, logger_count=None):
    # This is a function to insert data into exasol
    # Pre-create the SQL statements and have variables ready to be used
    if (alteryx_logs is not None) & (runtime is not None) & (parsed_workflows is not None) & (logger_count is not None):
        sql_statement = 'INSERT INTO {}.{} (USERNAME, APPLICATION, EVENT_TYPE, EVENT_TIMESTAMP, ALTERYX_LOGS, TOTAL_RUNTIME, PARSED_WORKFLOWS, LOGGER_COUNT) VALUES ({}, {}, {}, {}, {}, {}, {}, {});'.format(exa_schema, exa_table, quote_str(username_var), quote_str(application_var), quote_str(event_type_var), quote_str(currenttime_var), alteryx_logs, runtime, parsed_workflows, logger_count)
    else:
        sql_statement = 'INSERT INTO {}.{} (USERNAME, APPLICATION, EVENT_TYPE, EVENT_TIMESTAMP) VALUES ({}, {}, {}, {});'.format(exa_schema, exa_table, quote_str(username_var), quote_str(application_var), quote_str(event_type_var), quote_str(currenttime_var))

    # After preparing the SQL statements we need to execute these and then commit the changes to the database
    exasol_cur.execute(sql_statement)
    exasol_connection.commit()

def alteryx_logs_to_exasol(exasol_connection, exasol_cur, schema, table, alteryx_logs_list=None):
    # Function used to insert the alteryx log lists into the exasol database
    # Check to make sure there are actually logs in the list before executing
    if alteryx_logs_list is not None:
        # Then loop through the list of entries to insert and format the SQL before executing the transaction
        python_alteryx_logs_list = list(alteryx_logs_list)
        if len(python_alteryx_logs_list) > 1:
            values_for_entry = ""
            idx = 0
            for entry in python_alteryx_logs_list:
                if idx == 0:
                    value_entry = "({},{},{},{},{},{})".format(quote_str(entry[0]), quote_str(entry[1]), entry[2], quote_str(entry[3]), quote_str(entry[4]), quote_str(entry[5]))
                else:
                    value_entry = ",({},{},{},{},{},{})".format(quote_str(entry[0]), quote_str(entry[1]), entry[2], quote_str(entry[3]), quote_str(entry[4]), quote_str(entry[5]))
                values_for_entry = values_for_entry + value_entry
                idx =+ 1
            sql_alteryx_logs = 'INSERT INTO {}.{} (TOOL_NAME, TOOL_TYPE, TOOL_COUNT, USERNAME, EVENT_TIMESTAMP, APPLICATION) VALUES {};'.format(schema, table, values_for_entry) 
        else:
            sql_alteryx_logs = 'INSERT INTO {}.{} (TOOL_NAME, TOOL_TYPE, TOOL_COUNT, USERNAME, EVENT_TIMESTAMP, APPLICATION) VALUES ({}, {}, {}, {}, {}, {});'.format(schema, table, quote_str(entry[0]), quote_str(entry[1]), entry[2], quote_str(entry[3]), quote_str(entry[4]), quote_str(entry[5]))
        exasol_cur.execute(sql_alteryx_logs)
        # Commiting the transaction to the database
        exasol_connection.commit()

def tool_name_extract(node, tool_parse_regex, macro_parse_regex):
    # Function designed to extract a tool name from a node
    # Either parsing tool (nodes) as an Alteryx tool or Macro
    tool_tag = node.find('GuiSettings').get('Plugin')

    if tool_tag is not None:
        # If there is something returned from Plugin
        tool_name_regex = re.search(tool_parse_regex, tool_tag)
        if tool_name_regex:
            try:
                tool_name = tool_name_regex.group(0)
                macro_name = None
                # Testing to see if the tool is a container as this may container more tools
            except:
                tool_name = "Unknown Tool"
                macro_name = None
        else:
            tool_name = "Unknown Tool"
            macro_name = None
    else:
        macro = node.find('EngineSettings').get('Macro')
        if macro:
            try:
                macro_name_regex = re.search(macro_parse_regex, macro).group(0)
                macro_name_regex_final = re.search(tool_parse_regex, macro_name_regex)
                if macro_name_regex_final:
                    macro_name = macro_name_regex_final.group(0)
                    tool_name = None
                else:
                    macro_name = macro_name_regex
                    tool_name = None
            except:
                macro_name = "Unknown Macro"
                tool_name = None
        else:
            macro_name = "Unknown Macro"
            tool_name = None

    if tool_name:
        return tool_name, None
    elif macro_name:
        return None, macro_name
    else:
        return "Unknown Tool", None

def parse_alteryx_logs(user_alteryx_log_folder, last_alteryx_log_date, username_var, currenttime_var, application_var, unparsed_alteryx_workflows=None):
    for root, dirs, files in os.walk(user_alteryx_log_folder):
        # Loop through the files in the log directory and inialisie variables 
        total_parsed_alteryx_tool_list = []
        if unparsed_alteryx_workflows is None:
            unparsed_alteryx_workflows = []
        logger_tools_found = 0
        alteryx_run_count = 0
        alteryx_parsed_workflows = 0
        alteryx_run_time = []
        # Creating lists for alteryx and macro tools for pre-processing
        alteryx_tool_list = []
        macro_tool_list = []
        for fname in files:
            file_path = os.path.join(root,fname)
            filestat = os.stat(file_path)
            file_mod_timestamp = datetime.datetime.fromtimestamp(filestat.st_mtime)
            if file_mod_timestamp > last_alteryx_log_date:
                alteryx_run_count += 1
                log_file = open(file_path,'r', encoding='utf-16-le')
                log_file_lines = log_file.readlines()
                first_line = log_file_lines[0]
                last_line = log_file_lines[len(log_file_lines)-1]

                # Compile regex and search for times
                time_regex = re.compile('[\d:.]+')
                time_find = time_regex.findall(last_line)
                alteryx_run_time.append(time_find[0])

                # Compile regex and search for workflow directory
                first_line_begin_removed = re.sub('Started running ','',first_line)
                yxmd_location = re.search('\.yxmd', first_line_begin_removed)

                if yxmd_location:
                    # Clean directory path
                    workflow = first_line_begin_removed[0:yxmd_location.end()]
                    workflow_path = workflow.replace(u'\ufeff', '')

                    # Setup regex for tool parsing and macro parsing
                    tool_parse_regex = re.compile('\w+$')
                    macro_parse_regex = re.compile('.+?(?=.yxmc)')

                    #Try and parse yxmd file with XML
                    try:
                        workflow_xml = ET.parse(workflow_path)

                        alteryx_parsed_workflows += 1
                        # Intialising tool lists for each workflow
                        workflow_alteryx_tool_list = []
                        workflow_macro_tool_list = []
                        # Parsing XML files
                        workflow_root = workflow_xml.getroot()
                        alteryx_nodes = workflow_root.find('Nodes')
                        alteryx_tools = alteryx_nodes.findall('Node')

                        # Either parsing tool (nodes) as an Alteryx tool or Macro
                        for tool in alteryx_tools:
                            tool_name, macro_name = tool_name_extract(tool, tool_parse_regex, macro_parse_regex)
                            # Check to see if there are tools
                            if tool_name == "ToolContainer":
                                # If the tool is a container look at the child tools and parse these out as well
                                container_tools = tool.find('ChildNodes').findall('Node')

                                if container_tools:
                                    for cont_tool in container_tools:
                                        # Add all of the tools to the workflow list 
                                        cont_tool_name, cont_macro_name = tool_name_extract(cont_tool, tool_parse_regex, macro_parse_regex)

                                        if cont_tool_name == "ToolContainer":

                                            container_container_tools = cont_tool.find('ChildNodes').findall('Node')

                                            if container_container_tools:
                                                for cont_cont_tool in container_container_tools:
                                                    # 2nd inside of the container
                                                    cont_cont_tool_name, cont_cont_macro_name = tool_name_extract(cont_cont_tool, tool_parse_regex, macro_parse_regex)

                                                    if cont_cont_tool_name:
                                                        workflow_alteryx_tool_list.append(cont_cont_tool_name)
                                                    if cont_cont_macro_name:
                                                        workflow_macro_tool_list.append(cont_cont_macro_name)

                                        if cont_tool_name:
                                            workflow_alteryx_tool_list.append(cont_tool_name)
                                        if cont_macro_name:
                                            workflow_macro_tool_list.append(cont_macro_name)

                            if tool_name:
                                workflow_alteryx_tool_list.append(tool_name)
                            if macro_name:
                                workflow_macro_tool_list.append(macro_name)


                        if "Logger" in workflow_macro_tool_list:
                            # If a logger is found in the macro list then add 1 to logger tools found variable
                            # and DO NOT add the tools to the unique list
                            logger_tools_found += 1
                        else:
                            # If no logger is found then append the tools onto the total list
                            if len(workflow_alteryx_tool_list) > 0:
                                for tool in workflow_alteryx_tool_list:
                                    alteryx_tool_list.append(tool)
                            if len(workflow_macro_tool_list) > 0:
                                for macro in workflow_macro_tool_list:
                                    macro_tool_list.append(macro)

                    except Exception as parsing_error:
                        if unparsed_alteryx_workflows is not None:
                            unparsed_alteryx_workflows.append([workflow_path, currenttime_var.strftime("%Y-%m-%d %H:%M:%S.%f")])
                        # print(parsing_error)


    # Creating cross-tab entries to insert into the database
    if len(alteryx_tool_list) > 0:
        unique_alteryx_tool_list = list(set(alteryx_tool_list))
        alteryx_final_tool_list = cross_tab(unique_alteryx_tool_list, alteryx_tool_list, "AlteryxTool")
    else:
        alteryx_final_tool_list = []
    if len(macro_tool_list) > 0:
        unique_alteryx_macro_list = list(set(macro_tool_list))
        macro_final_tool_list = cross_tab(unique_alteryx_macro_list, macro_tool_list, "Macro")
    else:
        macro_final_tool_list = []

    # Creating cross-tab lists
    total_parsed_alteryx_tool_list = macro_final_tool_list + alteryx_final_tool_list
    if len(total_parsed_alteryx_tool_list) > 0:
        tag_list_of_list(total_parsed_alteryx_tool_list, username_var)
        tag_list_of_list(total_parsed_alteryx_tool_list, str(currenttime_var))
        tag_list_of_list(total_parsed_alteryx_tool_list, application_var)
    else:
        total_parsed_alteryx_tool_list = None

    # Calculating run time
    total_alteryx_runtime = 0
    zero_time = datetime.datetime.strptime("00:00:00.000","%H:%M:%S.%f")
    # Loop for runtime
    for runtime in alteryx_run_time:
        t = datetime.datetime.strptime(runtime,"%H:%M:%S.%f")
        time_change = t - zero_time
        total_alteryx_runtime = total_alteryx_runtime + time_change.seconds

    return total_parsed_alteryx_tool_list, alteryx_parsed_workflows, alteryx_run_count, total_alteryx_runtime, logger_tools_found, unparsed_alteryx_workflows


def parse_alteryx_workflows(alteryx_workflow_list, username_var, currenttime_var, application_var):
    # Loop through the files in the log directory and inialisie variables 
    total_parsed_alteryx_tool_list = []
    logger_tools_found = 0
    alteryx_run_count = 0
    alteryx_parsed_workflows = 0
    alteryx_run_time = []
    unparsed_alteryx_workflows = []
    # Creating lists for alteryx and macro tools for pre-processing
    alteryx_tool_list = []
    macro_tool_list = []
    # Setup regex for tool parsing and macro parsing
    tool_parse_regex = re.compile('\w+$')
    macro_parse_regex = re.compile('.+?(?=.yxmc)')

    for alteryx_workflow in alteryx_workflow_list:
        date_of_workflow = alteryx_workflow[1]
        alteryx_workflow_path = alteryx_workflow[0]
        alteryx_workflow_original_parse_date = dateutilparser.parse(date_of_workflow)
        #Try and parse yxmd file with XML
        try:
            workflow_xml = ET.parse(alteryx_workflow_path)

            alteryx_parsed_workflows += 1
            # Intialising tool lists for each workflow
            workflow_alteryx_tool_list = []
            workflow_macro_tool_list = []
            # Parsing XML files
            workflow_root = workflow_xml.getroot()
            alteryx_nodes = workflow_root.find('Nodes')
            alteryx_tools = alteryx_nodes.findall('Node')

            # Either parsing tool (nodes) as an Alteryx tool or Macro
            for tool in alteryx_tools:

                tool_name, macro_name = tool_name_extract(tool, tool_parse_regex, macro_parse_regex)
                # Check to see if there are tools
                if tool_name == "ToolContainer":
                    # If the tool is a container look at the child tools and parse these out as well
                    container_tools = tool.find('ChildNodes').findall('Node')
                    if container_tools:
                        for cont_tool in container_tools:
                            # Add all of the tools to the workflow list 
                            cont_tool_name, cont_macro_name = tool_name_extract(cont_tool, tool_parse_regex, macro_parse_regex)
                            if cont_tool_name == "ToolContainer":

                                container_container_tools = cont_tool.find('ChildNodes').findall('Node')

                                if container_container_tools:
                                    for cont_cont_tool in container_container_tools:
                                        # 2nd inside of the container
                                        cont_cont_tool_name, cont_cont_macro_name = tool_name_extract(cont_cont_tool, tool_parse_regex, macro_parse_regex)

                                        if cont_cont_tool_name:
                                            workflow_alteryx_tool_list.append(cont_cont_tool_name)
                                        if cont_cont_macro_name:
                                            workflow_macro_tool_list.append(cont_cont_macro_name)

                            if cont_tool_name:
                                workflow_alteryx_tool_list.append(cont_tool_name)
                            if cont_macro_name:
                                workflow_macro_tool_list.append(cont_macro_name)
                if tool_name:
                    workflow_alteryx_tool_list.append(tool_name)
                if macro_name:
                    workflow_macro_tool_list.append(macro_name)


            if "Logger" in workflow_macro_tool_list:
                # If a logger is found in the macro list then add 1 to logger tools found variable
                # and DO NOT add the tools to the unique list
                logger_tools_found += 1
            else:
                # If no logger is found then append the tools onto the total list
                if len(workflow_alteryx_tool_list) > 0:
                    for tool in workflow_alteryx_tool_list:
                        alteryx_tool_list.append(tool)
                if len(workflow_macro_tool_list) > 0:
                    for macro in workflow_macro_tool_list:
                        macro_tool_list.append(macro)

        except Exception as parsing_error:
            # If there is an error ensure that the filepath and date associated with it are saved
            unparsed_alteryx_workflows.append([alteryx_workflow_path, date_of_workflow])
            # print(parsing_error)


    # Creating cross-tab entries to insert into the database
    if len(alteryx_tool_list) > 0:
        unique_alteryx_tool_list = list(set(alteryx_tool_list))
        alteryx_final_tool_list = cross_tab(unique_alteryx_tool_list, alteryx_tool_list, "AlteryxTool")
    else:
        alteryx_final_tool_list = []
    if len(macro_tool_list) > 0:
        unique_alteryx_macro_list = list(set(macro_tool_list))
        macro_final_tool_list = cross_tab(unique_alteryx_macro_list, macro_tool_list, "Macro")
    else:
        macro_final_tool_list = []

    # Creating cross-tab lists
    total_parsed_alteryx_tool_list = macro_final_tool_list + alteryx_final_tool_list
    if len(total_parsed_alteryx_tool_list) > 0:
        tag_list_of_list(total_parsed_alteryx_tool_list, username_var)
        tag_list_of_list(total_parsed_alteryx_tool_list, str(currenttime_var))
        tag_list_of_list(total_parsed_alteryx_tool_list, application_var)
    else:
        total_parsed_alteryx_tool_list = None

    # Calculating run time
    total_alteryx_runtime = 0
    zero_time = datetime.datetime.strptime("00:00:00.000","%H:%M:%S.%f")
    # Loop for runtime
    for runtime in alteryx_run_time:
        t = datetime.datetime.strptime(runtime,"%H:%M:%S.%f")
        time_change = t - zero_time
        total_alteryx_runtime = total_alteryx_runtime + time_change.seconds

    return total_parsed_alteryx_tool_list, alteryx_parsed_workflows, alteryx_run_count, total_alteryx_runtime, logger_tools_found, unparsed_alteryx_workflows

# -----------------------------------------------------------------------------------------------------------#
#                                 START OF MAIN SCRIPT LOGIC
# -----------------------------------------------------------------------------------------------------------#

# Exception log is used to log any exceptions found during running
exception_log = []

# Create get the current list of unparsed workflows
try:
    with open('unparsed_alteryx_workflows_log.txt', 'r') as logfile:
        unparsed_alteryx_workflows_csv = csv.reader(logfile, delimiter=",")
        unparsed_alteryx_workflows = []
        for row in unparsed_alteryx_workflows_csv:
            unparsed_alteryx_workflows.append(row)
except:
    print("Error loading unparsed workflow logs")
    unparsed_alteryx_workflows = None


username_var = os.environ.get("USERNAME")
currenttime_var = datetime.datetime.now()
# Setting global variables - the current username according to the OS (likely to be windows) and the date time from the system

# Below are mnaully set variables for each version of the executable
# These take the first two arguments after the executable i.e. app_messenger.exe -Start -Alteryx
# This allows the executable to run for all different types of application event with the arguments the only change
event_type_var = str(sys.argv[2])
application_var = str(sys.argv[1])

# The program will  always log unless a successful connection is made
exasol_connection_made = False # Setting exasol connection made to false 
postgres_connection_made = False # Setting postgres connection made to false

# Initialising whether the alteryx log files have been written correctly
written_alteryx_log_files = True

# Setting the Alteryx logging location (generated by xml_logging_enabler)
user_alteryx_log_folder = "C:/Users/"+username_var+"/Documents/Alteryx Log"

# Loading the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Importing last time Alteryx logs were read (will only be used when sending alteryx start)
last_alteryx_log_date = dateutilparser.parse(config['lastupdate']['timestamp'])

# Set connection credentials if they are available then try to connect
if config['exasol'].getboolean('use'):
    # If a config for exasol exists and use is true then we will configure the postgres 
    # connection using the values in the config file
    exa_host = config['exasol']['host']
    exa_port = config['exasol']['port']
    exa_uid = config['exasol']['user']
    exa_pwd = config['exasol']['password']
    exa_schema = config['exasol']['schema']
    exa_table = config['exasol']['table']
    exa_alteryx_table = config['exasol']['alteryx_log_table']
    websocket_str = 'ws://'+ exa_host + ':' + exa_port
    try:
        # Try connecting to exasol and if successful then set connection made to true
        exasol_conn = EXASOL.connect(websocket_str, exa_uid, exa_pwd)
        # Connecting to Exasol database using the websocket connection string, if connection is successful we set the exasol_connection_made to True
        # This enables us to use this in logic later to decide whether to push data to the Exasol database
        exasol_connection_made = True
        # Creating the Exasol cursor that we use to insert data later in the script
        exasol_cur = exasol_conn.cursor()
    except:
        print("Unable to connect")

if config['postgres'].getboolean('use'):
    # If a config for postgres exists and use is true then we will configure the postgres 
    # connection using the values in the config file
    postgres_host = config['postgres']['host']
    postgres_port = config['postgres']['port']
    postgres_uid = config['postgres']['user']
    postgres_pwd = config['postgres']['password']
    postgres_schema = config['postgres']['schema']
    postgres_table = config['postgres']['table']
    connection_string = "dbname={} user={} host={} port={} password={}".format(quote_str(postgres_schema), quote_str(postgres_uid), quote_str(postgres_host), quote_str(postgres_port), quote_str(postgres_pwd))
    try:
        # Try connecting to postgres then setting the connect made variable to true
        postgres_conn = psycopg2.connect(connection_string)
        # These are the connection details to the London LEK Database for app monitoring - i.e. where it sends the events
        # This should be replaced with the local equivalents where possible and assigned a separate user (for security reasons)
        postgres_connection_made = True
        postgres_cur = postgres_conn.cursor()

        # Here we generate a connection object (postgres_conn) using the psycopg2 module
        # If unsuccessful the script skips to the except section 
        # If successful it sets the connection_made variable to true (used in logic later)
        # Then we set a cursor

        # If successfully connected then send the logfile if exists
        # The logfile should be populated with all events that we haven't been able to log whilst unable to connect to the DB   
    except:
        postgres_connection_made = False

# -----------------------------------------------------------------------------------------------------------#
#                                 SEND ANY CURRENT LOG FILES STORED
# -----------------------------------------------------------------------------------------------------------#


if exasol_connection_made or postgres_connection_made:
    # If a connection is made then we send the current log files to the database

    # The script is designed to log application events (as specified by the user)
    # If it can't connect to the database it saves these events to a log file
    # If it is able to connect to the database it first tries to send the log file of application events
    # It then tries to send the current application event
    try:    
        # If successfully connected then send the logfile if exists
        # The logfile should be populated with all events that we haven't been able to log whilst unable to connect to the DB
        if os.path.isfile('log.txt'):
            # This checks if there is a log file (i.e. any unsent application events)
            # If it detects the file it then opens it and reads it as a .csv with an application event in each row
            try:
                with open('log.txt', 'r') as logfile:
                    reader = csv.reader(logfile, delimiter=",")
                    # Reading the text file as a csv and saving as an object of tuples
                    try:
                        for row in reader:
                            # Iterating through rows and inserting them into database using connection created earlier

                            if postgres_connection_made:
                                # If a postgres configuration is created then try sending log file
                                # If this creates an exception (error) then set connection to false
                                try:
                                    cur.execute("INSERT INTO public.application_events (username, application, event_type, event_timestamp) VALUES (%s, %s, %s, %s)",(row[0], row[1], row[2], row[3]))
                                    conn.commit()
                                except:
                                    postgres_connection_made = False

                            if exasol_connection_made:
                                # If an exasol configuration is available and connection has been made try sending log file
                                # If this creates an exception (error) then set the connection to false
                                try:
                                    app_logs_to_exasol(exasol_conn, exasol_cur, exa_schema, exa_table, row[0], row[1], row[2], row[3])
                                except:
                                    exasol_connection_made = False

                        if postgres_connection_made or exasol_connection_made:
                            # Only close the file and remove if EITHER connection is still true (i.e. the log file was sent to at least one of the databases)
                            logfile.close()
                            # Closing the file and removing so future tests don't try and send empty files
                            os.remove('log.txt')

                    except Exception as e:
                        # If unable to send all the logfile we close the log file so that we don't delete these events
                        # The file is also not removed here so at the next application event it tries to send again
                        logfile.close()
                        postgres_connection_made = False
                        exasol_connection_made = False
            except Exception as logfile_exception:
                # Exception if unable to open the log file
                exception_log.append(logfile_exception)

        # We only want the alteryx workflow parsing to be present when we are actually
        if 'alteryx' in application_var.lower():
            # If alteryx is in the applciation variables
            if os.path.isfile('alteryx_condensed_logs.txt'):
                    with open('alteryx_condensed_logs.txt', 'r', encoding = 'utf-8') as alteryx_logfile_read:
                        alteryx_reader = csv.reader(alteryx_logfile_read, delimiter="," )
                        # Reading the text file as a csv and saving as an object of tuples
                        if config['exasol'].getboolean('use'):
                        # If Exasol is configured send the log file to Exasol
                            try:
                                alteryx_logs_to_exasol(exasol_conn, exasol_cur, exa_schema, exa_alteryx_table, alteryx_reader)
                                # If successful in sending the logs to Exasol close the file and remove
                                alteryx_logfile_read.close()
                                os.remove('alteryx_condensed_logs.txt')

                            except Exception as e:
                                # If there is an erro when sending the log file to Exasol set the connection to False so logs are created
                                exasol_connection_made = False
                                # print(e)
        else:
            # This is where the script will go if no log files are found
            exasol_connection_made = False
    except:
        # This is what the script executes if a connection cannot be created to the DB
        exasol_connection_made = False


# -----------------------------------------------------------------------------------------------------------#
#                         GENERATE NEW ENTRIES AND EITHER SEND OR ADD TO LOG FILES
# -----------------------------------------------------------------------------------------------------------#

# Here if the application is Alteryx we will parse the alteryx log files and either send them to the database (if connections have been made)
# Alternatively we save them to log files

# If the event in an Alteryx shortcut we will parse Alteryx files to either insert them or save them to log files
if 'alteryx' in application_var.lower():
    # Only parse the Alteryx files if they are either opening or closing alteryx
    total_parsed_alteryx_tool_list, alteryx_parsed_workflows, alteryx_run_count, total_alteryx_runtime, logger_tools_count, unparsed_alteryx_workflows = parse_alteryx_logs(user_alteryx_log_folder, last_alteryx_log_date, username_var, currenttime_var, application_var, unparsed_alteryx_workflows)
    # Now we parse the unparsed alteryx workflows
    unparsed_alteryx_tool_list, unparsed_alteryx_parsed_workflows, unparsed_alteryx_run_count, unparsed_total_alteryx_runtime, unparsed_logger_tools_found, unparsed_alteryx_workflows = parse_alteryx_workflows(unparsed_alteryx_workflows, username_var, currenttime_var, application_var)
    # Now we need to combine the parsed workflows and unparsed workflows
    if unparsed_alteryx_tool_list:
        for unparsed_tool in unparsed_alteryx_tool_list:
            total_parsed_alteryx_tool_list.append(unparsed_tool)
        alteryx_parsed_workflows = alteryx_parsed_workflows + unparsed_alteryx_parsed_workflows
        alteryx_run_count = alteryx_run_count + unparsed_alteryx_run_count
        total_alteryx_runtime = total_alteryx_runtime + unparsed_total_alteryx_runtime
        logger_tools_count = logger_tools_count + unparsed_logger_tools_found

# Now we have tried to send the logfile we need to send the actual application event 
# If EITHER connection has been made then send off the event log and the alteryx parsed log
if postgres_connection_made or exasol_connection_made:

    if postgres_connection_made:
        try:
            # Here we are re-using the connection we made earlier in the script
            # Inserting current values into the database
            postgres_cur.execute("INSERT INTO public.application_events (username, application, event_type, event_timestamp) VALUES (%s, %s, %s, %s)",(username_var, application_var, event_type_var, currenttime_var))
            postgres_conn.commit()

        except Exception as insert_error:
            # If there is an error inserting set connection as false
            postgres_connection_made = False


    if exasol_connection_made:
        # If an exasol configuration is available and connection has been made try sending log file
        # If this creates an exception (error) then set the connection to false

        # Test if the Alteryx files have been parsed (and if there are runs) insert these details otherwise just insert the basic values
        if 'alteryx' in application_var.lower():
            try:
                app_logs_to_exasol(exasol_conn, exasol_cur, exa_schema, exa_table, username_var, application_var, event_type_var, currenttime_var, alteryx_run_count, total_alteryx_runtime, alteryx_parsed_workflows, logger_tools_count)
            except Exception as exasol_app_event_logger_new:
                # If there was an error inserting set connection as false
                exasol_connection_made = False
                written_alteryx_log_files = False
                print("Unable to send app_event logs alteryx")
                print(exasol_app_event_logger_new)
        else:
            try:
                app_logs_to_exasol(exasol_conn, exasol_cur, exa_schema, exa_table, username_var, application_var, event_type_var, currenttime_var) #alteryx_logs, runtime, parsed_workflows, logger_numbers
            except:
                # If there was an error inserting set connection as false
                exasol_connection_made = False
                print("Unable to app event logs non-alteryx")


# Only if BOTH connections have failed should you write the logs to a file for later sending to databases
if (not postgres_connection_made) and (not exasol_connection_made):

    # If the alteryx logs have been parsed then the tool list should be inserted into log files
    if total_parsed_alteryx_tool_list:
        try:
            # Opening a file to contain the tool list of parsed workflows
            with open('alteryx_condensed_logs.txt','a', encoding = 'utf-8', newline='') as alteryx_logfile:
                alteryx_writer = csv.writer(alteryx_logfile, delimiter=",")
                alteryx_writer.writerows(total_parsed_alteryx_tool_list)
                # Writing the unsent application events to a csv file with each line representing an application event

            alteryx_logfile.close()
            written_alteryx_log_files = True
            # Closing log file to ensure that no other changes are saved and can be accessed by the script if required again
        except Exception as logging_error_exception:
            exception_log.append(logging_error_exception)


    # This part of the script runs if the values cannot be added to the database
    # This adds the application event to the logfile 
    if alteryx_run_count:
        entry = [[username_var, application_var, event_type_var, currenttime_var, alteryx_run_count, total_alteryx_runtime, alteryx_parsed_workflows, logger_tools_count],]
    else:
        entry = [[username_var, application_var, event_type_var, currenttime_var, '', '', '', ''],]

    # Creating a tuple (array) of the application event - effectively an array of objects (although this should never exceed one object)
    try:
        # Open a log file to write new logs or append onto existing logs
        with open('log.txt','a', encoding = 'utf-8', newline='') as logfile:
            writer = csv.writer(logfile, delimiter=",")
            writer.writerows(entry)
            # Writing the unsent application events to a csv file with each line representing an application event

        logfile.close()
        # Closing log file to ensure that no other changes are saved and can be accessed by the script if required again
        
    except Exception as single_app_event_error:
        exception_log.append(single_app_event_error)

# Only if the alteryx tool lists have not been written to either DB or log files do we update the config file and delete old alteryx log files
if written_alteryx_log_files:
    # This should only run if the parsed logfiles have either been sent to the database or written to log files
    config['lastupdate']['timestamp'] = currenttime_var.strftime("%Y-%m-%d %H:%M:%S.%f")
    with open('config.ini', 'w') as new_configfile:
        config.write(new_configfile)
    # Delete any Alteryx log files if they are older than 30 days, this is to stop 
    for file in os.listdir(user_alteryx_log_folder):
        fullpath   = os.path.join(user_alteryx_log_folder,file)
        timestamp  = os.stat(fullpath).st_ctime # get timestamp of file
        createtime = datetime.datetime.fromtimestamp(timestamp)
        now        = datetime.datetime.now()
        delta      = now - createtime
        if delta.days > 30:
            os.remove(fullpath)
    # Write down any unparsed alteryx workflows to a log file
    with open('unparsed_alteryx_workflows_log.txt', 'w', encoding = 'utf-8', newline='') as unparsed_logfiles:
                unparsed_alteryx_writer = csv.writer(unparsed_logfiles, delimiter=",")
                unparsed_alteryx_writer.writerows(unparsed_alteryx_workflows)
                # Writing the unparsed workflows events to a csv file with each line representing an workflow
                unparsed_logfiles.close()

# This checks if a connection was made and if it was it closes the connection (which conserves connection slots and also prevents intrusions)
if postgres_connection_made:
    try:
        # Closing the postgres connection if exists
        postgres_conn.close()
    except:
        postgres_connection_made = False
else:
    postgres_connection_made = False


# Checking if Exasol connection made and closing if exists
if exasol_connection_made:
    try:
        # Closing the Exasol connection if exists
        exasol_connection.close()
    except:
        exasol_connection_made = False
else:
    exasol_connection_made = False

