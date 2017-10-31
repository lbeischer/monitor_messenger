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

def app_logs_to_exasol(exasol_connection, exasol_cur, schema, table, username, password, application, event, timestamp, alteryx_logs=None, runtime=None, parsed_workflows=None):
    # This is a function to insert data into exasol
    # Pre-create the SQL statements and have variables ready to be used
    if alteryx_logs & runtime & parsed_workflows:
        sql_alteryx = 'INSERT INTO {}.{} (USERNAME, APPLICATION, EVENT_TYPE, EVENT_TIMESTAMP, ALTERYX_LOGS, TOTAL_RUNTIME, PARSED_WORKFLOWS) VALUES ({}, {}, {}, {});'.format(exa_schema, exa_table, quote_str(username_var), quote_str(application_var), quote_str(event_type_var), quote_str(currenttime_var))
    else:
        sql_others = 'INSERT INTO {}.{} (USERNAME, APPLICATION, EVENT_TYPE, EVENT_TIMESTAMP) VALUES ({}, {}, {}, {});'.format(exa_schema, exa_table, quote_str(username_var), quote_str(application_var), quote_str(event_type_var), quote_str(currenttime_var))

    # After preparing the SQL statements we need to execute these and then commit the changes to the database
    exasol_cur.execute(sql_others)
    exasol_connection.commit()

def alteryx_logs_to_exasol(exasol_connection, exasol_cur, schema, table, alteryx_logs_list=None):
    # Function used to insert the alteryx log lists into the exasol database
    # Check to make sure there are actually logs in the list before executing
    if alteryx_logs_list is not None:
        # Then loop through the list of entries to insert and format the SQL before executing the transaction
        for entry in alteryx_logs_list:
            sql_alteryx_logs = 'INSERT INTO {}.{} (TOOL_NAME, TOOL_TYPE, TOOL_COUNT, USERNAME, EVENT_TIMESTAMP, APPLICATION) VALUES ({}, {}, {}, {}, {}, {});'.format(schema, table, quote_str(entry[0]), quote_str(entry[1]), entry[2], quote_str(entry[3]), quote_str(entry[4]), quote_str(entry[5]))
            print(sql_alteryx_logs)
            exasol_cur.execute(sql_alteryx_logs)
            # Commiting the transaction to the database
            exasol_connection.commit()


def parse_alteryx_logs(user_alteryx_log_folder, last_alteryx_log_date, username_var, currenttime_var, application_var):
    for root, dirs, files in os.walk(user_alteryx_log_folder):
        # Loop through the files in the log directory and inialisie variables 
        total_parsed_alteryx_tool_list = []
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
                        

                        # Parsing XML files
                        workflow_root = workflow_xml.getroot()
                        alteryx_nodes = workflow_root.find('Nodes')
                        alteryx_tools = alteryx_nodes.findall('Node')

                        # Either parsing tool (nodes) as an Alteryx tool or Macro
                        for tool in alteryx_tools:

                            tool_tag = tool.find('GuiSettings').get('Plugin')

                            if tool_tag is not None:
                                tool_list = []
                                tool_name_regex = re.search(tool_parse_regex, tool_tag)
                                if tool_name_regex:
                                    try:
                                        tool_name = tool_name_regex.group(0)
                                    except:
                                        tool_name = "Unknown Tool"
                                else:
                                    tool_name = "Unknown Tool"
                                # Append the tool on the list
                                alteryx_tool_list.append(tool_name)
                            else:
                                macro = tool.find('EngineSettings').get('Macro')
                                if macro:
                                    try:
                                        macro_name_regex = re.search(macro_parse_regex, macro).group(0)
                                        macro_name_regex_final = re.search(tool_parse_regex, macro_name_regex)
                                        if macro_name_regex_final:
                                            macro_name = macro_name_regex_final.group(0)
                                        else:
                                            macro_name = macro_name_regex
                                    except:
                                        macro_name = "Unknown Macro"
                                    # Append the macro on the list
                                    macro_tool_list.append(macro_name)
                    except:
                        print("Log file not accessible")

    # Creating cross-tab entries to insert into the database
    unique_alteryx_tool_list = list(set(alteryx_tool_list))
    unique_alteryx_macro_list = list(set(macro_tool_list))
    # Creating cross-tab lists
    macro_final_tool_list = cross_tab(unique_alteryx_macro_list, macro_tool_list, "Macro")
    alteryx_final_tool_list = cross_tab(unique_alteryx_tool_list, alteryx_tool_list, "AlteryxTool")
    total_parsed_alteryx_tool_list = macro_final_tool_list + alteryx_final_tool_list
    tag_list_of_list(total_parsed_alteryx_tool_list, username_var)
    tag_list_of_list(total_parsed_alteryx_tool_list, str(currenttime_var))
    tag_list_of_list(total_parsed_alteryx_tool_list, application_var)

    # Calculating run time
    print("Parsed workflows: " + str(alteryx_parsed_workflows))
    print("Alteryx workflows run: " + str(alteryx_run_count))
    total_alteryx_runtime = 0
    zero_time = datetime.datetime.strptime("00:00:00.000","%H:%M:%S.%f")
    # Loop for runtime
    for runtime in alteryx_run_time:
        t = datetime.datetime.strptime(runtime,"%H:%M:%S.%f")
        time_change = t - zero_time
        total_alteryx_runtime = total_alteryx_runtime + time_change.seconds
    print(total_alteryx_runtime)
    return total_parsed_alteryx_tool_list, alteryx_parsed_workflows, alteryx_run_count, total_alteryx_runtime


# -----------------------------------------------------------------------------------------------------------#
#                                 START OF MAIN SCRIPT LOGIC
# -----------------------------------------------------------------------------------------------------------#


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
        print("Unable to connect to postgres")

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

            #print("Log file found, sending to database")
            try:
                with open('log.txt', 'rb') as logfile:
                    reader = csv.reader(logfile, delimiter=",")
                    # Reading the text file as a csv and saving as an object of tuples
                    try:

                        for row in reader:
                            # Iterating through rows and inserting them into database using connection created earlier
                            cur.execute("INSERT INTO public.application_events (username, application, event_type, event_timestamp) VALUES (%s, %s, %s, %s)",(row[0], row[1], row[2], row[3]))
                            conn.commit()
                        #print("Logfile added to Database")
                        logfile.close()
                        # Closing the file and removing so future tests don't try and send empty files
                        os.remove('log.txt')
                        #print("Logfile deleted")

                    except:

                        #print("Unable to execute query")
                        #print(e)
                        logfile.close()
                        # If unable to send all the logfile we close the log file so that we don't delete these events
                        # The file is also not removed here so at the next application event it tries to send again

                with open('alteryx_condensed_logs.txt', 'r', encoding = 'utf-8') as alteryx_logfile_read:
                    alteryx_reader = csv.reader(alteryx_logfile_read, delimiter="," )
                    # Reading the text file as a csv and saving as an object of tuples
                    if config['exasol'].getboolean('use'):
                        print("Sending file to exasol")
                        alteryx_logs_to_exasol(exasol_conn, exasol_cur, exa_schema, exa_alteryx_table, alteryx_reader)
                        alteryx_logfile_read.close()

            except:

                # Exception if unable to open the log file
                print("Unable to open file")

        else:
            # This is where the script will go if no log file is found
            postgres_connection_made = False
            #print("No log file found")
    except:
        # This is what the script executes if a connection cannot be created to the DB
        postgres_connection_made = False
        #print ("Unable to connect to the database")






# Here if the application is Alteryx we will parse the alteryx log files and either send them to the database (if connections have been made)
# Alternatively we save them to log files

if application_var.lower() == 'alteryx':

    total_parsed_alteryx_tool_list, alteryx_parsed_workflows, alteryx_run_count, total_alteryx_runtime = parse_alteryx_logs(user_alteryx_log_folder, last_alteryx_log_date, username_var, currenttime_var, application_var)



    try:

        with open('alteryx_condensed_logs.txt','a', encoding = 'utf-8', newline='') as alteryx_logfile:
            alteryx_writer = csv.writer(alteryx_logfile, delimiter=",")
            alteryx_writer.writerows(total_parsed_alteryx_tool_list)

            # Writing the unsent application events to a csv file with each line representing an application event

        alteryx_logfile.close()
        print("File closed")
        # Closing log file to ensure that no other changes are saved and can be accessed by the script if required again
        if os.path.isfile('alteryx_condensed_logs.txt'):
            # This checks if there is a log file (i.e. any unsent application events)
            # If it detects the file it then opens it and reads it as a .csv with an application event in each row
            try:
                #something
                with open('alteryx_condensed_logs.txt', 'rb') as alteryx_logfile_read:
                    alteryx_reader = csv.reader(alteryx_logfile_read, delimiter=",")
                    # Reading the text file as a csv and saving as an object of tuples
                    try:
                        # Some note here
                        for row in alteryx_reader:
                            # Iterating through rows and inserting them into database using connection created earlier
                            print(row)
                            # cur.execute("INSERT INTO public.application_events (username, application, event_type, event_timestamp) VALUES (%s, %s, %s, %s)",(row[0], row[1], row[2], row[3]))
                            # conn.commit()
                        #print("Logfile added to Database")
                        logfile.close()
                        # Closing the file and removing so future tests don't try and send empty files
                    except:
                        #Something didn't work
                        print("something else")
            except:
                #something else didn't work
                print("something")
    except:
        # connection_made = False
        print("Unable to write to log")





# Now we have tried to send the logfile we need to send the actual application event 

try:
    # Here we are re-using the connection we made earlier in the script
    # If this connection was not made earlier it will fail the try and move to the exception

    cur.execute("INSERT INTO public.application_events (username, application, event_type, event_timestamp) VALUES (%s, %s, %s, %s)",(username_var, application_var, event_type_var, currenttime_var))
    conn.commit()
    # Inserting current values into the database
except:

    # This part of the script runs if the values cannot be added to the database
    # This adds the application event to the logfile 

    #print("Unable to execute query")
    #print(e)

    entry = [[username_var, application_var, event_type_var, currenttime_var],]

    # Creating a tuple (array) of the application event - effectively an array of objects (although this should never exceed one object)

    try:

        with open('log.txt','a') as logfile:
            writer = csv.writer(logfile, delimiter=",")
            writer.writerows(entry)

            # Writing the unsent application events to a csv file with each line representing an application event

        logfile.close()
        # Closing log file to ensure that no other changes are saved and can be accessed by the script if required again
        
        #print("Written to log file")
    except:
        postgres_connection_made = False
        #print("Unable to write to log")

# This checks if a connection was made and if it was it closes the connection (which conserves connection slots and also prevents intrusions)

if postgres_connection_made:
    try:

        conn.close()
    except:
        postgres_connection_made = False
        #print("Unable to Close Connection")
else:
    postgres_connection_made = False
    #print("Connection not made")