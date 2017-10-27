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
    exa_host = config['exasol']['host']
    exa_port = config['exasol']['port']
    exa_uid = config['exasol']['user']
    exa_pwd = config['exasol']['password']
    exa_schema = config['exasol']['schema']
    exa_table = config['exasol']['table']
    websocket_str = 'ws://'+ exa_host + ':' + exa_port
    try:
        # Try connecting to exasol and if successful then set connection made to true
        exasol_conn = EXASOL.connect(websocket_str, exa_uid, exa_pwd)
        exasol_connection_made = True
        print("Connected to Exasol")
        exasol_cur = exasol_conn.cursor()
    except:
        print("Unable to connect")


sql = 'INSERT INTO {}.{} ("username", "application", "event_type", "event_timestamp") VALUES ({}, {}, {}, {});'.format(exa_schema, exa_table, quote_str(username_var), quote_str(application_var), quote_str(event_type_var), quote_str(currenttime_var))
id_sql =  'SELECT "id" FROM {}.{} WHERE "username" = {} AND "application" = {} AND "event_type" = {} AND "event_timestamp" = {}'.format(exa_schema, exa_table, quote_str(username_var), quote_str(application_var), quote_str(event_type_var), quote_str(currenttime_var))

exasol_cur.execute(sql)
exasol_conn.commit()
print("Commited")
exasol_cur.execute(id_sql)
id_result = exasol_cur.fetchone()
print(id_result[0])

for root, dirs, files in os.walk(user_alteryx_log_folder):
    alteryx_run_count = 0
    alteryx_parsed_workflows = 0
    alteryx_run_time = []
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
                workflow = first_line_begin_removed[0:yxmd_location.end()]
                alteryx_parsed_workflows += 1
                workflow_path = workflow.replace(u'\ufeff', '')
                workflow_xml = ET.parse(workflow_path)
                workflow_root = workflow_xml.getroot()
                alteryx_nodes = workflow_root.find('Nodes')
                alteryx_tools = alteryx_nodes.findall('Node')
                for tool in alteryx_tools:
                    tool_tag = tool.find('GuiSettings').get('Plugin')
                    if tool_tag is not None:
                        print(tool_tag)
                    else:
                        macro = tool.find('EngineSettings').get('Macro')
                        if macro:
                            print("It is a macro")
                            print(macro)


print("Parsed workflows: " + str(alteryx_parsed_workflows))
print(alteryx_run_time)
print("Alteryx workflows run: " + str(alteryx_run_count))
total_alteryx_runtime = 0
zero_time = datetime.datetime.strptime("00:00:00.000","%H:%M:%S.%f")

for runtime in alteryx_run_time:
    t = datetime.datetime.strptime(runtime,"%H:%M:%S.%f")
    time_change = t - zero_time
    print(time_change)
    total_alteryx_runtime = total_alteryx_runtime + time_change.seconds
    print(total_alteryx_runtime)



# # The script is designed to log application events (as specified by the user)
# # If it can't connect to the database it saves these events to a log file
# # If it is able to connect to the database it first tries to send the log file of application events
# # It then tries to send the current application event

# try:
#     conn = psycopg2.connect("dbname='appmonitoring' user='appmonitor_agent' host='EUPOSTGRESQL01' port='5432' password='lek'")
#     # These are the connection details to the London LEK Database for app monitoring - i.e. where it sends the events
#     # This should be replaced with the local equivalents where possible and assigned a separate user (for security reasons)
#     connection_made = True
#     cur = conn.cursor()

#     # Here we generate a connection object (conn) using the psycopg2 module
#     # If unsuccessful the script skips to the except section 
#     # If successful it sets the connection_made variable to true (used in logic later)
#     # Then we set a cursor
    
#     # If successfully connected then send the logfile if exists
#     # The logfile should be populated with all events that we haven't been able to log whilst unable to connect to the DB

#     if os.path.isfile('log.txt'):
#         # This checks if there is a log file (i.e. any unsent application events)
#         # If it detects the file it then opens it and reads it as a .csv with an application event in each row

#         #print("Log file found, sending to database")
#         try:
#             with open('log.txt', 'rb') as logfile:
#                 reader = csv.reader(logfile, delimiter=",")
#                 # Reading the text file as a csv and saving as an object of tuples
#                 try:

#                     for row in reader:
#                         # Iterating through rows and inserting them into database using connection created earlier
#                         cur.execute("INSERT INTO public.application_events (username, application, event_type, event_timestamp) VALUES (%s, %s, %s, %s)",(row[0], row[1], row[2], row[3]))
#                         conn.commit()
#                     #print("Logfile added to Database")
#                     logfile.close()
#                     # Closing the file and removing so future tests don't try and send empty files
#                     os.remove('log.txt')
#                     #print("Logfile deleted")

#                 except Exception, e:

#                     #print("Unable to execute query")
#                     #print(e)
#                     logfile.close()
#                     # If unable to send all the logfile we close the log file so that we don't delete these events
#                     # The file is also not removed here so at the next application event it tries to send again
#         except:

#             # Exception if unable to open the log file
#             print("Unable to open file")

#     else:
#         # This is where the script will go if no log file is found
#         connection_made = False
#         #print("No log file found")
# except:
#     # This is what the script executes if a connection cannot be created to the DB
#     connection_made = False
#     #print ("Unable to connect to the database")


# # Now we have tried to send the logfile we need to send the actual application event 

# try:
#     # Here we are re-using the connection we made earlier in the script
#     # If this connection was not made earlier it will fail the try and move to the exception

#     cur.execute("INSERT INTO public.application_events (username, application, event_type, event_timestamp) VALUES (%s, %s, %s, %s)",(username_var, application_var, event_type_var, currenttime_var))
#     conn.commit()
#     # Inserting current values into the database
# except Exception, e:

#     # This part of the script runs if the values cannot be added to the database
#     # This adds the application event to the logfile 

#     #print("Unable to execute query")
#     #print(e)

#     entry = [[username_var, application_var, event_type_var, currenttime_var],]

#     # Creating a tuple (array) of the application event - effectively an array of objects (although this should never exceed one object)

#     try:

#         with open('log.txt','a') as logfile:
#             writer = csv.writer(logfile, delimiter=",")
#             writer.writerows(entry)

#             # Writing the unsent application events to a csv file with each line representing an application event

#         logfile.close()
#         # Closing log file to ensure that no other changes are saved and can be accessed by the script if required again
        
#         #print("Written to log file")
#     except:
#         connection_made = False
#         #print("Unable to write to log")

# # This checks if a connection was made and if it was it closes the connection (which conserves connection slots and also prevents intrusions)

# if connection_made:
#     try:

#         conn.close()
#     except:
#         connection_made = False
#         #print("Unable to Close Connection")
# else:
#     connection_made = False
#     #print("Connection not made")