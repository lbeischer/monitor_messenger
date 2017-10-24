# Python script to write event to databases specified in config.ini
#
# Lewis Beischer
# 24/10/2017

# This section imports the required libraries for the messenger

import psycopg2 # This imports the Python to Postgresql library - this will need to be replaced if sending to a different DB
import os, time, datetime, csv, sys # Importing system packages
import configparser # Importing config parser


username_var = os.environ.get("USERNAME")
currenttime_var = datetime.datetime.now()

# Setting global variables - the current username according to the OS (likely to be windows) and the date time from the system


# Below are mnaully set variables for each version of the executable
# These take the first two arguments after the executable i.e. app_messenger.exe -Start -Alteryx
# This allows the executable to run for all different types of application event with the arguments the only change

event_type_var = str(sys.argv[2])
application_var = str(sys.argv[1])

connection_made = False # Setting connection made to false - the program will then always log unless a successful connection is made

# The script is designed to log application events (as specified by the user)
# If it can't connect to the database it saves these events to a log file
# If it is able to connect to the database it first tries to send the log file of application events
# It then tries to send the current application event

try:
	conn = psycopg2.connect("dbname='appmonitoring' user='appmonitor_agent' host='EUPOSTGRESQL01' port='5432' password='lek'")
	# These are the connection details to the London LEK Database for app monitoring - i.e. where it sends the events
	# This should be replaced with the local equivalents where possible and assigned a separate user (for security reasons)
	connection_made = True
	cur = conn.cursor()

	# Here we generate a connection object (conn) using the psycopg2 module
	# If unsuccessful the script skips to the except section 
	# If successful it sets the connection_made variable to true (used in logic later)
	# Then we set a cursor
	
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

				except Exception, e:

					#print("Unable to execute query")
					#print(e)
					logfile.close()
					# If unable to send all the logfile we close the log file so that we don't delete these events
					# The file is also not removed here so at the next application event it tries to send again
		except:

			# Exception if unable to open the log file
			print("Unable to open file")

	else:
		# This is where the script will go if no log file is found
		connection_made = False
		#print("No log file found")
except:
	# This is what the script executes if a connection cannot be created to the DB
	connection_made = False
	#print ("Unable to connect to the database")


# Now we have tried to send the logfile we need to send the actual application event 

try:
	# Here we are re-using the connection we made earlier in the script
	# If this connection was not made earlier it will fail the try and move to the exception

	cur.execute("INSERT INTO public.application_events (username, application, event_type, event_timestamp) VALUES (%s, %s, %s, %s)",(username_var, application_var, event_type_var, currenttime_var))
	conn.commit()
	# Inserting current values into the database
except Exception, e:

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
		connection_made = False
		#print("Unable to write to log")

# This checks if a connection was made and if it was it closes the connection (which conserves connection slots and also prevents intrusions)

if connection_made:
	try:

		conn.close()
	except:
		connection_made = False
		#print("Unable to Close Connection")
else:
	connection_made = False
	#print("Connection not made")