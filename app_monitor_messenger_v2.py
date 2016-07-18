# Python script to write event to PostgreSQL database
#
# Lewis Beischer


import psycopg2
import os
import time
import datetime
import csv
import os.path
import sys


username_var = os.environ.get("USERNAME")
currenttime_var = datetime.datetime.now()

# These grab the windows username and current datetime.
# Below are mnaully set variables for each version of the executable
print("testing")
event_type_var = str(sys.argv[2])
application_var = str(sys.argv[1])

connection_made = False

# Will try to connect to the Postgres Database

try:
	conn = psycopg2.connect("dbname='appmonitoring' user='appmonitor_agent' host='EUPOSTGRESQL01' port='5432' password='lek'")
	connection_made = True
	cur = conn.cursor()

	# If successfully connected then send the logfile if exists
	# The logfile should be populated with all events that we haven't been able to log whilst unable to connect to the DB

	if os.path.isfile('log.txt'):
		#print("Log file found, sending to database")

		try:
			with open('log.txt', 'rb') as logfile:
				reader = csv.reader(logfile, delimiter=",")
				# Reading the text file as a csv and saving as an object of tuples
				try:

					for row in reader:
						# Iterating through rows and inserting them into database
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
		except:

			#print("Unable to open file")

	else:

		#print("No log file found")
except:

	#print ("Unable to connect to the database")


# Now we have sent the logfile we need to send the live 

try:

	cur.execute("INSERT INTO public.application_events (username, application, event_type, event_timestamp) VALUES (%s, %s, %s, %s)",(username_var, application_var, event_type_var, currenttime_var))
	conn.commit()
	# Inserting current values into the database
except Exception, e:

	#print("Unable to execute query")
	#print(e)
	entry = [[username_var, application_var, event_type_var, currenttime_var],]
	# Setting a tuple of objects
	try:

		with open('log.txt','a') as logfile:
			writer = csv.writer(logfile, delimiter=",")
			writer.writerows(entry)
		# Writing to a log file if the script fails to insert into postgres
		logfile.close()
		#print("Written to log file")
	except:

		#print("Unable to write to log")


if connection_made:
	try:

		conn.close()
	except:

		#print("Unable to Close Connection")
else:

	#print("Connection not made")