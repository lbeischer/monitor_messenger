# Python script to re-write Alteryx User Settings file
#
# Lewis Beischer

# This section imports the required libraries for the XML settings change
import os
import xml.etree.ElementTree as ET

# Getting username of user
user_name = os.environ.get("USERNAME")

# Programmatically creating directory values for use in program
user_directory = "/Users/"+user_name+"/AppData/Roaming/Alteryx/Engine"
user_alteryx_temp_folder = "C:/Users/" +user_name+"/AppData/Local/Temp/"
user_alteryx_log_folder = "C:/Users/"+user_name+"/Documents/Alteryx Log"

# Looping through user directories for Alteryx engine (in case multiple engines)
for dir in os.listdir(user_directory):
    path_dir = os.path.join(user_directory, dir)
    # Only carrying forward directories (so not additional XML files)
    if os.path.isdir(path_dir):
        os.listdir(path_dir)
        # For all files in the Engine version directory
        for engine_xml in os.listdir(path_dir):
            if engine_xml == 'UserSettings.xml':
                # Only pull the file path and directory if the xml is called UserSettings
                user_settings_xml_path = user_directory+"/"+dir+"/"+engine_xml
                user_settings_output_xml_path = user_directory+"/"+dir+"/"

                # Parsing the user settings xml and getting the root and global settings elements
                user_settings_xml = ET.parse(user_settings_xml_path)
                xml_root = user_settings_xml.getroot()
                global_settings = xml_root.find('GloablSettings')

                # This section of the script checks to see if the variables we are trying to add
                # are already in the UserSettings xml. If not, it adds the appropriate values
                # to enable Alteryx Logging
                if global_settings.find('LogFilePath'):
                    print("has LogFilePath")
                else:
                    logfilepath = ET.SubElement(global_settings, 'LogFilePath')
                    logfilepath.text = user_alteryx_log_folder
                    logfilepath.set('NoInherit', 'True')

                if global_settings.findall('DefaultMemory'):
                    print("has DefaultMemory")
                else:
                    defaultmemory = ET.SubElement(global_settings, 'DefaultMemory')
                    defaultmemory.set('value', '2024')
                    defaultmemory.set('NoInherit', 'True')

                if global_settings.findall('RunAtLowerPriority'):
                    print("has RunAtLowerPriority")
                else:
                    runlowerpriority = ET.SubElement(global_settings, 'RunAtLowerPriority')
                    runlowerpriority.set('value', 'False')
                    runlowerpriority.set('NoInherit', 'True')

                if global_settings.findall('DefaultTempFilePath'):
                    print("has DefaultTempFilePath")
                else:
                    defaulttempfilepath = ET.SubElement(global_settings, 'DefaultTempFilePath')
                    defaulttempfilepath.text = user_alteryx_temp_folder
                    defaulttempfilepath.set('NoInherit', 'True')

                # This writes the updates xml back to the same directory
                user_settings_xml.write(user_settings_output_xml_path + "UserSettings.xml")