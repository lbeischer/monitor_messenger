import os
import xml.etree.ElementTree as ET

user_name = os.environ.get("USERNAME")

user_directory = "/Users/"+user_name+"/AppData/Roaming/Alteryx/Engine"
user_alteryx_temp_folder = "C:/Users/" +user_name+"/AppData/Local/Temp/"
user_alteryx_log_folder = "C:/Users/"+user_name+"/Documents/Alteryx Log"

os.listdir(user_directory)

for dir in os.listdir(user_directory):
    path_dir = os.path.join(user_directory, dir)
    if os.path.isdir(path_dir):
        os.listdir(path_dir)
        for engine_xml in os.listdir(path_dir):
            if engine_xml == 'UserSettings.xml':
                user_settings_xml_path = user_directory+"/"+dir+"/"+engine_xml

print(user_settings_xml_path)

user_settings_xml = ET.parse(user_settings_xml_path)
xml_root = user_settings_xml.getroot()
global_settings = xml_root.find('GloablSettings')

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

user_settings_xml.write('output_test.xml')

print(global_settings)