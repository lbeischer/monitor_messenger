import os
import xml.etree.ElementTree as ET

user_name = os.environ.get("USERNAME")

user_directory = "/Users/"+user_name+"/AppData/Roaming/Alteryx/Engine"

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
if global_settings.findall('LogFilePath'):
    print("has LogFilePath")
if global_settings.findall('DefaultMemory'):
    print("has DefaultMemory")
if global_settings.findall('RunAtLowerPriority'):
    print("has RunAtLowerPriority")
if global_settings.findall('DefaultTempFilePath'):
    print("has DefaultTempFilePath")



print(global_settings)