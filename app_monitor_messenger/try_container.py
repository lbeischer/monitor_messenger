import re
import xml.etree.ElementTree as ET


path_to_procurement = "X:\Enabling Tools\Procurement synergy\Analysis\Procurement synergy_v06.yxmd"
macro_tool_list = []
alteryx_tool_list = []


workflow_path = path_to_procurement

# Setup regex for tool parsing and macro parsing
tool_parse_regex = re.compile('\w+$')
macro_parse_regex = re.compile('.+?(?=.yxmc)')

#Try and parse yxmd file with XML
try:
    workflow_xml = ET.parse(workflow_path)

    # Intialising tool lists for each workflow
    workflow_alteryx_tool_list = []
    workflow_macro_tool_list = []

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
            workflow_alteryx_tool_list.append(tool_name)
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

except:
    print("Log file not accessible")

total_tool_list = macro_tool_list + alteryx_tool_list
print(total_tool_list)

unique_list = list(set(total_tool_list))
print(unique_list)