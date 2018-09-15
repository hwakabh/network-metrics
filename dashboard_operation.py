import requests
import json
import simplejson
import pprint
import copy
import random
import sys
import csv
import StringIO
import threading
import datetime

from pysnmp.hlapi import *
from pysnmp.entity.rfc3413.oneliner import cmdgen

#from influxdb import InfluxDBClient
import configurations as cf


def walk(switch, oid):
    retstr = ""
    cmdGen = cmdgen.CommandGenerator()

    errorIndication, errorStatus, errorIndex, varBinds = cmdGen.nextCmd(
        cmdgen.CommunityData(cf.snmp_community),
        cmdgen.UdpTransportTarget((switch, 161)),
        oid
    )

    if errorIndication:
        print(errorIndication)
    elif errorStatus:
        print('%s at %s' % (errorStatus.prettyPrint(), errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
    else:
        for varBind in varBinds:
            retstr += ' = '.join([x.prettyPrint() for x in varBind])
            retstr += "\n"
    return retstr


def find_each_port(region, location, hostname, ipaddress, stype, oidList, measurement):
    test_sw_port_list = []
    port_description_dict = {}

    for oid in oidList:
        s = StringIO.StringIO(walk(ipaddress, oid))
        for line in s:
            portNum = line.split(" = ")[0].split(".")[-1]
            port_description = line.split(" = ")[1]

            test_sw_port_list = test_sw_port_list + [portNum]
            port_description_dict[portNum] = port_description

    return test_sw_port_list, port_description_dict


def get_ports_from_switch(switches):
    oidList = [".1.3.6.1.2.1.2.2.1.2"]
    measurementList = cf.measurementList

    test_sw_port_dict = {}
    port_description_dict = {}
    for index in range(len(switches)):
        test_sw_port_dict[switches[index].hostname] = []
        test_sw_port_dict[switches[index].hostname], port_description_dict[switches[index].hostname] = find_each_port(switches[index].region, switches[index].location, switches[index].hostname, switches[index].ipaddress, switches[index].stype, oidList, measurementList)
        test_sw_port_dict[switches[index].hostname] = sorted(list(set(test_sw_port_dict[switches[index].hostname])))

    return test_sw_port_dict, port_description_dict


def from_csv_find_each_port(csv_file_path):
    test_sw_port_dict = {}
    port_description_dict = {}
    port_name_dict = {}

    with open(csv_file_path , 'rb',) as port_csvfile:
        f = csv.reader(port_csvfile, delimiter=',')
        next(f)
        for row in f:
            if row[1].split(" = ")[0].split(".")[-2] == str(18):
                portNum = row[1].split(" = ")[0].split(".")[-1]
                port_description = row[1].split(" = ")[1]

                if row[0] not in test_sw_port_dict:
                    test_sw_port_dict[row[0]] = []
                test_sw_port_dict[row[0]] = test_sw_port_dict[row[0]] + [portNum]

                if row[0] not in port_description_dict:
                    port_description_dict[row[0]] = {}
                port_description_dict[row[0]][portNum] = port_description

            if row[1].split(" = ")[0].split(".")[-2] == str(1):
                portNum = row[1].split(" = ")[0].split(".")[-1]
                port_name = row[1].split(" = ")[1]

                if row[0] not in port_name_dict:
                    port_name_dict[row[0]] = {}
                port_name_dict[row[0]][portNum] = port_name

    return test_sw_port_dict, port_description_dict, port_name_dict


def str_hook(obj):
    return {k.encode('utf-8') if isinstance(k,unicode) else k :
            v.encode('utf-8') if isinstance(v, unicode) else v for k,v in obj}


def get_arguments(arguments):
    params = dict()
    for arg in arguments:
        if "=" in arg:
            params.update(dict([arg.split("=")]))
    return params


def track_path_to_key(input_json, target_key, update_value, path_tracker = None ,  last_key = None):
    if path_tracker is None:
        path_tracker = []

    if last_key is not None:
        path_tracker = list(path_tracker + [last_key])

    if type(input_json) is dict and input_json:
        for key in input_json:
            # path_tracker = path_tracker + [key]
            if key == target_key:
                input_json[key] = update_value

            if type(input_json[key]) is dict or type(input_json[key]) is list:
                track_path_to_key(input_json[key], target_key, update_value, path_tracker, key)
    elif type(input_json) is list and input_json:
        for entity in input_json:
            track_path_to_key(entity, target_key, update_value, path_tracker, input_json.index(entity))
    else:
        if path_tracker: #If path_tracker is not empty
            path_tracker.pop()


def update_panel_json(input_json, target_key, update_value, target_value=None):
    if type(input_json) is dict and input_json:
        for key in input_json:
            if key == target_key:
                if target_value is None:
                    input_json[key] = update_value
                else:
                    if input_json[key] == target_value:
                        input_json[key] = update_value

            if type(input_json[key]) is dict or type(input_json[key]) is list:
                update_panel_json(input_json[key], target_key, update_value)

    elif type(input_json) is list and input_json:
        for entity in input_json:
            update_panel_json(entity, target_key, update_value)


def duplicate_panels(original_panel, tag_dict_list, where_key):
    target_panel = []
    temp_panel = copy.deepcopy(original_panel)

    for tag_key in tag_dict_list.keys():
        if tag_key == where_key:
            for tag_value in tag_dict_list[tag_key]:
                for query in range (len(original_panel["targets"])):
                    for count in range (len(original_panel["targets"][query]["tags"])):
                        if original_panel["targets"][query]["tags"][count]["key"] == tag_key:
                            #if original_panel["targets"][query]["tags"][count]["value"] != tag_value:
                            temp_panel["targets"][query]["tags"][count]["value"] = tag_value
                            temp_panel["id"] = random.randint(1, 9999999999999999)
                            if query == len(original_panel["targets"])-1:
                                temp_panel["title"] = tag_value
                                target_panel = target_panel + [copy.deepcopy(temp_panel)]
    return target_panel


def duplicate_panels_for_ports(original_panel, ports_per_switch, port_description_per_switch, port_name_per_switch, where_key, target_switch):
    target_panel = []
    temp_panel = copy.deepcopy(original_panel)

    for tag_key in ports_per_switch.keys():
        if tag_key == target_switch:
            for tag_value in ports_per_switch[tag_key]:
                for query in range (len(original_panel["targets"])):
                    for count in range (len(original_panel["targets"][query]["tags"])):
                        if original_panel["targets"][query]["tags"][count]["key"] == "switch_name":
                            temp_panel["targets"][query]["tags"][count]["value"] = target_switch

                        if original_panel["targets"][query]["tags"][count]["key"] == where_key: #and this_switch:
                            temp_panel["targets"][query]["tags"][count]["value"] = tag_value
                            temp_panel["id"] = random.randint(1, 9999999999999999)
                            if port_name_per_switch:
                                temp_panel["title"] = port_name_per_switch[tag_key][tag_value]
                            else:
                                temp_panel["title"] = "Port " + tag_value
                            #target_panel = target_panel + [copy.deepcopy(temp_panel)]
                            if query == len(original_panel["targets"])-1:
                                temp_panel["description"] = port_description_per_switch[tag_key][tag_value]
                                #print "description ", tag_key, tag_value, port_description_per_switch[tag_key][tag_value]
                                target_panel = target_panel + [copy.deepcopy(temp_panel)]

    return target_panel


def duplicate_dashboard(input_json, replace_target, replace_tag):
    if type(input_json) is dict and input_json:
        for key in input_json:
            if input_json[key] == replace_target:
                input_json[key] = replace_tag

            if type(input_json[key]) is dict or type(input_json[key]) is list:
                duplicate_dashboard(input_json[key], replace_target, replace_tag)

    elif type(input_json) is list and input_json:
        for entity in input_json:
            if entity == replace_target:
                entity = replace_tag
            duplicate_dashboard(entity, replace_target, replace_tag)


def duplicate_dashboard_by_replacing_string(input_json_path, replace_target, replace_tag):
    with open(input_json_path, 'r+') as file:
        content = file.read()
        content = content.replace(replace_target, replace_tag)
        return content


class Switch:
    def __init__(self,region,location,hostname,ipaddress,stype):
        self.region = region
        self.location = location
        self.hostname = hostname
        self.ipaddress = ipaddress
        self.stype = stype


if __name__ == '__main__':

#Load config info from csv
    switches = []
    config_file_name = cf.config_file_name
    f = open(config_file_name)

    with open(config_file_name , 'rb',) as csvfile:
        f = csv.reader(csvfile, delimiter=',')
        next(f, None)
        for row in f:
            switchinfo = Switch(row[0],row[1],row[2],row[4],row[5])
            switches.append(switchinfo)

    tag_dict_list = {}
    region_list=[]
    location_list=[]
    hostname_list=[]
    ipaddress_list=[]

    for i in range(0, len(switches)):
        region_list = region_list + [switches[i].region]
        location_list = location_list + [switches[i].location]
        hostname_list = hostname_list + [switches[i].hostname]
        ipaddress_list = ipaddress_list + [switches[i].ipaddress]

    tag_dict_list["region"] = region_list
    tag_dict_list["location"] = location_list
    tag_dict_list["switch_name"] = hostname_list
    tag_dict_list["ipaddress"] = ipaddress_list

# Load variables from arguments
    command_params_dict = get_arguments(sys.argv)

    # Only options listed here are allowed to use.
    option_list = ["command", "json", "dashboard", "target_row", "where_key", "target_switch", "target_key", "target_value", "update_value", "replace_target", "replace_with", "csv", "send_snmp"]
    command_not_allowed = []
    for check_command in command_params_dict.keys():
        if check_command not in option_list:
            command_not_allowed = command_not_allowed + [check_command]
    if len(command_not_allowed) > 0:
        print "You set " + str(len(command_not_allowed)) + " illegal option(s): " + str(command_not_allowed)
        print "Avaliable option list: " + str(option_list)
        print "To show detailed explanation, execute -> python dashboard_operation.py help"
        exit()

    command = command_params_dict.get("command")
    if command == "update" or command == "copy" or command == "dashboard_copy":
        pass
    else:
        print "You can use the following commands. \n"
        print "Case 1: command=copy. \n" \
              "You need to set the following options. \n" \
              "json=path_to_panel_json \n" \
              "dashboard=dashboard_name_to_configure  \n" \
              "target_row=row_to_which_duplicated_panels_are_added \n" \
              "where_key=key_by_which_panels_are_duplicated\n" \
              "    *** If you set port_number as where_key, you need to set target_switch.\n" \
              "        target_switch=switch_in_which_all_port_panels_are_duplicated.  \n" \
              "    *** target_switch name must be listed in the following csv. \n" \
              "        -> " , config_file_name, \
              "\n\n" \
              "Command example -> python /this/python command=copy where_key=switch_name json=/path/to/panel/json dashboard=dashboard_name target_row=row_name \n" \
              "Command example -> python /this/python command=copy where_key=port_number target_switch=switch_name_to_duplicate_port_panel json=/path/to/panel/json dashboard=dashboard_name target_row=row_name \n"

        print "Case 2: command=update. \n" \
              "You need to set the following options. \n" \
              "target_key=key_in_panel_to_be_updated \n" \
              "update_value=new_value_assigned_to_target_key \n" \
              "    *** This is Optional *** \n " \
              "    You can also pass update value as a json file. \n" \
              "    In this case, set update_value=json \n" \
              "    Then, set json=/json/path \n" \
              "    In the json file, you can also define update value as list type format such as [1, 2, 3]. \n" \
              "    To know a correct json format for a certain key, see panel json on grafana dashboard. \n" \
              "    Panels will be broken if unsupported format value is assigned as update value. \n " \
              "\n " \
              "    *** This is Optional *** \n " \
              "    You can also update target_key which has a specific value in case there are some target_keys which have a common name. \n" \
              "    In this case, you need to set target_value=some_value. \n" \
              "    Then, only target_key which has target_value will be updated with update_value.\n" \
              "dashboard=dashboard_name_to_configure  \n" \
              "target_row=row_in_which_panels_to_update_exist \n" \
              "Note that all rows will be updated if you set nothing to target_row. \n" \
              "\n" \
              "Command example -> python /this/python command=update target_key=target_key update_value=some_value dashboard=dashboard_name target_row=row_name \n" \
              "Command example -> python /this/python command=update target_key=target_key update_value=json json=/panel/config/json/path dashboard=dashboard_name target_row=row_name \n" \
              "Command example -> python /this/python command=update target_key=target_key target_value=some_value_to_change update_value=some_value dashboard=dashboard_name target_row=row_name \n"

        print "Case 3: command=dashboard_copy. \n" \
              "You need to set the following options. \n" \
              "json=path_to_dashboard_template_json \n" \
              "replace_target=strings_which_you_want_replace\n" \
              "replace_with=column_name_in_csv_by_which_dashboards_will_be_duplicated\n" \


        print "Exit."
        exit()

    # Show command parameters before executing this .py
    print "\nYou are going to send the following command parameters.\n" \
          "You can NOT undo the operation.\n" \
          "So it is recommended to test on a temporary dashboard first.\n"

    for key in sorted(command_params_dict.keys()):
        print key, ": ", command_params_dict.get(key)

    print "\n"
    print("Press Enter if it is okay to proceed...")
    raw_input("If you are not sure, execute -> python dashboard_operation help")
    print "Process started..."

    json_path = command_params_dict.get("json")
    csv_path = command_params_dict.get("csv")
    if csv_path is None:
        csv_path = "./sample_port_list.csv"

    send_snmp = command_params_dict.get("send_snmp") #This .py will send snmp to switches if this is enabled.
    where_key = command_params_dict.get("where_key")
    target_switch = command_params_dict.get("target_switch")
    target_key = command_params_dict.get("target_key")
    target_value = command_params_dict.get("target_value")
    update_value = command_params_dict.get("update_value")
    replace_target = command_params_dict.get("replace_target")
    replace_with = command_params_dict.get("replace_with")

    # sys.argv load values as string. So, convert True/False into boolean.
    if update_value is not None:
        if update_value.lower() == "true":
            update_value = True
        elif update_value.lower() == "false":
            update_value = False

    if update_value == "json":
        with open(json_path, 'r') as update_value_as_json:
            update_value = json.load(update_value_as_json)

    row_name = command_params_dict.get("target_row")
    dashboard_name = command_params_dict.get("dashboard")

    if command != "dashboard_copy" and dashboard_name is None:
        print "Dashboard: ", dashboard_name
        print "Your dashboard name is missing. Set parameter: dashboard=dashboard_name_to_configure"
        print "Exit."
        exit()
    else:
        dashboard_name = dashboard_name.replace(" ", "-")  # For handling white space.
        dashboard_name = dashboard_name.replace(".", "-")  # For handling period.

    #Set true if you want to test.
    copy_port_test = False
    if copy_port_test:
        command = "dashboard_copy"
        replace_target = "target_name"
        replace_with = "switch_name"
        json_path="./sample.json"

    #######Set Parameters Above

#Get dashboard data as json
#This value should be defined in the config file
    grafana_base_url = "http://localhost:3000/api/dashboards/db/"

    if command == "dashboard_copy":
        try:
            print json_path
            with open(json_path, "r") as json_data:
                try:
                    if replace_target is None or replace_with is None:
                        "You need to set both replace_target and replace_with when you run commnad=dashboard_copy.\n"
                        "    replace_target=strings_which_you_want_replace\n" \
                        "    replace_with=strings_with_which_target_is_replaced"
                        exit()
                    dashboard_json_template = simplejson.load(json_data)

                    post_dashboard = {}
                    for each_replace_tag in tag_dict_list[replace_with]:
                        temp_json_string = duplicate_dashboard_by_replacing_string(json_path , replace_target, each_replace_tag)
                        temp_dashboard_json_template = simplejson.loads(temp_json_string)
                        temp_dashboard_json_template["title"] = "copied_" + str(each_replace_tag)
                        temp_dashboard_json_template["id"] = None
                        post_dashboard["dashboard"] = temp_dashboard_json_template
                        post_dashboard["overwrite"] = True

                        r = requests.post(grafana_base_url, json=post_dashboard, auth=("admin", "admin"), headers={"Content-Type": "application/json"})
                        print r.text

                except ValueError:
                    print "Failed to load json..."

        except IOError as e:
            print e

        exit()

#Get dashboard json from API
    headers = {"Content-Type": "application/json"}
    r = requests.get(grafana_base_url + dashboard_name, auth=("admin", "admin"))
    response_json = r.text
    dashboard_json = json.loads(response_json, object_pairs_hook=str_hook)
    print "Get response code: ", r.status_code

#Duplicate a panel.
    if command == "copy":
        if json_path is not None:
            #Read panel data as json which is used as the orignal of copies
            try:
                with open(json_path, "r") as json_data:
                    try:
                        panel_json = json.load(json_data)

                        ports_per_switch = {}
                        port_description_per_switch = {}
                        port_name_per_switch = {}
                        if where_key == "port_number": #In case for duplicating panels for each ports
                            if send_snmp == "enabled":
                                ports_per_switch, port_description_per_switch = get_ports_from_switch(switches)
                            else:
                                ports_per_switch, port_description_per_switch, port_name_per_switch = from_csv_find_each_port(csv_path)

                        if row_name is not None:
                            for dashboard_row_index in range(0, len(dashboard_json["dashboard"]["rows"])):
                                if row_name == dashboard_json["dashboard"]["rows"][dashboard_row_index]["title"]:
                                    if where_key == "port_number":
                                        merged_list = dashboard_json["dashboard"]["rows"][dashboard_row_index]["panels"] + duplicate_panels_for_ports(panel_json, ports_per_switch, port_description_per_switch, port_name_per_switch, where_key, target_switch)
                                    else:
                                        merged_list = dashboard_json["dashboard"]["rows"][dashboard_row_index]["panels"] + duplicate_panels(panel_json, tag_dict_list, where_key)
                                    dashboard_json["dashboard"]["rows"][dashboard_row_index]["panels"] = merged_list
                        else:
                            if where_key == "port_number":
                                merged_list = dashboard_json["dashboard"]["rows"][0]["panels"] + duplicate_panels_for_ports(panel_json, ports_per_switch, port_description_per_switch, port_name_per_switch, where_key, target_switch)
                            else:
                                merged_list = dashboard_json["dashboard"]["rows"][0]["panels"] + duplicate_panels(panel_json, tag_dict_list, where_key)

                            dashboard_json["dashboard"]["rows"][0]["panels"] = merged_list
                        dashboard_json["overwrite"] = True
                        r = requests.post(grafana_base_url, json=dashboard_json, auth=("admin", "admin"), headers=headers)
                        print r.text

                    except ValueError:
                        print "Failed to load json"

            except IOError as e:
                print e

        if json_path is None:
            print "You should specify json path with parameter: json=path_to_panel_json"

#Specify a key, which should be unique in the panel json, then its value is updated.
    if command == "update":
        if update_value is not None:
            if row_name is not None:
                for dashboard_row_index in range(0, len(dashboard_json["dashboard"]["rows"])):
                    if row_name == dashboard_json["dashboard"]["rows"][dashboard_row_index]["title"]:
                        for each_panel in dashboard_json["dashboard"]["rows"][dashboard_row_index]["panels"]:
                            update_panel_json(each_panel, target_key, update_value, target_value)

            else:
                for dashboard_row_index in range(0, len(dashboard_json["dashboard"]["rows"])):
                    for each_panel in dashboard_json["dashboard"]["rows"][dashboard_row_index]["panels"]:
                        update_panel_json(each_panel, target_key, update_value, target_value)

            dashboard_json["overwrite"] = True
            r = requests.post(grafana_base_url, json=dashboard_json, auth=("admin", "admin"), headers=headers)
            print "Post response json: ", r.text

        else:
            print "Set update_value."
