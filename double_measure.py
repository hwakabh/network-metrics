import json
import csv
import datetime
import StringIO
import threading

from pysnmp.entity.rfc3413.oneliner import cmdgen
from influxdb import InfluxDBClient

import configurations as cf


def walk(switch, oid, cmdGen):
    retstr = ""
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



def getValues(measurement, hostname, portNum, valueNow, client, day):
    ## Calc byte/sec
    query = "select value from " + measurement + " where switch_name = '" + hostname + "' and port_number = '" + portNum +  "' and time > (now() - 15m) order by time desc limit 1"
    result = client.query(query).raw
    c = 0
    valueSum = 0
    if 'series' in result:
        for v in result['series'][0]['values']:
            valueSum += int(v[1])
            c += 1
    if (c == 0):
        valueByteSec = 0
    else:
        valueByteSec = (valueNow - valueSum)/300


    ## Calc valueAvg
    query = "select value_ByteSec from " + measurement + " where switch_name = '" + hostname + "' and port_number = '" + portNum +  "' and time > (now() - " + day + "d - 1h ) and time < (now() - " + day + "d) order by time desc limit 12"
    result = client.query(query).raw
    c = 0
    valueSum = 0
    if 'series' in result:
        for v in result['series'][0]['values']:
            valueSum += int(v[1])
            c += 1
    if (c == 0):
        valueAvg = 0
    else:
        valueAvg = valueSum / c    # type(valueAvg) => int | long


    ## Calc valueDiff
    valueDiff = valueAvg - valueByteSec


    ## Calc valueRate
    if (valueAvg != 0):
        valueRate = float(valueByteSec) / float(valueAvg)
    else:
        valueRate = 0.0

    ## Calc valueBf
    query = "select value from " + measurement + " where switch_name = '" + hostname + "' and port_number = '" + portNum +  "' and time > (now() - " + day + "d - 1h ) and time < (now() - " + day + "d) order by time desc limit 1"
    result = client.query(query).raw
    c = 0
    valueSum = 0
    if 'series' in result:
        for v in result['series'][0]['values']:
            valueSum += int(v[1])
            c += 1
    if (c == 0):
        valueBf = 0
    else:
        valueBf = valueSum / c

    ## Calc valueBf byte/sec
    query = "select value from " + measurement + " where switch_name = '" + hostname + "' and port_number = '" + portNum +  "' and time > (now() - " + day + "d - 1h ) and time < (now() - " + day + "d) order by time desc limit 2"
    result = client.query(query).raw
    c = 0
    valueSum = 0
    if 'series' in result:
        for v in result['series'][0]['values']:
            if c > 0 :
                valueSum += int(v[1])
            c += 1
    if (c == 0):
        valueBfByteSec = 0
    else:
        valueBfByteSec = (valueBf - valueSum)/300

    return Values(valueNow, valueAvg, valueDiff, valueRate, valueBf, valueByteSec, valueBfByteSec)


def print_json(region, location, hostname, vendor, ipaddress, stype, oidList, measurement, cmdGen):
    client = InfluxDBClient(\
                            cf.influx_ip,\
                            cf.influx_port,\
                            cf.influx_user,\
                            cf.influx_pass,\
                            cf.influx_dbname\
                            )
    count = 0
    for oid in oidList:
        s = StringIO.StringIO(walk(ipaddress, oid, cmdGen))
        
        for line in s:
            portNum = line.split(" = ")[0].split(".")[-1]
            portValue = line.split(" = ")[1]
            # for real-time data
            valueNow = int(portValue[:-1]) - (2**63 - 1)   # type(valueAvg) => int | long


            values_c = getValues(measurement[count], hostname, portNum, valueNow, client, "7")
            values_6 = getValues(measurement[count], hostname, portNum, valueNow, client, "6")


            isvlan = False
            if vendor == 'Arista' and int(portNum) > 100:
                isvlan = True

            fields_dict = {\
                           'value': valueNow,\
                           'value_ByteSec': values_c.valueByteSec,\
                           'value_Bf': values_c.valueBf,\
                           'value_Avg': values_c.valueAvg,\
                           'value_Diff': values_c.valueDiff,\
                           'value_Rate': values_c.valueRate,\
                           'value_Bf_ByteSec': values_c.valueBfByteSec,\
                           'value_6bf': values_6.valueBf,\
                           'value_6bf_Avg': values_6.valueAvg,\
                           'value_6bf_Diff': values_6.valueDiff,\
                           'value_6bf_rate': values_6.valueRate,\
                           'value_6bf_ByteSec': values_c.valueBfByteSec,\
                           }
            tags_dict = {\
                         'region' : region,\
                         'location' : location,\
                         'switch_address': ipaddress,\
                         'is_vlan' : isvlan, \
                         'switch_name': hostname,\
                         'switch_type' : stype,\
                         'port_number': portNum}
            main_dict = {\
                         'points' : [\
                                     {'measurement': measurement[count],\
                                      'time': str(datetime.datetime.now()),
                                      'tags': tags_dict,\
                                      'fields': fields_dict}
                                     ]
                         }
            json_str = json.dumps(main_dict, sort_keys=True, indent=4, separators=(',', ': '))

            pyjson = json.loads(json_str)   
            client.write(pyjson, params={'db': cf.influx_dbname})
        count += 1


class myThread (threading.Thread):
    def __init__(self,switches, oid, measurement):
        threading.Thread.__init__(self)
        self.region = switches.region
        self.location = switches.location
        self.hostname = switches.hostname
        self.vendor = switches.vendor
        self.ipaddress = switches.ipaddress
        self.stype = switches.stype
        self.oid = oid
        self.measurement = measurement
        self.cmdGen = cmdgen.CommandGenerator()
    def run(self):
        print str(datetime.datetime.now()) + " : Starting " + self.hostname + " thread\n"
        print_json(self.region, self.location, self.hostname ,self.vendor, self.ipaddress, self.stype, self.oid, self.measurement, self.cmdGen)
        print str(datetime.datetime.now()) + " : Exiting " + self.hostname + " thread\n"

class Switch:
    def __init__(self,region,location,hostname,vendor,ipaddress,stype):
        self.hostname = hostname
        self.vendor = vendor
        self.ipaddress = ipaddress
        self.region = region
        self.location = location
        self.stype = stype

class Values:
    def __init__(self,valueNow, valueAvg, valueDiff, valueRate, valueBf, valueByteSec, valueBfByteSec):
        self.valueNow = valueNow
        self.valueAvg = valueAvg
        self.valueDiff = valueDiff
        self.valueRate = valueRate
        self.valueBf = valueBf
        self.valueByteSec = valueByteSec
        self.valueBfByteSec = valueBfByteSec


if __name__ == '__main__':
    print str(datetime.datetime.now()) + " : Starting Main Thread."
    oidList = cf.oidList
    measurementList = cf.measurementList

    # Read csv
    switches = []
    with open(cf.config_file_name, 'rb',) as csvfile:
        f = csv.reader(csvfile, delimiter=',')
        next(f, None)
        for row in f:
            switchinfo = Switch(row[0],row[1],row[2],row[3],row[4],row[5])
            switches.append(switchinfo)
    csvfile.close()

    threads = []
    threadID = 0
    # Create new threads
    for tName in switches:
                thread = myThread(switches[threadID], oidList, measurementList)
                thread.start()
                threads.append(thread)
                threadID += 1
    # Wait for all threads to complete
    for t in threads:
        t.join()
    print str(datetime.datetime.now()) + " : Exiting Main Thread"

