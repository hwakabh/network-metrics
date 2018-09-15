import sys
import json
import time
from influxdb import InfluxDBClient

influx = 'test_influxdb'
inport = 8086
org_db = 'source_influx_db'
tgt_db = 'target_influx_db'
table_name = "test_table"


class Point:
    def __init__(self,
                 client,
                 measurement, 
                 time,  # time
                 input,  # input
                 input_avg_5,  # input_avg_5
                 input_avg_6,  # input_avg_6
                 input_avg_7,  # input_avg_7
                 input_diff_5,  # input_diff_5
                 input_diff_6,  # input_diff_6
                 input_diff_7,  # input_diff_7
                 input_rates_5,  # input_rates_5
                 input_rates_6,  # input_rates_6
                 input_rates_7,  # input_rates_7
                 input_value_5,  # input_value_5
                 input_value_6,  # input_value_6
                 input_value_7,  # input_value_7
                 infra_version,  # infra_version
                 output,  # output
                 output_avg_5,  # output_avg_5
                 output_avg_6,  # output_avg_6
                 output_avg_7,  # output_avg_7
                 output_diff_5,  # output_diff_5
                 output_diff_6,  # output_diff_6
                 output_diff_7,  # output_diff_7
                 output_rates_5,  # output_rates_5
                 output_rates_6,  # output_rates_6
                 output_rates_7,  # output_rates_7
                 output_value_5,  # output_value_5
                 output_value_6,  # output_value_6
                 output_value_7,  # output_value_7
                 vr_global_ip  # vr_global_ip
                 ):


        self.client = client
        self.measurement = measurement
        self.time = time
        self.input = input
        self.output = output
        self.vr_global_ip = vr_global_ip
        self.infra_version = infra_version
        self.input_avg_5 = input_avg_5
        self.input_avg_6 = input_avg_6
        self.input_avg_7 = input_avg_7
        self.input_diff_5 = input_diff_5
        self.input_diff_6 = input_diff_6
        self.input_diff_7 = input_diff_7
        self.input_rates_5 = input_rates_5
        self.input_rates_6 = input_rates_6
        self.input_rates_7 = input_rates_7
        self.input_value_5 = input_value_5
        self.input_value_6 = input_value_6
        self.input_value_7 = input_value_7
        self.output_avg_5 = output_avg_5
        self.output_avg_6 = output_avg_6
        self.output_avg_7 = output_avg_7
        self.output_diff_5 = output_diff_5
        self.output_diff_6 = output_diff_6
        self.output_diff_7 = output_diff_7
        self.output_rates_5 = output_rates_5
        self.output_rates_6 = output_rates_6
        self.output_rates_7 = output_rates_7
        self.output_value_5 = output_value_5
        self.output_value_6 = output_value_6
        self.output_value_7 = output_value_7

        # self.value_6bf = 0
        # self.value_6bf_Avg = 0
        # self.value_6bf_ByteSec = 0
        # self.value_6bf_Diff = 0
        # self.value_6bf_rate = 0.0
        # self.value_Avg = 0
        # self.value_Bf = 0
        # self.value_Bf_ByteSec = 0

        #self.value_ByteSec_input, self.value_ByteSec_output = self.get_ByteSec(self.input, "0")
        self.value_ByteSec_input= self.get_ByteSec(self.input, "0", "input")
        self.value_ByteSec_output = self.get_ByteSec(self.output, "0", "output")

        # print "self in out ", self.value_ByteSec_input, self.value_ByteSec_output
        # self.value_Diff = 0
        # self.value_Rate = 0.0

    def get_json(self):
        fields_dict = {
            'input': self.input, #self.value_Rate,
            'output': self.output, #self.value_Bf_ByteSec,
            'input_bps': float(self.value_ByteSec_input), #self.value_Rate,
            'output_bps': float(self.value_ByteSec_output), #self.value_Bf_ByteSec

            'input_avg_5': float(self.input_avg_5),
            'input_avg_6': float(self.input_avg_6),
            'input_avg_7': float(self.input_avg_7),
            'input_diff_5': float(self.input_diff_5),
            'input_diff_6': float(self.input_diff_6),
            'input_diff_7': float(self.input_diff_7),
            'input_rates_5': float(self.input_rates_5),
            'input_rates_6': float(self.input_rates_6),
            'input_rates_7': float(self.input_rates_7),
            'input_value_5': float(self.input_value_5),
            'input_value_6': float(self.input_value_6),
            'input_value_7': float(self.input_value_7),
            'output_avg_5': float(self.output_avg_5),
            'output_avg_6': float(self.output_avg_6),
            'output_avg_7': float(self.output_avg_7),
            'output_diff_5': float(self.output_diff_5),
            'output_diff_6': float(self.output_diff_6),
            'output_diff_7': float(self.output_diff_7),
            'output_rates_5': float(self.output_rates_5),
            'output_rates_6': float(self.output_rates_6),
            'output_rates_7': float(self.output_rates_7),
            'output_value_5': self.output_value_5,
            'output_value_6': self.output_value_6,
            'output_value_7': self.output_value_7

            # 'value_6bf ': self.value_6bf,
            # 'value_6bf_Avg ': self.value_6bf_Avg,
            # 'value_6bf_ByteSec ': self.value_6bf_ByteSec,
            # 'value_6bf_Diff ': self.value_6bf_Diff,
            # 'value_6bf_rate ': self.value_6bf_rate,
            # 'value_Avg ': self.value_Avg,
            # 'value_Bf ': self.value_Bf,
            # 'value_Bf_ByteSec ': self.value_Bf_ByteSec,
            # 'value_Diff ': self.value_Diff,
            # 'value_Rate ': self.value_Rate

        }
        tags_dict = {
            "infra_version": self.infra_version,
            "vr_global_ip": self.vr_global_ip,
            "zone": ""
        }

        points = {
                    'measurement': table_name,
                    'time': self.time,
                    'tags': tags_dict,
                    'fields': fields_dict
                }

        json_str = json.dumps(points, sort_keys=True, indent=4, separators=(',', ': '))

        # print json_str
        return json_str

    def get_ByteSec(self, input_now, day, metric):
        query = "select " + metric +  " from " + table_name + " where vr_global_ip = '" + self.vr_global_ip +  "' and time > ('" + str(
            self.time) + "' - " + day + "d - 20h) and time < ('" + str(
            self.time) + "' - " + day + "d) order by time desc limit 1"
        result = self.client.query(query).raw
        #print "result in bytesec ", result
        c = 0
        valueSum = 0
        if 'series' in result:
            for v in result['series'][0]['values']:
                valueSum += float(v[1])
                #valueSum_output += float(v[2])
                c += 1
        if (c == 0):
            valueByteSec = 0.0
            #valueByteSec_output = 0

        else:
            #print "input_now - valueSum", input_now, valueSum
            valueByteSec = (input_now - valueSum) / 1200.0
            #valueByteSec_output = (input_now - valueSum_output) / 1200

        return valueByteSec

    # def get_avg(self, day):
    #     query = "select value_ByteSec from " + self.measurement + " where switch_name = '" + self.input_diff_6 + "' and port_number = '" + self.input_avg_6 + "' and time > ('" + str(self.time) + "' - " + str(day) + "d - 1h) and time < ('" + str(self.time) + "' - " + str(day) + "d) order by time desc limit 12"
    #     result = self.client.query(query).raw
    #     c = 0
    #     valueSum = 0
    #     if 'series' in result:
    #         for v in result['series'][0]['values']:
    #             valueSum += int(v[1])
    #             c += 1
    #     if (c == 0):
    #         valueAvg = 0
    #     else:
    #         valueAvg = valueSum / c
    #     return valueAvg

    # def get_diff(self, value_Avg, value_ByteSec):
    #     valueDiff = value_Avg - value_ByteSec
    #     return valueDiff

    # def get_rate(self, value_Avg, value_ByteSec):
    #     if (value_Avg != 0):
    #         valueRate = float(value_ByteSec) / float(value_Avg)
    #     else:
    #         valueRate = 0.0
    #     return valueRate

    # def get_bf(self,day):
    #     query = "select value from " + self.measurement + " where switch_name = '" + self.input_diff_6 + "' and port_number = '" + self.input_avg_6 + "' and time > ('" + str(self.time) + "' - " + day + "d - 15m) and time < ('" + str(self.time) + "' - " + day + "d) order by time desc limit 1"
    #     result = self.client.query(query).raw
    #     c = 0
    #     valueSum = 0
    #     if 'series' in result:
    #         for v in result['series'][0]['values']:
    #             valueSum += int(v[1])
    #             c += 1
    #     if (c == 0):
    #         valueBf = 0
    #     else:
    #         valueBf = valueSum / c
    #     return valueBf

def str_hook(obj):
    return {k.encode('utf-8') if isinstance(k,unicode) else k :
            v.encode('utf-8') if isinstance(v, unicode) else v for k,v in obj}


if __name__ == '__main__':

    start_time = time.time()
    client = InfluxDBClient(influx,inport,'root','root',org_db)

    #First of all I need to know what columns exist in the database.
    # query = " show field keys"
    # result = client.query(query).raw
    # print " show field keys", result
    #
    # query = " show tag keys"
    # result = client.query(query).raw
    # print " show tag keys", result

#    query = "select * from " + table_name + " order by time desc limit 10"

    #TEST
    query = "select * from " + table_name + "   limit 2000"
    result = client.query(query).raw
    print("%s seconds elapsed. 1" % (time.time() - start_time))

    #print result

    rc_b = 0
    pointlist = []
    measurement = result["series"][0]["name"]
    # print "client", client
    # print "measurement", measurement

    for r in result["series"][0]["values"]:
        values = []
        for s in r:
            values.append(s)

        point = Point(

            client,  #
            measurement,  #
            values[0],  # time
            values[1],  # input
            values[2],  # input_avg_5
            values[3],  # input_avg_6
            values[4],  # input_avg_7
            values[5],  # input_diff_5
            values[6],  # input_diff_6
            values[7],  # input_diff_7
            values[8],  # input_rates_5
            values[9],  # input_rates_6
            values[10],  # input_rates_7
            values[11],  # input_value_5
            values[12],  # input_value_6
            values[13],  # input_value_7
            values[14],  # infra_version
            values[15],  # output
            values[16],  # output_avg_5
            values[17],  # output_avg_6
            values[18],  # output_avg_7
            values[19],  # output_diff_5
            values[20],  # output_diff_6
            values[21],  # output_diff_7
            values[22],  # output_rates_5
            values[23],  # output_rates_6
            values[24],  # output_rates_7
            values[25],  # output_value_5
            values[26],  # output_value_6
            values[27],  # output_value_7
            values[28]  # vr_global_ip

        )
        pointlist.append(point)
        rc_b += 1

    print("%s seconds elapsed. 2" % (time.time() - start_time))

    main_dict = {
        'points': [
        ]
    }

    client2 = InfluxDBClient(influx,inport,'root','root',tgt_db)
    for r in pointlist:
        pyjson = json.loads(r.get_json(), object_pairs_hook=str_hook)
        #print "pyjson", pyjson
        main_dict['points'].append(pyjson)

    #print "main dict", main_dict

    client2.write(main_dict, params={'db': tgt_db})
    print("%s seconds elapsed. 3" % (time.time() - start_time))

    query = "select count(input) from " + table_name
    result = client2.query(query).raw

    for r in result["series"][0]["values"]:
        rc_a = r[1]
    print str(rc_b) + " records in from " + org_db + ":" + table_name + ", "
    print str(rc_a) + " records out to " + tgt_db + ":" + table_name + ". "
    print("%s seconds elapsed." % (time.time() - start_time))