from influxdb import InfluxDBClient
import json
import datetime


def getAvg_of_query_result(result):
    c = 0
    valuesum = 0
    if 'series' in result:
        for v in result['series'][0]['values']:
            valuesum += int(v[1])
            c += 1

    if c == 0:
        return 0
    else:
        return valuesum / c


# def getByteSec_DaysBefore(metric, day):
#     # Get average
#     # query = "select " + metric + " from vr_traffic where vr_global_ip = '" + vr_global_ip + "' and time < now() -" + str(day) + "m and time > now() -" + str(12*day) + "m limit 12"
#     # query = "select " + metric + " from vr_traffic where vr_global_ip = '" + vr_global_ip + "' and time < now() -" + str(
#     #     day) + "d and time > now() -" + str(day) + "d -1h order by time desc limit 12"
#     value_DaysBefore_ByteSec = calcByteSec_bf(metric, vr_global_ip, day)
#     return value_DaysBefore_ByteSec


# "input", 7, input_now, input_avg
def getDiffDays(metric, value_now, value_old):
    # value_now and value_avg are already converted into bps.
    # So just return the difference.
    value_diff = value_old - value_now
    return float(value_diff)


def getRateDays(metric, value_now, value_old):
    # value_now and value_avg are already converted into bps.
    # So just caluculate the ratio current / old
    if value_now <> 0 and value_old <> 0:
        value_rate = float(value_now) / float(value_old)
    else:
        value_rate = 0.0
    return float(value_rate)


def getOneDay(metric, day):
    # Get one history value
    # query = "select " + metric + " from vr_traffic where vr_global_ip = '" + vr_global_ip + "' and time < now() -" + str(day) + "m and time > now() -" + str(day) + "m limit 1"
    #     day) + "d and time > now() -" + str(day) + "d -2h order by time desc limit 1"
    query = "select " + metric + " from vr_traffic where vr_global_ip = '" + vr_global_ip + "' and time < ( now() - " + str(day) + "d  ) and time > ( now() - " + str(day) + "d  -2h ) order by time desc limit 1"
    value_now = getAvg_of_query_result(client.query(query).raw)
    return value_now


def calcByteSec(metric, value_now):
    query = "select " + metric + " from vr_traffic where vr_global_ip = '" + vr_global_ip + "' order by time desc limit 1"
    value_nowByteSec = float((value_now - getAvg_of_query_result(client.query(query).raw)) / (20.0 * 60.0))
    return value_nowByteSec


def calcByteSec_bf(metric, vr_global_ip, day):
    query = "select " + metric + " from vr_traffic where vr_global_ip = '" + vr_global_ip + "' and time < ( now() - " + str(day) + "d ) and time > ( now() - " + str(day) + "d  -2h ) order by time desc limit 2"
    result = client.query(query).raw

    if 'series' in result and len(result['series'][0]['values']) > 1:
        diff = float(result['series'][0]['values'][0][1] - result['series'][0]['values'][1][1])
        bps_bf = diff / (20.0 * 60.0)
        return bps_bf
    else:
        bps_bf = 0.0
        return bps_bf

def moving_average_daysBefore(metric, vr_global_ip, day):
    query = "select " + metric + " from vr_traffic where vr_global_ip = '" + vr_global_ip + "' and time < ( now() - " + str(day) + "d  ) and time > ( now() - " + str(day) + "d  -2h ) order by time desc limit 4"
    result = client.query(query).raw

    diff_sum = 0.0
    if 'series' in result and len(result['series'][0]['values']) > 1:
        for each_value_index in range(len(result['series'][0]['values'])):
            if each_value_index < len(result['series'][0]['values']) - 1:
                each_diff = float(result['series'][0]['values'][each_value_index][1] - result['series'][0]['values'][each_value_index + 1][1])
                diff_sum = diff_sum + each_diff
        bps_moving_average_daysBefore = float((diff_sum/(len(result['series'][0]['values']) - 1)) / (20.0 * 60.0))
        return bps_moving_average_daysBefore
    else:
        bps_moving_average_daysBefore = 0.0
        return bps_moving_average_daysBefore


if __name__ == '__main__':
    print str(datetime.datetime.now()) + " : Starting vRouter data load."
    # only for testing
    # current_UTCtime = pd.to_datetime(datetime.datetime.now()) + pd.tseries.offsets.Hour(-9)
    client = InfluxDBClient('test_influxdb', 8086, 'root', 'root', 'cloudstackvr3')

    ###Test using dummy data
    with open('/mnt/output_new_sample.json', 'r') as f:
        jsonData = json.load(f)
    f.close()

    for vr in jsonData:
        # vr["time"] = str(current_UTCtime)
        vr_global_ip = vr["tags"]["vr_global_ip"]
        input_now = vr["fields"]["input"]
        output_now = vr["fields"]["output"]

        # Bytes/s for current json value
        input_now_ByteSec = calcByteSec("input", input_now)
        output_now_ByteSec = calcByteSec("output", output_now)

        # # Bytes/s for past json value
        input_moving_average_daysBefore_ByteSec = moving_average_daysBefore("input", vr_global_ip, 7)
        output_moving_average_daysBefore_ByteSec = moving_average_daysBefore("output", vr_global_ip, 7)

        vr["fields"]["input_avg_7"] = moving_average_daysBefore("input", vr_global_ip, 7)  #Need moving average?
        vr["fields"]["output_avg_7"] = moving_average_daysBefore("output", vr_global_ip, 7) #Need moving average?
        vr["fields"]["input_bps"] = input_now_ByteSec
        vr["fields"]["output_bps"] = output_now_ByteSec
        vr["fields"]["input_diff_7"] = getDiffDays("input", input_now_ByteSec, input_moving_average_daysBefore_ByteSec)
        vr["fields"]["output_diff_7"] = getDiffDays("output", output_now_ByteSec, output_moving_average_daysBefore_ByteSec)
        vr["fields"]["input_rates_7"] = getRateDays("input", input_now_ByteSec, input_moving_average_daysBefore_ByteSec)
        vr["fields"]["output_rates_7"] = getRateDays("output", output_now_ByteSec, output_moving_average_daysBefore_ByteSec)
        vr["fields"]["input_value_7"] = getOneDay("input", 7)
        vr["fields"]["output_value_7"] = getOneDay("output", 7)
        vr["fields"]["input_bps_7"] = calcByteSec_bf("input", vr_global_ip, 7)
        vr["fields"]["output_bps_7"] = calcByteSec_bf("output", vr_global_ip, 7)

        # # Bytes/s for past json value
        input_moving_average_daysBefore_ByteSec = moving_average_daysBefore("input", vr_global_ip, 6)
        output_moving_average_daysBefore_ByteSec = moving_average_daysBefore("output", vr_global_ip, 6)

        vr["fields"]["input_avg_6"] = moving_average_daysBefore("input", vr_global_ip, 6)  # Need moving average?
        vr["fields"]["output_avg_6"] = moving_average_daysBefore("output", vr_global_ip, 6)  # Need moving average?
        vr["fields"]["input_bps"] = input_now_ByteSec
        vr["fields"]["output_bps"] = output_now_ByteSec
        vr["fields"]["input_diff_6"] = getDiffDays("input", input_now_ByteSec, input_moving_average_daysBefore_ByteSec)
        vr["fields"]["output_diff_6"] = getDiffDays("output", output_now_ByteSec, output_moving_average_daysBefore_ByteSec)
        vr["fields"]["input_rates_6"] = getRateDays("input", input_now_ByteSec, input_moving_average_daysBefore_ByteSec)
        vr["fields"]["output_rates_6"] = getRateDays("output", output_now_ByteSec, output_moving_average_daysBefore_ByteSec)
        vr["fields"]["input_value_6"] = getOneDay("input", 6)
        vr["fields"]["output_value_6"] = getOneDay("output", 6)
        vr["fields"]["input_bps_6"] = calcByteSec_bf("input", vr_global_ip, 6)
        vr["fields"]["output_bps_6"] = calcByteSec_bf("output", vr_global_ip, 6)


    client.write_points(jsonData)
    print str(datetime.datetime.now()) + " : End vRouter data load."