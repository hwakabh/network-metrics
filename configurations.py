## configuration file
config_file_name = '/mnt/switch_list.csv'

## variables for Visualize targets(e.g. Networking Switch)
snmp_community = 'public'
oidList = [".1.3.6.1.2.1.31.1.1.1.6",".1.3.6.1.2.1.31.1.1.1.10",".1.3.6.1.2.1.31.1.1.1.15"]

## variables for InfluxDB
influx_ip = '<YOUR_INFLUXDB_IP>'
influx_port = 3005
influx_user = 'root'
influx_pass = 'root'
influx_dbname = 'doublemeasure'

## variables for python multi-threading
measurementList = ["port_in","port_out","port_high"]