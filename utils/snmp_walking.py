from pysnmp.hlapi import *
from pysnmp.entity.rfc3413.oneliner import cmdgen
import json


def walk(switch, oid):
    cmdGen = cmdgen.CommandGenerator()

    errorIndication, errorStatus, errorIndex, varBinds = cmdGen.nextCmd(
        cmdgen.CommunityData('public'),
        cmdgen.UdpTransportTarget((switch, 161)),
        oid
    )
    if errorIndication:
        print(errorIndication)
    elif errorStatus:
        print('%s at %s' % (errorStatus.prettyPrint(), errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
    else:
        for varBind in varBinds:
            print(' = '.join([x.prettyPrint() for x in varBind]))


if __name__ == '__main__':
    walk('localhost', '.1.3.6.1.2.1.31.1.1.1.6')
    walk('localhost', '.1.3.6.1.2.1.31.1.1.1.10')
    walk('localhost', '.1.3.6.1.2.1.31.1.1.1.15')