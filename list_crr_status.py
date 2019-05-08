#!/usr/bin/python
#
# Quick-n-dirty to dump location CRR backlog for a Zenko cloud destination.
# Can be limited to destination+bucket as well but I haven't tested the output
# for that.

import os
import boto3
import argparse
import sys
import json
from kafka import BrokerConnection, KafkaConsumer, TopicPartition
from kafka.protocol.admin import ListGroupsRequest_v1, DescribeGroupsRequest_v1
import socket
import urllib

##
# Configuration

# Until I do a little more book lernin' I'll have to spell-out the Kafka
# brokers
KAFA_BROKERS=('10.233.66.185', '10.233.111.236', '10.233.80.157', '10.233.78.203', '10.233.124.236')
KAFA_BROKER_PORT=9092


bootstrap = []
for s in KAFA_BROKERS:
    bootstrap.append("{0}:{1}".format(s,KAFA_BROKER_PORT))

##
# Base-2 human-readable, might want to make the base-10 
def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def list_avail_destinations(args, broker_addr, port):
    ''' List the replication groups currenly using '''
    bc = BrokerConnection(broker_addr, int(port), socket.AF_INET)
    bc.connect_blocking()

    list_groups_request = ListGroupsRequest_v1()
    future = bc.send(list_groups_request)
    while not future.is_done:
        for resp, f in bc.recv():
            f.success(resp)
    
    bc.close()

    # Only show CRR groups - might want to extend to lifecycel
    for group in future.value.groups:
        if group[0].find('backbeat-replication-group-') == 0:
            print(group[0].replace('backbeat-replication-group-', ''))


def describe_group(args, topic):
    '''
    Get group descriptions. Important are the partitions and last committed
    offset.
    '''
    global bootstrap
    out = ()

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id="backbeat-replication-group-{0}".format(args.destination),
        enable_auto_commit=False
    )
    topics = consumer.topics()
    if not topic in topics:
        return(False)

    for part in consumer.partitions_for_topic(topic):
        tp = TopicPartition(topic, part)
        consumer.assign([tp])
        committed = consumer.committed(tp)
        consumer.seek_to_end(tp)
        last_offset = consumer.position(tp)
        out += ({"topic": topic,
                 "partition": part,
                 "committed": committed,
                 "last_offset": last_offset,
                 "lag": (last_offset - committed)},)

    consumer.close(autocommit=False)
    return out

def dump_from_offset(args, topic, part, offset, end):
    '''
    Dump from last comitted to last known offset. Or time-out. which is what
    usually happens.
    '''
    global bootstrap

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id="backbeat-replication-group-{0}".format(args.destination),
        enable_auto_commit=False,
        consumer_timeout_ms=1000
    )
    partition = TopicPartition(topic, part)
    consumer.assign([partition])
    consumer.seek(partition, offset)

    ttl_bytes = 0
    o_count = 0
    for msg in consumer:
        if msg.offset > end: break
        else:
            logline = json.loads(msg.value)
            if args.bucket and logline['bucket'] != args.bucket:
                continue

            value = json.loads(logline['value'])
            for backend in value["replicationInfo"]['backends']:
                if backend['site'] == args.destination:
                    sys.stdout.write('src loc: {0}'.format(value['dataStoreName']))
                    if args.bucket == False:
                        sys.stdout.write(':{0}'.format(logline['bucket']))
                    sys.stdout.write(', key: "{0}"'.format(value['key']))
                    if "isDeleteMarker" in value and value["isDeleteMarker"] == True:
                        print(" (delete)")
                    else:
                        print(", size: {0}".format(sizeof_fmt(int(value['content-length']))))
                        ttl_bytes += int(value['content-length'])
                    o_count += 1
                    break
    return o_count, ttl_bytes

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Show CRR status of a bucket/destination')
    parser.add_argument('--queues', '-l', action="store_true", help='List remote destinations available')
    parser.add_argument('--brokerlist', '-r', default=False, help='Kafka broker list; host:port, comma delimited')
    parser.add_argument('--bucket', '-b', default=False, help='List only info on a src (Zenko) bucket')
    parser.add_argument('--destination', '-d', default=False, help='target destination to view')
    parser.add_argument('--summary', '-s', help='just output summary', action="store_true")
    args = parser.parse_args()

    if args.brokerlist:
        brokerlist = args.brokerlist.split(',')
        for n in range(0, len(args.brokerlist)):
            brokerlist[n] = brokerlist[n].trim()
    else:
        brokerlist = bootstrap

    if args.queues:
        for broker in brokerlist:
            br = broker.split(":")[0]
            pt = broker.split(":")[1]
            list_avail_destinations(args, br, pt)
    if args.destination:
        zgroupinfo = describe_group(args, 'backbeat-replication')

        if args.summary:
            for zg in zgroupinfo:
                print(zg)
            
            sys.exit(0)
        else:
            o_count = 0
            ttl_bytes = 0
            for zg in zgroupinfo:
                count, obytes = dump_from_offset(args, 'backbeat-replication', zg['partition'], zg['committed'], zg['last_offset'])
                o_count += count
                ttl_bytes += obytes
            print("{0} objects for {1}".format(o_count, sizeof_fmt(ttl_bytes)))

