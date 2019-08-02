#!/usr/bin/python
# Quick-n-dirty to dump CRR backlog for a Zenko cloud destination.
import os
import boto3
import argparse
import sys
import json
from kafka import BrokerConnection, KafkaConsumer, TopicPartition
from kafka.protocol.admin import ListGroupsRequest_v1, DescribeGroupsRequest_v1
import socket
import urllib
import time

##
# Configuration

# Until I do a little more book lernin' I'll have to spell-out the Kafka
# brokers
KAFA_ADDRS=()
KAFA_PORT=9092
CONSUMER_TIMEOUT=2000

bootstrap = []
for s in KAFA_ADDRS:
    bootstrap.append("{0}:{1}".format(s, KAFA_PORT))


globals = {
    "max_q_time": 0
}

##
# Base-2 human-readable
def sizeof_fmt2(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

##
# Base 10 human-readable
def sizeof_fmt10(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1000.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1000.0
    return "%.1f%s%s" % (num, 'Y', suffix)

def list_avail_destinations(args, broker_addr, port):
    ''' List the groups currenly using '''
    bc = BrokerConnection(broker_addr, int(port), socket.AF_INET)
    bc.connect_blocking()

    list_groups_request = ListGroupsRequest_v1()
    future = bc.send(list_groups_request)
    while not future.is_done:
        for resp, f in bc.recv():
            f.success(resp)
    bc.close()

    # Only show CRR groups - might want to extend to lifecycle
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
        try:
            out += ({"topic": topic,
                    "partition": part,
                    "committed": committed,
                    "last_offset": last_offset,
                    "lag": (last_offset - committed)},)
        except TypeError:
            sys.stderr.write("bad/missing info on consumer group (doesn't exist?)\n")
            sys.exit(1)

    consumer.close(autocommit=False)
    return out

def dump_from_offset(args, topic, part, offset, end, nownow=None):
    '''
    Dump from last comitted to last known offset. Or time-out. which is what
    usually happens.
    '''
    global bootstrap, globals

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id="backbeat-replication-group-{0}".format(args.destination),
        enable_auto_commit=False,
        consumer_timeout_ms=int(args.timeout)
    )
    partition = TopicPartition(topic, part)
    consumer.assign([partition])
    consumer.seek(partition, offset)

    ttl_bytes = 0
    o_count = 0
    queuelist = {}
    for msg in consumer:
        if msg.offset > end: break
        else:
            logline = json.loads(msg.value)
            if args.bucket and logline['bucket'] != args.bucket:
                continue
            if 'canary' in logline:
                continue
            value = json.loads(logline['value'])
            for backend in value["replicationInfo"]['backends']:
                if backend['site'] == args.destination:
                    queue_time = nownow - (float(msg.timestamp)/1000)
                    if globals['max_q_time'] < queue_time:
                        globals['max_q_time'] = queue_time
                    
                    line = ""
                    if args.csv:
                        line += '"{0}'.format(value['dataStoreName'])
                        if args.bucket == False:
                            line += ':{0}'.format(logline['bucket'])
                        line += '", {0}, "{1}"'.format(part, value['key'])
                        if "isDeleteMarker" in value and value["isDeleteMarker"] == True:
                            line += ', "(delete)"'
                        else:
                            line += ", {0}".format(int(value['content-length']))
                            ttl_bytes += int(value['content-length'])
                        line += ", {0:.2f}".format(round(queue_time, 2))
                        o_count += 1
                        queuelist["{0}{1}".format(str(int(msg.timestamp)), value['key'])] = line
                        break # we hit the backend we're looking for. stop.
                    else:
                        line += 'src: {0}'.format(value['dataStoreName'])
                        if args.bucket == False:
                            line += ':{0}'.format(logline['bucket'])
                        line += ', part: {0}, key: "{1}"'.format(part, value['key'])
                        if "isDeleteMarker" in value and value["isDeleteMarker"] == True:
                            line += " (delete)"
                        else:
                            line += ", size: {0}".format(sizeof_fmt10(int(value['content-length'])))
                            ttl_bytes += int(value['content-length'])
                        line += ", {0:.2f} sec".format(round(queue_time, 2))
                        o_count += 1
                        queuelist["{0}{1}".format(str(int(msg.timestamp)), value['key'])] = line
                        break # we hit the backend we're looking for. stop.
    
    return o_count, ttl_bytes, queuelist

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Show CRR status and timings of obejcts headed to a destination')
    parser.add_argument('--queues', '-l', action="store_true", help='List remote destinations available')
    parser.add_argument('--brokerlist', '-r', default=False, help='Kafka broker list; host:port, comma delimited')
    parser.add_argument('--bucket', '-b', default=False, help='List only info on a src (Zenko) bucket')
    parser.add_argument('--destination', '-d', default=False, help='target destination to view')
    parser.add_argument('--summary', '-s', help='just output summary', action="store_true")
    parser.add_argument('--timeout', '-t', help='consumer timeout', default=CONSUMER_TIMEOUT)
    parser.add_argument('--csv', '-c', help='output in CSV format', action="store_true", default=False)
    args = parser.parse_args()

    if args.brokerlist:
        bootstrap = []
        brokerlist = args.brokerlist.split(',')
        for n in range(0, len(brokerlist)):
            bootstrap.append(brokerlist[n].strip())

    if args.queues:
        for broker in bootstrap:
            try:
                br = broker.split(":")[0]
                pt = broker.split(":")[1]
                list_avail_destinations(args, br, pt)
            except: pass
        sys.exit(0)

    if args.destination:
        zgroupinfo = describe_group(args, 'backbeat-replication')

        if args.summary:
            for zg in zgroupinfo:
                print(zg)
            sys.exit(0)
        else:
            o_count = 0
            ttl_bytes = 0
            part_count = {}
            output = {}

            if args.csv:
                print("src, partition, key, size, seconds in queue")
            else:
                sys.stdout.write('hang on....')
                sys.stdout.flush()
            
            for zg in zgroupinfo:
                # Super ugly hack to try and account for time spent in queue
                rightnow = time.time()+( (int(args.timeout)/1000)*len(zgroupinfo))
                
                count, obytes, queuelist = dump_from_offset(
                        args, 'backbeat-replication', zg['partition'], zg['committed'], zg['last_offset'], nownow=rightnow)
                output.update(queuelist)
                o_count += count
                ttl_bytes += obytes
                part_count[zg['partition']] = len(queuelist)

            for key in sorted(output):
                print("{0}".format(output[key]))
            
            if not args.csv:
                print("{0} objects for {1}, max time in-queue: {2:.2f} sec".format(
                    o_count, sizeof_fmt10(ttl_bytes), globals['max_q_time']))
                for p in part_count:
                    print('part {0}: {1}'.format(p, part_count[p]))

