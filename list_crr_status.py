#!/usr/bin/python
import os
import boto3
import argparse
import sys
import json
from kafka import BrokerConnection
from kafka.protocol.admin import ListGroupsRequest_v1, DescribeGroupsRequest_v1
from kafka import KafkaConsumer, TopicPartition
import socket

##
# Configuration
PROFILE_DEF="default"
KAFA_BROKERS=('10.233.66.185', '10.233.111.236', '10.233.80.157', '10.233.78.203', '10.233.124.236')
KAFA_BROKER_PORT=9092


bootstrap = []
for s in KAFA_BROKERS:
    bootstrap.append("{0}:{1}".format(s,KAFA_BROKER_PORT))

def list_avail_destinations(args, broker_addr):

    bc = BrokerConnection(broker_addr, int(args.brokerport), socket.AF_INET)
    bc.connect_blocking()

    list_groups_request = ListGroupsRequest_v1()
    future = bc.send(list_groups_request)
    while not future.is_done:
        for resp, f in bc.recv():
            f.success(resp)
    
    bc.close()

    # Only show CRR groups - might want to extend to lifecycel
    for group in future.value.groups:
        if group[0].find('backbeat-replication-group') == 0:
            print(group[0])


def describe_group(args, topic):
    global bootstrap
    out = ()

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=args.group,
        enable_auto_commit=False
    )
    topics = consumer.topics()
    if not topic in topics:
        return(False)


    for p in consumer.partitions_for_topic(topic):
        tp = TopicPartition(topic, p)
        consumer.assign([tp])
        committed = consumer.committed(tp)
        consumer.seek_to_end(tp)
        last_offset = consumer.position(tp)
        out += ({"topic": topic, "partition": p, "committed": committed, "last_offset": last_offset, "lag": (last_offset - committed)},)
    consumer.close(autocommit=False)
    return out

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='show CRR status of a bucket, destination location or object')
    parser.add_argument('--bucket', '-b', default=False, help='target bucket')
    parser.add_argument('--queues', '-l', action="store_true", help='list remote destinations present in the target bucket')
    parser.add_argument('--key', '-k', default=False, help='target object')
    parser.add_argument('--destination', '-d', default=False, help='target destination to view')
    parser.add_argument('--endpoint', default='https://s3.amazonaws.com')
    parser.add_argument('--profile', default=PROFILE_DEF)
    parser.add_argument('--brokerlist', '-r', default=KAFA_BROKERS, help='Kafka broker host')
    parser.add_argument('--brokerport', '-p', default=KAFA_BROKER_PORT, help='Kafka broker port')
    parser.add_argument('--group', '-g', default=False, help='consumer group')
    args = parser.parse_args()

    if args.queues:
        for broker in KAFA_BROKERS:
            list_avail_destinations(args, broker)
    if args.group:
        print(describe_group(args, 'backbeat-replication'))
