#!/usr/bin/python

import argparse
import subprocess
import sys
import os


##
# Set env
subenv = os.environ.copy()


def set_queue_host():

    kafka_client_pod = (
        subprocess.Popen(
            "kubectl get pods |grep debug-kafka-client |awk '{print $1}'",
            shell=True,
            env=subenv,
            stdout=subprocess.PIPE,
        )
        .stdout.read()
        .strip()
    )
    cmd = "kubectl exec -it {0} env |grep ZENKO_QUEUE_SERVICE_HOST".format(
        kafka_client_pod
    )
    cmd += "|gawk -F= '{print $2}'"
    queue_host = (
        subprocess.Popen(cmd, shell=True, env=subenv, stdout=subprocess.PIPE)
        .stdout.read()
        .strip()
    )

    return kafka_client_pod, queue_host


def list_consumer_groups(kafka_client_pod, queue_host):

    scmd = "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {0}:9092 --list".format(
        queue_host
    )
    dcmd = "kubectl exec {0} -- {1}".format(kafka_client_pod, scmd)
    cgroups = (
        subprocess.Popen(dcmd, shell=True, env=subenv, stdout=subprocess.PIPE)
        .stdout.read()
        .strip()
    )
    print(cgroups)


def show_group(kafka_client_pod, queue_host, group):

    scmd = "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {0}:9092 --describe --group {1}".format(
        queue_host, group
    )
    dcmd = "kubectl exec {0} -- {1}".format(kafka_client_pod, scmd)
    group_txt = (
        subprocess.Popen(dcmd, shell=True, env=subenv, stdout=subprocess.PIPE)
        .stdout.read()
        .strip()
    )
    print(group_txt)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Lists consumer groups and output lag. Make sure "kubectl" is set up in your shell before running'
    )
    parser.add_argument(
        "--list", "-l", help="list available consumer groups", action="store_true"
    )
    parser.add_argument(
        "--group", "-g", help="output lag values for consumer group", default=False
    )
    args = parser.parse_args()

    kafka_client_pod, queue_host = set_queue_host()

    if args.list == True:
        list_consumer_groups(kafka_client_pod, queue_host)
    elif args.group:
        show_group(kafka_client_pod, queue_host, args.group)
