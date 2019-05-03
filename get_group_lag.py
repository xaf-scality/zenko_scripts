#!/usr/bin/python

import argparse
import subprocess
import sys
import os

##
# Configuration
release = "galaxy-z"
kubectl_command_path = "/home/centos/zenko-stack/metalk8s-1.1.0-alpha1/.shell-env/metalk8s/bin/"
kubectl_auth = "/home/centos/zenko-stack/metalk8s/inventory/galaxy-z/artifacts/admin.conf"

##
# Set env
subenv = os.environ.copy()
subenv["PATH"] = "{0}:{1}".format(kubectl_command_path, os.environ["PATH"])
subenv["KUBECONFIG"] = kubectl_auth

def set_queue_host():
    rel_var = release.upper()
    rel_var = rel_var.replace('-', '_', 3)
    kafka_client_pod = subprocess.Popen(
                        "kubectl get pods |grep debug-kafka-client |awk '{print $1}'",
                        shell=True,
                        env=subenv,
                        stdout=subprocess.PIPE).stdout.read().strip()
    cmd = "kubectl exec -it {0} env |grep ZENKO_QUEUE_SERVICE_HOST".format(kafka_client_pod)
    cmd += "|gawk -F= '{print $2}'"
    queue_host = subprocess.Popen(cmd, shell=True, env=subenv,
                        stdout=subprocess.PIPE).stdout.read().strip()
                        
    return kafka_client_pod, queue_host

def list_consumer_groups(kafka_client_pod, queue_host):

    scmd = "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {0}:9092 --list".format(queue_host)
    dcmd = 'kubectl exec {0} -- {1}'.format(kafka_client_pod, scmd)
    cgroups = subprocess.Popen(dcmd, shell=True, env=subenv,
                        stdout=subprocess.PIPE).stdout.read().strip()
    print(cgroups)

def show_group(kafka_client_pod, queue_host, group):

    scmd = "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {0}:9092 --describe --group {1}".format(queue_host, group)
    dcmd = 'kubectl exec {0} -- {1}'.format(kafka_client_pod, scmd)
    group_txt = subprocess.Popen(dcmd, shell=True, env=subenv,
                        stdout=subprocess.PIPE).stdout.read().strip()
    print(group_txt)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='list consumer groups and output lag')
    parser.add_argument('--list', '-l', help='list available consumer groups', action="store_true")
    parser.add_argument('--group', '-g', help='output lag values for consumer group', default=False)
    args = parser.parse_args()

    kafka_client_pod, queue_host = set_queue_host()

    if args.list == True:
        list_consumer_groups(kafka_client_pod, queue_host)
    elif args.group:
        show_group(kafka_client_pod, queue_host, args.group)
