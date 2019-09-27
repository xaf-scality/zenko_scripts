#!/usr/bin/python
'''
Ugly hack to get reset the pending and failed counters in redis
'''

import subprocess
import sys
import os
import  argparse

##
# Configuration (in case this is not already in you PATH)
kubectl_command_path = ""
kubectl_auth = ""

##
# Set env
subenv = os.environ.copy()
subenv["PATH"] = "{0}:{1}".format(kubectl_command_path, os.environ["PATH"])
subenv["KUBECONFIG"] = kubectl_auth

def find_redis_master():
    cmd = "kubectl get pods | grep redis | gawk '{print $1}'"
    redis_pods = subprocess.Popen(cmd, shell=True, env=subenv,
                        stdout=subprocess.PIPE).stdout.read().strip().split('\n')

    for pod in redis_pods:
        cmd = 'redis-cli info|grep ^role |cut -d: -f2'
        dcmd = 'kubectl exec {0} -c redis -- {1}  2> /dev/null'.format(pod, cmd)
        role = subprocess.Popen(dcmd, shell=True, env=subenv,
                        stdout=subprocess.PIPE).stdout.read().strip()
        if role == 'master':
            return pod

def list_locations(master_pod):
    
    src_buckets = (),
    dest_locations = ()

    cmd = r"kubectl exec {0} -c redis -- redis-cli keys \*".format(master_pod)
    r_keys = subprocess.Popen(cmd, shell=True, env=subenv,
                        stdout=subprocess.PIPE).stdout.read().strip().split('\n')
    for key in r_keys:
        path = key.split(":")
        if not path[1] in src_buckets:
            src_buckets += (path[1],)
        if not path[0] in dest_locations:
            dest_locations += (path[0],)

    print("\nThe following destination locations have keys in redis:")
    for loc in dest_locations:
        print(loc)

def reset_pending(master_pod, dest):
    bad = False
    for k in ['bytespending', 'opspending']:

        pending_key = "{0}:bb:crr:{1}".format(dest, k)
        cmd = 'redis-cli get {0}'.format(pending_key)
        dcmd = 'kubectl exec {0} -c redis -- {1}'.format(master_pod, cmd)
        output = subprocess.Popen(dcmd, shell=True, env=subenv,
                                stdout=subprocess.PIPE).stdout.read().strip()
        
        # Keep from making a mess out of redis
        if output == '':
            print('key ({0}) does not exist'.format(pending_key))
            return

        cmd = 'redis-cli set {0} 0'.format(pending_key)
        dcmd = 'kubectl exec {0} -c redis -- {1}'.format(master_pod, cmd)
        output = subprocess.Popen(dcmd, shell=True, env=subenv,
                                stdout=subprocess.PIPE).stdout.read().strip()
        if output != 'OK': bad == True

    if bad == True: print('NOK')
    else: print('OK')

def reset_failed(master_pod, dest):
    
    keypattern = 'bb:crr:failed:{0}:*'.format(dest)
    cmd = 'redis-cli keys {0}'.format(keypattern)
    dcmd = 'kubectl exec {0} -c redis -- {1}'.format(master_pod, cmd)
    keys = subprocess.Popen(dcmd, shell=True, env=subenv,
                                stdout=subprocess.PIPE).stdout.read().strip().split('\n')
    
    for k in keys:
        cmd = 'redis-cli del {0}'.format(k)
        dcmd = 'kubectl exec {0} -c redis -- {1}'.format(master_pod, cmd)
        output = subprocess.Popen(dcmd, shell=True, env=subenv,
                                stdout=subprocess.PIPE).stdout.read().strip()
        print("deleted keys: {0}".format(output))
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='reset failed/pending Zenko counters in redis USE WITH CARE')
    parser.add_argument('--list', '-l', help='list available storage locations', action="store_true")
    parser.add_argument('--failed', '-f', help='reset failed transfers to 0 for a location', default=False)
    parser.add_argument('--pending', '-p', help='reset pending transfers to 0 for a location', default=False)
    args = parser.parse_args()

    master_pod = find_redis_master()
    if not master_pod:
        print("can't find a master")
        sys.exit(1)
    
    if args.list:
        list_locations(master_pod)
    elif args.pending:
        reset_pending(master_pod, args.pending)
    elif args.failed:
        reset_failed(master_pod, args.failed)
