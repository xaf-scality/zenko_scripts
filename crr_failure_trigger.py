#!/bin/python
#
# NOTES: the "retry-failed" cronjobs needs to be enabled for this script to
#        work. In Zenko/vaules.yml you need:
#
#            maintanence.enabled = True
#
#        and you need to retain more than one passed successful run in the
#        pod listing:
#
#            maintanance.successfulJobsHistory = 3
#
#        Then "helm upgrade" your deployment
import subprocess
import json
import sys
import os
import time
import smtplib
from email.mime.text import MIMEText

##
# Configuration
failure_threshold = 2 # Failures at or greater than this number result in an
                      # alert. Needs to be the same or less than the retained
                      # successfulJobsHistory value.
failure_log_ret = 3   # Number of completed pods to expect for inspection
ack_on_failure=True   # Send only one message per 'ack_timeout' value below
ack_timout=86400
kubectl_command_path = '/home/centos/zenko-stack/metalk8s-1.1.0-alpha1/.shell-env/metalk8s/bin/'
kubectl_auth = '/home/centos/zenko-stack/metalk8s/inventory/galaxy-z/artifacts/admin.conf'
email_on_failure=True
email_recipient='centos@localhost'
ack_file='/tmp/zenko_failures_ack'

##
# Set env
subenv = os.environ.copy()
subenv["PATH"] = "{0}:{1}".format(kubectl_command_path, os.environ["PATH"])
subenv["KUBECONFIG"] = kubectl_auth

def send_message(subject, message):
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['To'] = email_recipient
    s = smtplib.SMTP('localhost')
    s.sendmail(msg['From'], [email_recipient], msg.as_string())
    s.quit()

def do_something(subj, message):
    if os.path.isfile(ack_file):
        with open(ack_file, "r") as ft:
            ack_t = int(ft.read())
            try:
                if ack_t < (int(time.time()) - ack_timout):
                    send_message('ATTN: Probem with Zenko CRR Retries (repeat notice)', message)
                    with open(ack_file, "w") as ft:
                        ft.write(str(int(time.time())))
                    return
            except Exception as e:
                 send_message(subj, 'problem with failure message script: {0}'.format(str(e)))   
    else:
        send_message(subj, message)
        with open(ack_file, "w") as ft:
            ft.write(str(int(time.time())))


def check_retry_pods():
    ##
    # Get the retry pod names
    retry_pods_text = subprocess.Popen("kubectl get pods | grep retry-failed |grep Completed | gawk '{print $1}'",
                        shell=True,
                        env=subenv,
                        stdout=subprocess.PIPE).stdout.read().strip()
    retry_pods = retry_pods_text.split('\n')

    failed_dests = {}
    for pod in retry_pods:
        try:
            logtext = json.loads(subprocess.Popen("kubectl logs {0}".format(pod),
                        shell=True,
                        env=subenv,
                        stdout=subprocess.PIPE).stdout.read().strip())
        except Exception as e:
            do_something('Problem checking retry pod logs',
                    'problem with retry pod: {0}'.format(e))
            continue

        if "error" in logtext:
            do_something('Zenko Retry Job Error',
                    'problem with retry pod: {0}'.format(logtext['error']))
        else:
            for dest in logtext['result']:
                if not dest in failed_dests:
                    failed_dests[dest] = 1
                else:
                    failed_dests[dest] += 1


    for dest in failed_dests:
        if failed_dests[dest] >= failure_threshold:
            message_buffer = '''
Please check the status of your Zenko cluster. A CRR failure threshold has
been exceeded (or a monitoring script error has been encountered) for 
destination:

{0}

Current configuration is to alert when {1} out of {2} CRR retry attempts fail.
'''.format(dest, failure_threshold, failure_log_ret)
            do_something('Too many CRR failures', message_buffer)
    
    return(True)

if __name__ == "__main__":
    
    sys.exit(check_retry_pods())
