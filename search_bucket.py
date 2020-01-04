#!/usr/bin/python3

import sys, os, base64, datetime, hashlib, urllib, hmac
import argparse
import requests
import configparser
import logging
from datetime import datetime
from time import mktime
import boto3
import xml.dom.minidom as MD 

def get_profile(profile):
    config = configparser.ConfigParser()
    config.read('{0}/.aws/credentials'.format(os.environ['HOME']))

    if 'aws_access_key_id' not in config[profile] or 'aws_secret_access_key' not in config[profile]:
        # Make sure we have credentials
        access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        if access_key is None or secret_key is None:
            print('No access key is available.')
            sys.exit()
    else:
        access_key = config[profile]['aws_access_key_id']
        secret_key = config[profile]['aws_secret_access_key']

    return({'access_key': access_key,
            'secret_key': secret_key})

def parse_endpoint(endpointstr):
    # chop up the endpoint
    proto = endpointstr[:endpointstr.find("//")-1]

    hostport = endpointstr[(endpointstr.find("//")+2):]
    if hostport.find(":") > 0:
        host = hostport[:hostport.find(":")]
        port = hostport[(hostport.find(":")+1):]
    else:
        host = hostport
        port = None

    return({"hostport": hostport, "proto": proto, "port": port, "host": host})

##
#  Ripped right out of the AWS code examples
def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

##
# Ripped right out of the AWS code examples
def getSignatureKey(key, dateStamp, regionName, serviceName):
    kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, 'aws4_request')
    return kSigning

def bucket_location(args):

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client("s3", endpoint_url=args.endpoint)
    response = s3.get_bucket_location(Bucket=args.bucket)
    return(response)

def get_signed_headers(bucket, searchstr, host, region, access_key, secret_key):
    '''
    Sort of a dense process that's documented elsewere better: 
    https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
    '''
    # We're at now now:
    t = datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ')
    datestamp = t.strftime('%Y%m%d')
    algorithm = 'AWS4-HMAC-SHA256'
    service = 's3'
    canonical_uri = '/{0}'.format(bucket)
    canonical_querystring = 'search='+urllib.parse.quote_plus(searchstr)
    signed_headers = 'host;x-amz-content-sha256;x-amz-date'
    payload_hash = hashlib.sha256(('').encode('utf-8')).hexdigest()
    canonical_headers = 'host:{0}\nx-amz-content-sha256:{1}\nx-amz-date:{2}\n'.format(host, payload_hash, amzdate)
    canonical_request = '{0}\n{1}\n{2}\n{3}\n{4}\n{5}'.format(
            'GET',
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash
        )
    credential_scope = "{0}/{1}/{2}/aws4_request".format(datestamp, region, service)
    string_to_sign = "{0}\n{1}\n{2}\n{3}".format(algorithm, amzdate, credential_scope,
            hashlib.sha256(canonical_request.encode('utf-8')).hexdigest())
    signing_key = getSignatureKey(secret_key, datestamp, region, service)
    signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()
    authorization_header = "{0} Credential={1}/{2}, SignedHeaders={3}, Signature={4}".format(
            algorithm,
            access_key,
            credential_scope,
            signed_headers,
            signature
        )
    return {'x-amz-date':amzdate, 'x-amz-content-sha256': payload_hash, 'Authorization':authorization_header}

def print_raw(xmltxt):
    print(xmltxt)

def print_xml(xmltxt):
    parsed = MD.parseString(xmltxt)
    print(parsed.toprettyxml(indent="    "))

def print_json(xmltxt):
    print("not implimented")

def print_csv(xmltxt):
    print("not implimented")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Seach a bucket. Go ahead. Try it."
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile", default='default', help='boto3 profile name to use for credentials')
    parser.add_argument("--endpoint", default="https://s3.amazonaws.com", help='zenko endpoint URL')
    parser.add_argument("--ca-bundle", default=False, dest="cabundle")
    parser.add_argument("--query", default="", help="zenko md search query (e.g. tags.color=green)")
    parser.add_argument("--output", default="xml", help="one of: raw, xml, json or csv")
    args = parser.parse_args()

    if args.cabundle:
        os.environ["AWS_CA_BUNDLE"] = args.cabundle
        os.environ["REQUESTS_CA_BUNDLE"] = args.cabundle

    location_info = bucket_location(args)
    if 'LocationConstraint' in location_info:
        region = location_info['LocationConstraint']
    else:
        sys.stderr.write("bucket does not exist? exiting.\n")
        sys.exit(1)

    creds = get_profile(args.profile)
    epdata = parse_endpoint(args.endpoint)

    headers = get_signed_headers(
            args.bucket,
            args.query,
            epdata['hostport'],
            region,
            creds['access_key'],
            creds['secret_key'])
    
    #request_url = endpoint + '/weka-lab-tier-vrtx-1?' + canonical_querystring
    request_url = '{0}/{1}?{2}'.format(args.endpoint, args.bucket, 'search='+urllib.parse.quote_plus(args.query))
    r = requests.get(request_url, headers=headers)
    
    if args.output == 'xml':
        print_xml(r.text)
    elif args.output == 'raw':
        print_raw(r.text)
    elif args.output == 'json':
        print_json(r.text)
    elif args.output == 'csv':
        print_csv(r.text)
    else: print(r.text)

    
