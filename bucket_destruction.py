#!/usr/bin/python3
import os
import boto3
import argparse
import sys
from multiprocessing import Pool

PROFILE_DEF="default"
SHADUP=False
LOGSTUFF=True

def just_go(args):

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client('s3', endpoint_url=args.endpoint)

    zap_objects(args, s3)
    zap_mpus(args, s3)

    return True


def zap_objects(args, s3):

    lspgntr = s3.get_paginator('list_object_versions')

    if args.prefix:
        page_iterator = lspgntr.paginate(Bucket=args.bucket, Prefix=args.prefix)
    else:
        page_iterator = lspgntr.paginate(Bucket=args.bucket)

    for ovs in page_iterator:

        objs = {'Objects': [], 'Quiet': False}

        try:
            for ver in ovs["Versions"]:
                if args.noncurrent and ver["IsLatest"]:
                    logme("skipping {0} ({1}".format(ver['Key'], ver['VersionId']))
                    continue
                objs['Objects'].append({'Key': ver['Key'], 'VersionId': ver['VersionId']})
                logme("deleting {0} ({1}".format(ver['Key'], ver['VersionId']))
        except KeyError: pass
        except Exception as e:
            sys.stderr.write(e)

        if not args.skipmarkers:

            try:
                for ver in ovs["DeleteMarkers"]:
                    objs['Objects'].append({'Key': ver['Key'], 'VersionId': ver['VersionId']})
                    logme("deleting marker {0} ({1}".format(ver['Key'], ver['VersionId']))
            except KeyError:
                pass
            except Exception as e:
                sys.stderr.write(e)

        if len(objs['Objects']) == 0:
            logme("no versions or markers to delete")
        else:
            s3.delete_objects(Bucket=args.bucket, Delete=objs)


def zap_mpus(args, s3):

    lspgntr = s3.get_paginator('list_multipart_uploads')
    page_iterator = lspgntr.paginate(Bucket=args.bucket)

    for mpus in page_iterator:

        if 'Uploads' in mpus:
            logme("Stray/unfinished MPUs found, aborting them....")
            for mpu in mpus['Uploads']:
                s3.abort_multipart_upload(Bucket=args.bucket, Key=mpu["Key"], UploadId=mpu['UploadId'])
                logme("deleted {0}".format(mpu['UploadId']))

def logme(text):
    if LOGSTUFF:
        print(text)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='delete all versions and delete markers in a bucket')
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--profile', default=PROFILE_DEF)
    parser.add_argument('--endpoint', default='https://s3.amazonaws.com')
    parser.add_argument('--ca-bundle', default=False, dest='cabundle')
    parser.add_argument('--prefix', default=False, dest='prefix', help='delete at and beyond prefix')
    parser.add_argument('--noncurrent', action='store_true', help='deletes only non-current objects and markers')
    parser.add_argument('--skipmarkers', action='store_true', help='deletes everything but delete markers')
    parser.add_argument('--quiet', action='store_true', help='supress output')
    args = parser.parse_args()

    # No idea why boto can't get this via API
    if args.cabundle:
        os.environ['AWS_CA_BUNDLE'] = args.cabundle

    if args.quiet:
        LOGSTUFF = False

    just_go(args)

