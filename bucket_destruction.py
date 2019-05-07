#!/usr/bin/python
import os
import boto3
import argparse


PROFILE_DEF="default"

def just_go(args):

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client('s3', endpoint_url=args.endpoint)
        
    response = s3.list_objects(Bucket=args.bucket)
    
    if 'Contents' in response:
        print("bucket not empty - delete objects and then run this script to remove all old\nversions")
    else:
    
        paginator = s3.get_paginator('list_object_versions')
        page_iterator = paginator.paginate(Bucket=args.bucket)

        for ovs in page_iterator:
            
            objs = {'Objects': [], 'Quiet': False}
            
            try:
                for ver in ovs["DeleteMarkers"]:
                    objs['Objects'].append({'Key': ver['Key'], 'VersionId': ver['VersionId']})
                    print("deleting marker {0} ({1}".format(ver['Key'], ver['VersionId']))
            except KeyError:
                pass
            except Exception as e:
                print(e)
                
            try:
                for ver in ovs["Versions"]:
                    objs['Objects'].append({'Key': ver['Key'], 'VersionId': ver['VersionId']})
                    print("deleting {0} ({1}".format(ver['Key'], ver['VersionId']))
            except KeyError:
                pass
            except Exception as e:
                print(e)
              
            s3.delete_objects(Bucket=args.bucket, Delete=objs)
            
    
    paginator = s3.get_paginator('list_multipart_uploads')
    page_iterator = paginator.paginate(Bucket=args.bucket)
    
    for mpus in page_iterator:
    
        if 'Uploads' in mpus:
            print("Stray/unfinished MPUs found, aborting them....")
            for mpu in mpus['Uploads']:
                s3.abort_multipart_upload(Bucket=args.bucket, Key=mpu["Key"], UploadId=mpu['UploadId'])
                print("deleted {0}".format(mpu['UploadId']))    
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='delete all versions and delete markers in a bucket')
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--profile', default=PROFILE_DEF)
    parser.add_argument('--endpoint', default='https://s3.amazonaws.com')
    parser.add_argument('--cabundle', default=False)
    args = parser.parse_args()

    just_go(args)
