
import os
import pandas as pd
import botocore
import boto3
from .funcs import get_both, disk_2_s3, open_file, read

def read_csv(s3_path, *args, **kwargs):
    '''
    Read a csv file from s3 into memory in a pandas dataframe
    '''
    return pd.read_csv(s3_path, *args, **kwargs)


def read_json(s3_path, encoding="utf-8", bytes=True, *args, **kwargs):
    '''
    Read a json file from s3 into memory in a pandas dataframe.
    '''                    
    return pd.read_json(s3_path, *args, **kwargs)

def to_csv(df, s3_path, acl='private', *args, **kwargs):
    '''    
    Writes a dataframe to local (transient), then uploads the dataframe to s3.
    '''    
    temp_file_ = s3_path.split('/')[-1]

    # note should just ask for kwargs.
    df.to_csv(temp_file_, *args, **kwargs) 
    
    disk_2_s3(temp_file_, s3_path, acl=acl)
    os.remove(temp_file_)
    
    return "File uploaded to '%s'" % s3_path

def to_json(df, s3_path, acl='private', *args, **kwargs):
    '''
    Writes a dataframe to local (transient), then uploads the dataframe to s3.
    '''    
    temp_file_ = s3_path.split('/')[-1]

    df.to_json(temp_file_, *args, **kwargs)
    disk_2_s3(temp_file_, s3_path, acl=acl)
    os.remove(temp_file_)
    
    return "File uploaded to '%s'" % s3_path
