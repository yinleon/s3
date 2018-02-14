
import os
import pandas as pd
import botocore
import boto3
from .funcs import get_both, disk_2_s3, open, read

def read_csv(s3_path, *args, **kwargs):
    '''
    Read a csv file from s3 into memory in a pandas dataframe
    '''
    try:
        buffer_in_binary = open(s3_path)
    
    except botocore.exceptions.ClientError as e:
        return "Unexpected error: %s" % e
    
    # note should just ask for kwargs.
    return pd.read_csv(buffer_in_binary, *args, **kwargs)


def read_json(s3_path, *args, **kwargs):
    '''
    Read a json file from s3 into memory in a pandas dataframe.
    '''                
    try:
        buffer_in_binary = read(s3_path)

    except botocore.exceptions.ClientError as e:
        return "Unexpected error: %s" % e
    
    # note should just ask for kwargs.  
    return pd.read_json(buffer_in_binary, *args, **kwargs)

def to_csv(df, s3_path, *args, **kwargs):
    '''    
    Writes a dataframe to local (transient), then uploads the dataframe to s3.
    '''    
    temp_file_ = s3_path.split('/')[-1]

    # note should just ask for kwargs.
    df.to_csv(temp_file_, *args, **kwargs) 
    
    disk_2_s3(temp_file_,s3_path)
    os.remove(temp_file_)
    
    return "File uploaded to '%s'" % s3_path

def to_json(df, s3_path, *args, **kwargs):
    '''
    Writes a dataframe to local (transient), then uploads the dataframe to s3.
    '''    
    temp_file_ = s3_path.split('/')[-1]

    df.to_json(temp_file_, *args, **kwargs)
    disk_2_s3(temp_file_, s3_path)
    os.remove(temp_file_)
    
    return "File uploaded to '%s'" % s3_path
