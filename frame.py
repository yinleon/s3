# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 16:16:28 2016

@author: lyin
"""
import os
import pandas as pd
import botocore
import boto3
from .funcs import get_both, disk_2_s3, open

def read_csv(s3_path, sep=',', compression='infer', nrows=None, skiprows = None, 
             header='infer', names=None, index_col=None, usecols=None, engine=None, 
             parse_dates=False, lineterminator=None, escapechar=None, encoding=None,
             iterator=False, chunksize=None,dtype=None, low_memory=True):
    '''
    Read a csv file from s3 into memory in a pandas dataframe
    '''

    try:
        buffer_in_binary = open(s3_path)
    
    except botocore.exceptions.ClientError as e:
        return "Unexpected error: %s" % e
    
    # note should just ask for kwargs.
    return pd.read_csv(filepath_or_buffer=buffer_in_binary, sep=sep, dtype=dtype,
                       compression=compression, nrows=nrows, header=header, 
                       names=names, index_col=index_col, usecols=usecols,
                       engine=engine, parse_dates=parse_dates, skiprows=skiprows,
                       lineterminator=lineterminator, escapechar=escapechar,
                       iterator=iterator, chunksize=chunksize, encoding=encoding,
                       low_memory=low_memory)


def read_json(s3_path, orient=None, typ='frame', dtype=True, convert_axes=True, 
              convert_dates=True, keep_default_dates=True, numpy=False, 
              precise_float=False, date_unit=None):
    '''
    Read a json file from s3 into memory in a pandas dataframe.
    '''                
    
    try:
        buffer_in_binary = open(s3_path)

    except botocore.exceptions.ClientError as e:
        return "Unexpected error: %s" % e
    
    # note should just ask for kwargs.  
    return pd.read_json(path_or_buf=buffer_in_binary , orient=orient, typ=typ, 
                        dtype=dtype, convert_axes=convert_axes, 
                        convert_dates=convert_dates, 
                        keep_default_dates=keep_default_dates, numpy=numpy, 
                        precise_float=precise_float, date_unit=date_unit)

def to_csv(df,s3_path,index=False, compression=None, sep=',', quoting=None, 
           chunksize=None, line_terminator='\n', escapechar=None, date_format=None,
           na_rep='', tupleize_cols=False,encoding=None):
    '''    
    Writes a dataframe to local (transient), then uploads the dataframe to s3.
    '''    
    temp_file_ = s3_path.split('/')[-1]

    # note should just ask for kwargs.
    df.to_csv(temp_file_, index=index, compression=compression, sep=sep,
              quoting=quoting, chunksize=chunksize, line_terminator=line_terminator,
              escapechar=escapechar, date_format=date_format, na_rep=na_rep,
              tupleize_cols=tupleize_cols,encoding=encoding) 
    
    disk_2_s3(temp_file_,s3_path)
    os.remove(temp_file_)
    
    return "File uploaded to '%s'" % s3_path

def to_json(df,s3_path,orient=None,date_format='epoch'):
    '''
    Writes a dataframe to local (transient), then uploads the dataframe to s3.
    '''    
    temp_file_ = s3_path.split('/')[-1]

    df.to_json(temp_file_,orient=orient,date_format=date_format)
    disk_2_s3(temp_file_, s3_path)
    os.remove(temp_file_)
    
    return "File uploaded to '%s'" % s3_path
