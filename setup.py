import sys
from setuptools import setup

setup(name='s34me',
	packages=['s3'],
	version='0.0.8',
	description='s34me is a high-level wrapper for handling s3 objects in boto3 and Pandas',
	author='leon yin',
	author_email='ly501@nyu.edu',
	url='https://github.com/yinleon/s3',
	keywords='aws s3 boto3 pandas dataframe',
	license='MIT',
	install_requires=[
		'pandas<=0.19.2',
		'boto3>=1.4.4',
		'botocore>=1.5.7',
		'scikit-learn>=0.18.1'
	]
)
