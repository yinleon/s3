import os
from sklearn.externals import joblib
from .funcs import disk_2_s3, wget

def dump_clf(clf, s3_path):
	'''
	Pickles a Scikit-Learn classifier to local.
	Loads that local file to s3.
	removes the local file.
	'''
	filename = s3_path.split('/')[-1]

	# write the pickle file to disk
	joblib.dump(clf, filename)
	
	# upload the pickle file to s3
	r = disk_2_s3(filename, s3_path)
	
	# remove the local pickle file.
	os.remove(filename)
	
	return r

def load_clf(s3_path):
	'''
	Downloads a pickled classifier from s3.
	unpickles it into a Scikit-Learn classifer.
	removes the pickled classifier.
	'''
	filename = s3_path.split('/')[-1]
	
	# download the sklearn pickled model locally
	wget(s3_path, filename)

	# load the pickle as a sklearn classifier
	clf = joblib.load(filename)

	# remove the local file.
	os.remove(filename)
	
	return clf
