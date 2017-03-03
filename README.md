# S3 helper
This is a module that is helpful both in a development notebooks and deployed production pipelines that work with unstructured s3 files.

The main use of this module is to programmatically, preview, process, and edit files around s3 by:

listing contents of s3 buckets using glob-like RegEx patterns.<br>
moving or copying files between buckets (filedrop -> archives).<br>
streaming csv and json files into Pandas dataframes on your local machine, 
without manually downloading them to disk.<br>
writing Pandas dataframes to csv and json files on s3.<br>
loading and unloading scikit-learn models from s3.

Pandas and Scikit-Learn and useful tools in the Python Data ecosystem.<br>
Check out the <a href='http://nbviewer.jupyter.org/github/yinleon/s3/blob/master/tutorial.ipynb'>tutorial</a> and see the module in action.


## Installation
Configure s3 as you would for boto3.
<a href="http://boto3.readthedocs.io/en/latest/guide/configuration.html">read here</a><br>
TLDR; Environment Variables or configuring AWS CLI work best.

## Usage
Install requirements
```pip install -r requirements.txt```

Either use in a local directory or<br>
In terminal- copy the `$PATH` (consider this a variable) of the module directory using: `pwd|pbcopy`

Add the module to the to PYTHONPATH

```
export PYTHONPATH="${PYTHONPATH}:$PATH"
```

To use in iPython environments like Jupyter
you can include these lines in each script:
```
import sys
sys.path.append("$PATH")
import s3

df = s3.read_csv('s3://bucket_name/key_name/file_name.tsv.gz', 
                 sep='\t', compression='gzip')
```

For continued use, the `$PATH` should be added to the iPython startup script

```
cd ~/.ipython/profile_default/startup
vim first.py
sys.path.append("PATH")
```


## Contributing
1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## Credits
Written by Leon Yin

## License
MIT