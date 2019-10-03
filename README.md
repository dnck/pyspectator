# Transports On the Cloud

# Quickstart

* ```python transport.py ./snapshots /nominee1 -access_key abcdef -secret_key abcdef -aws_bucket my_bucketname -aws_region my_s3_region```

This will deposit the files in the local directory, ```./snapshot``` into the s3 bucket, ```aws_bucket```, and name them something like, ```nominee1/date-seconds-uuid-filename```. Snapshots can be colocated within the bucket by their uuid.

## Introduction

The python program ```transport.py <full_path_to_folder>``` listens to a directory on the local file system for file uploads, or modifications to existing files in the directory. Upon an upload or modification, the program summarizes the content of the file, and writes the result to a file in a different directory (the destination path is optionally supplied on command line).

The full path to listen to is given on the command line as the only required parameter for the script. No flag is required, just the full path to the directory.

The user can also provide a second path with the ```-dest_folder``` flag. If this is supplied, the new files will be written to that directory. If a destination directory is not given, the program uses a default ```./bucket1``` path. If this path does not exist, the program creates it for the user.

## Requirements
The program uses only standard Python 3.6 packages.

## Usage
From within the ./bin directory (when debugging):

* ```python transport.py bucket0```

Once the program is running, the user can paste new snapshot files into ```./bucket0``` (or whatever other directory was supplied at execution), or modify the existing snapshot files in the ```./bucket0``` directory (if that was the directory supplied).

After doing so, a new file will be created in the ```./bucket1``` directory. The files are stored in the following format: ```uuid/original_fname-YEAR-M-D-uuid.txt```, where "original_fname" is the original uploaded file name, and uuid is a unique identifier in the case of repeat uploads or modifications to the same file, and the file ending changes accordingly.

*Please note there is a timeout on the get() method of the queue objects in the transports.py script of 12 hours. If no events occur in the ./bucket0 directory during these 12 hours, the function will break.*

## Tests
The test directory contains only one test for multiple records. A ```test.json``` file is provided, which can be uploaded to ```./bucket0``` while the transport.py is running by typing (from within the ```./simulation``` directory):

* ```cp ./test/test.json ./bucket0/test.json```

The program was tested on Linux, MacOS, and Windows OS.
