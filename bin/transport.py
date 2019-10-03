# -*- coding: utf-8 *-*
"""
This is the summary line.

The python program ```transport.py <watch_dir>```
listens to a directory on the local file system for
new file uploads, or modifications to existing files
in the directory. Upon an upload or modification,
the program sends the file changes to a destination
(the destination path is supplied on command line).

As the only required parameter for the script,
the full path to listen to is given on the command line
No flag is required, just the full path to the directory.

The user can also provide a second path with the
```-dest``` flag. If this is supplied,
the changed files will be written to that directory.
If a destination directory is not given, the program uses
a default ```./bucket1``` path. If this path does not exist,
the program creates it for the user.

Checked with pycodestyle
Checked with pydocstyle
Modified with black
pylint score: 10.0/10.0
imports sorted with isort
"""
import argparse
import boto3
import datetime
import json
import os
import re
import results_manager
import tarfile
import threading
import time
import uuid
from queue import Queue
from botocore.exceptions import ClientError


dirname, filename = os.path.split(os.path.abspath(__file__))

IO_OPTIONS = {
    'stdout_only': False, 'level': 'debug',
    'parentdir': '{}'.format(dirname),
    'log_filename': 'snapshot_change.log'
}

log_manager = results_manager.ResultsManager(IO_OPTIONS)

logger = log_manager.logger

SNAPSHOT_FILES = ["snapshot.gc", "snapshot.meta", "snapshot.meta.bkp",
    "snapshot.state", "snapshot.state.bkp"
]
WATCH_INTERVAL = 15.0 #seconds


def watch_directory(snapshot_directory, incomplete_snapshot_queue):
    """Watch bucket_dir for changes & send new or modified json to a queue."""
    current_fs = [
        os.path.join(os.path.abspath(snapshot_directory), i)
        for i in os.listdir(snapshot_directory)
        if i in SNAPSHOT_FILES
    ]

    f_mod_times = {
        i: os.stat(i).st_mtime for i in current_fs
    }

    while True:

        new_uploads = {
            os.path.join(os.path.abspath(snapshot_directory), i)
            for i in os.listdir(snapshot_directory)
            if i in SNAPSHOT_FILES
        }.difference(set(current_fs))

        if new_uploads:

            for new_file in new_uploads:

                current_fs.append(new_file)

                f_mod_times[new_file] = os.stat(
                    new_file
                ).st_mtime

                logger.debug("New or modified file: {}".format(new_file))
                send_to_queue(incomplete_snapshot_queue, new_file)

        for mod_file in f_mod_times:

            # in case someone deleted one of the files
            if os.path.isfile(mod_file):

                if (
                        get_delta(
                            f_mod_times[mod_file],
                            os.stat(mod_file).st_mtime,
                        )
                        > 0
                ):
                    current_fs.remove(mod_file)

        time.sleep(1.0)


def send_to_queue(queue_container, obj):
    """Send an object to a queue."""
    queue_container.put(obj)


def get_new_files_from_queue(
    incomplete_snapshot_queue,
    complete_snapshot_queue):
    """
    Get a new .json upload from a queue and send to another queue.

    Timeout on the queue get method after 12 hours
    """
    new_snapshot_files = []

    while True:

        new_file = incomplete_snapshot_queue.get(timeout=43200.00)

        if new_file and new_file not in new_snapshot_files:

            new_snapshot_files.append(new_file)

        if len(new_snapshot_files) == 5:

            send_to_queue(complete_snapshot_queue, new_snapshot_files)

            new_snapshot_files = []

def ship_snapshot(client, complete_snapshot_queue, destination, s3bucket_name):
    """ship_snapshot files received from nodes."""
    while True:

        new_files = complete_snapshot_queue.get( # a list
            timeout=43200.00 # block for this amount of time
        )
        # after a get is succcessful
        today_date = datetime.datetime.now().strftime(
            "%Y-%m-%d-%M-%S"
        )

        _uuid = str(uuid.uuid4())

        for file_name in new_files:

            fname = "{}-{}-{}".format(
                today_date, _uuid, file_name.split(os.sep)[-1]
            )

            if client is None:
                local_write_file(client, destination, _uuid, fname)
            else:
                new_file_name = os.path.join(destination, fname)

                success = upload_file(
                    client, file_name, s3bucket_name, object_name=fname
                )
                logger.debug(
                "Uploaded file {} to {} as {} = {}".format(
                    file_name, s3bucket_name, fname, success
                    )
                )

def local_write_file(client, destination, _uuid, new_file_name):
    # pretend like this is a signature
    MAKE_TAR = True
    uuid_pattern = r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}'
    re_uuid = re.compile(uuid_pattern, re.I).findall(new_file_name)

    try:
        _uuid == re_uuid.pop()
        new_snapshot_dir = "./bucket1/{}".format(_uuid)

        mkdir_ifnot_exists(new_snapshot_dir)

        if MAKE_TAR:
            compress_dir(
                "/Users/cook/Helix/pendulum-1/mainnet",
                new_snapshot_dir, _uuid
            )
        else:
            new_file_name = os.path.join(new_snapshot_dir, new_file_name)
            with open(new_file_name, "a") as _f:
                _f.write(new_file_name)

    except IndexError as error:
        print("Pattern does not match!")



def compress_dir(dir_to_tar, dest_to_tar, tarname):
    tar = tarfile.open(
        "{}.tar.gz".format(os.path.join(dest_to_tar, tarname)),
        "w:gz"
    )
    tar.add("{}".format(os.path.normpath(dir_to_tar)), arcname=tarname)
    tar.close()

def upload_file(client, fname_on_disk, bucket, object_name=None):
    """Upload a file to an S3 bucket
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = fname_on_disk
    # Upload the file
    try:
        response = client.upload_file(fname_on_disk, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def mkdir_ifnot_exists(dir_path):
    """Make a directory if it doesn't exist."""
    if not os.path.isdir(dir_path):

        os.mkdir(dir_path)

def dtobj_from_str(time_string):
    """Get a datetime object from an appropriately formatted string."""
    dt_obj = datetime.datetime.strptime(
        time_string, "%Y/%m/%d %H:%M:%S.%f"
    )

    return dt_obj


def ms_to_dtstr(time_stamp):
    """Get a datetime string from a *nix ms timestamp."""
    tstamp_to_string = datetime.datetime.fromtimestamp(
        time_stamp
    ).strftime("%Y/%m/%d %H:%M:%S.%f")

    return tstamp_to_string


def get_delta(send_timestamp, receive_timestamp1):
    """Get the change in time between two timestamps."""
    delta = dtobj_from_str(
        ms_to_dtstr(receive_timestamp1)
    ) - dtobj_from_str(ms_to_dtstr(send_timestamp))

    return delta.total_seconds()


if __name__ == "__main__":

    DEBUG = True

    PARSER = argparse.ArgumentParser(
        description=""
    )

    PARSER.add_argument(
        "watch_dir",
        metavar="Path of folder to watch for snapshot changes",
        type=str,
        help="The watch_dir on local filesystem to listen to for changes."
    )

    PARSER.add_argument(
        "dest",
        metavar="",
        type=str,
        default="./bucket1",
        help=""
    )

    PARSER.add_argument(
        "-access_key",
        metavar="access_key",
        type=str,
        default=None,
        help=""
    )

    PARSER.add_argument(
        "-secret_key",
        metavar="secret_key",
        type=str,
        default=None,
        help=""
    )
    PARSER.add_argument(
        "-aws_bucket",
        metavar="aws_bucket",
        type=str,
        default=None,
        help=""
    )
    PARSER.add_argument(
        "-region_name",
        metavar="region_name",
        type=str,
        default="eu-central-1",
        help=""
    )
    PARSER.add_argument(
        "-debug",
        metavar="debug",
        type=lambda s: s.lower() in ['true', 't', 'yes', '1'],
        default="True",
        help=""
    )

    ARGS = PARSER.parse_args()

    snapshot_directory = os.path.normpath(
        ARGS.watch_dir
    )

    destination_directory = os.path.normpath(
        ARGS.dest
    )

    incomplete_snapshot_queue = Queue()

    complete_snapshot_queue = Queue()

    if ARGS.debug:
        dirname, filename = os.path.split(os.path.abspath(__file__))
        destination_directory = (dirname, './bucket1')
        client = None

    elif not (ARGS.access_key, ARGS.secret_key, ARGS.aws_bucket) == (
        None, None, None
    ):
        client = boto3.client(
            's3',
            aws_access_key_id=ARGS.access_key,
            aws_secret_access_key=ARGS.secret_key,
            region_name=ARGS.region_name,
        )

    observe_dir_thread = threading.Thread(
        target=watch_directory,
        args=(snapshot_directory, incomplete_snapshot_queue,),
    )

    queue_moveto_complete_snapshot_thread = threading.Thread(
        target=get_new_files_from_queue,
        args=(incomplete_snapshot_queue, complete_snapshot_queue,),
    )

    ship_snapshot_thread = threading.Thread(
        target=ship_snapshot,
        args=(client, complete_snapshot_queue,
            destination_directory, ARGS.aws_bucket),
    )

    observe_dir_thread.start()

    queue_moveto_complete_snapshot_thread.start()

    ship_snapshot_thread.start()
