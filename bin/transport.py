# -*- coding: utf-8 *-*
"""
This is the summary line.

The python program ```transport.py <full_path_to_folder>```
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
import datetime
import json
import os
import re
import threading
import time
import uuid
from queue import Queue


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

                print(new_file)
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

def ship_snapshot(complete_snapshot_queue, destination):
    """ship_snapshot files received from nodes."""
    while True:

        new_files = complete_snapshot_queue.get( # a list
            timeout=43200.00 # block for this amount of time
        )
        # after a get is succcessful
        today_date = datetime.datetime.now().strftime(
            "%Y-%m-%d-%s"
        )

        _uuid = str(uuid.uuid4())

        for file_name in new_files:

            fname = "{}-{}-{}".format(
                today_date, _uuid, file_name.split(os.sep)[-1]
            )

            # new_file_name = os.path.join(
            #     destination, fname
            # )

            send_file(destination, _uuid, fname)

def send_file(destination, _uuid, new_file_name):
    # pretend like this is a signature
    uuid_pattern = r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}'
    re_uuid = re.compile(uuid_pattern, re.I).findall(new_file_name)
    try:
        _uuid == re_uuid.pop()
        new_snapshot_dir = "./bucket1/{}".format(_uuid)
        mkdir_ifnot_exists(new_snapshot_dir)
    except IndexError as error:
        print("Pattern does not match!")
    new_file_name = os.path.join(new_snapshot_dir, new_file_name)
    with open(new_file_name, "a") as _f:
        _f.write(new_file_name)


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
        description="Program listens for new .json files"
        + " in a directory, calculates aggregrates for the file,"
        + " and writes the result to a new .json file in a seperate directory."
    )

    PARSER.add_argument(
        "full_path_to_folder",
        metavar="Path of folder to listen to"
        + " for new json uploads",
        type=str,
        help="The full_path_to_folder on local filesystem"
        + " to listen to for uploads.",
    )

    PARSER.add_argument(
        "-dest",
        metavar="Path to folder to write"
        + " new summary .json files",
        type=str,
        default="./bucket1",
        help="The full path on the local"
        + " filesystem to write the summary results to.",
    )

    ARGS = PARSER.parse_args()

    snapshot_directory = os.path.normpath(
        ARGS.full_path_to_folder
    )

    destination_directory = os.path.normpath(
        ARGS.dest
    )

    incomplete_snapshot_queue = Queue()

    complete_snapshot_queue = Queue()

    if DEBUG:
        dirname, filename = os.path.split(os.path.abspath(__file__))
        destination_directory = (dirname, './bucket1')

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
        args=(complete_snapshot_queue, destination_directory),
    )

    observe_dir_thread.start()

    queue_moveto_complete_snapshot_thread.start()

    ship_snapshot_thread.start()