import argparse
import tarfile
import time
import os
import datetime
import uuid
import results_manager
import boto3

dirname, filename = os.path.split(os.path.abspath(__file__))


IO_OPTIONS = {
    'stdout_only': True, 'level': 'debug',
    'parentdir': '{}'.format(dirname),
    'log_filename': 'snapshot_change.log'
}

log_manager = results_manager.ResultsManager(IO_OPTIONS)

logger = log_manager.logger


def main(client, watch_dir, upload_bucket, snapshot_interval):
    while True:
        _uuid = str(uuid.uuid4())
        now_date = datetime.datetime.now().strftime(
            "%Y-%m-%d-%M-%S"
        )
        tarname = now_date + "-" + _uuid
        tarname = compress_dir(watch_dir, "backups", tarname)
        print(watch_dir, '\n', tarname, '\n', upload_bucket, '\n')
        # success = upload_dir(
        #     client, tarname, upload_bucket, object_name=tarname
        # )
        # if success:
        #     logger.info("Shipped tar file: {}".format(tarname))
        #     os.remove(tarname)
        # time.sleep(snapshot_interval)


def compress_dir(dir_to_tar, dest_to_tar, tarname):
    tarname = "{}.tar.gz".format(os.path.join(dest_to_tar, tarname))
    tar = tarfile.open(
        tarname,
        "w:gz"
    )
    tar.add("{}".format(os.path.normpath(dir_to_tar)), arcname=tarname)
    tar.close()
    return tarname

def upload_dir(client, fname_on_disk, bucket, object_name=None):
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

if __name__ == "__main__":

    PARSER = argparse.ArgumentParser(
        description=""
    )
    PARSER.add_argument(
        "-watch_dir",
        metavar="Path of folder to watch for snapshot changes",
        type=str,
        help="The watch_dir on local filesystem to listen to for changes."
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
        "-snapshot_interval",
        metavar="snapshot_interval",
        type=float,
        default=600.0,#10 minutes
        help=""
    )

    ARGS = PARSER.parse_args()

    mkdir_ifnot_exists("backups")

    watch_dir = ARGS.watch_dir

    snapshot_interval = ARGS.snapshot_interval

    if not (
        ARGS.access_key,
        ARGS.secret_key,
        ARGS.aws_bucket
        ) == (
        (
        None,
        None,
        None
        )
    ):
        client = boto3.client(
            's3',
            aws_access_key_id=ARGS.access_key,
            aws_secret_access_key=ARGS.secret_key,
            region_name=ARGS.region_name,
        )
    else:
        raise ValueError("Missing input parameters")

    upload_bucket = ARGS.aws_bucket

    main(client, watch_dir, upload_bucket, snapshot_interval)
