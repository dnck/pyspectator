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


def main(client, snapshot_dir, aws_bucket, snapshot_interval):
    while True:
        _uuid = str(uuid.uuid4())
        now_date = datetime.datetime.now().strftime(
            "%Y-%m-%d-%M-%S"
        )
        time_stamp = now_date + "-" + _uuid
        tar_orginal_name = time_stamp+".tar.gz"

        tarname = compress_dir(snapshot_dir, time_stamp)
        assert(os.path.join("tmp", tar_orginal_name) == tarname)

        success = upload_dir(
           client, tarname, aws_bucket, object_name=tar_orginal_name
        )

        if success:
            logger.info("Shipped tar file: {}".format(tarname))
            os.remove(tarname)
        time.sleep(snapshot_interval)


def compress_dir(dir_to_tar, tarname):
    tarname = "{}.tar.gz".format(os.path.join("tmp", tarname))
    tar = tarfile.open(
        tarname,
        "w:gz"
    )
    tar.add("{}".format(os.path.normpath(dir_to_tar)), arcname=tarname)
    tar.close()
    return tarname

def upload_dir(client, tarname, aws_bucket, object_name=None):
    """Upload a file to an S3 bucket
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = tarname
    # Upload the file
    try:
        response = client.upload_file(tarname, aws_bucket, object_name)
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
        "-snapshot_dir",
        metavar="Path of folder to watch for snapshot changes",
        type=str,
        help="The snapshot_dir on local filesystem to listen to for changes."
    )
    PARSER.add_argument(
        "-snapshot_interval",
        metavar="snapshot_interval",
        type=float,
        default=600.0,#10 minutes
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

    ARGS = PARSER.parse_args()

    mkdir_ifnot_exists("tmp")

    snapshot_dir = ARGS.snapshot_dir

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

    aws_bucket = ARGS.aws_bucket

    main(client, snapshot_dir, aws_bucket, snapshot_interval)
