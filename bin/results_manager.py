# -*- coding: utf-8 *-*
"""
Module for logging and saving
"""
from datetime import datetime
import json
import uuid
import os
import logging
import sys

LEVELS = {'info': logging.INFO, 'debug': logging.DEBUG}

class ResultsManager:
    """
    If options are provided, then a log file is created and saved under:
        parentdir/date/uuid/log_filename

    The date and uuid is internally generated.
    """
    def __init__(self,
        options={
            'stdout_only': True,
            'level': 'info',
            'parentdir': str,
            'log_filename': str
        }
    ):
        self.logger = logging
        self.level = LEVELS[options['level']]
        if not options['stdout_only']:
            self._parentdir = os.path.normpath(options['parentdir'])
            self._date = datetime.now().strftime("%Y-%m-%d")
            self._uuid = str(uuid.uuid4())
            self._results_dir = os.path.join(self._parentdir, "results")
            self._dated_results_dir = os.path.join(
                self._results_dir, self._date
            )
            self._unique_dated_results_dir = os.path.join(
                self._dated_results_dir, self._uuid
            )
            mkdir_if_not_exists(self._results_dir)
            mkdir_if_not_exists(self._dated_results_dir)
            mkdir_if_not_exists(self._unique_dated_results_dir)
            self._log_file = os.path.join(
                self._unique_dated_results_dir, options['log_filename']
            )
            self.configure_logger(self.level, self._log_file)
        else:
            self.configure_logger(self.level, False)

    def configure_logger(self, level='info', filename=None):
        if filename:
            self.logger.basicConfig(
                filename=self._log_file,
                level=self.level,
                format="%(asctime)s - %(levelname)s - %(message)s",
                datefmt="%m/%d/%Y %I:%M:%S %p",
            )
        else:
            self.logger.basicConfig(
                stream=sys.stdout,
                level=self.level,
                format="%(asctime)s - %(levelname)s - %(message)s",
                datefmt="%m/%d/%Y %I:%M:%S %p",
            )

def mkdir_if_not_exists(dirname):
    if not os.path.isdir(dirname):
        os.mkdir(dirname)

def write_dict_to_json(filename, data):
    """Write an in-memory Python dictionary to a formatted .json file."""
    filename = os.path.normpath(filename)
    with open(filename, "w") as file_obj:
        json.dump(data, file_obj)
