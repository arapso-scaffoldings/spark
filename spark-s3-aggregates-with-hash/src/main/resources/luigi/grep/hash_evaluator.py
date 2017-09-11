import hashlib
import logging
from os import path
from os import environ
from os.path import pardir

from luigi import Task
from luigi.contrib.spark import SparkSubmitTask
from luigi.s3 import S3Client


def fmt_day(date):
    return "{date:%Y-%m-%d}".format(date=date)


def jar(rel_path):

    project_dir = path.join(environ['PYTHONPATH'], pardir)
    return path.join(path.abspath(project_dir), "luigi-jars", rel_path)


def _gen_hash(files):
    sorted_files = sorted(files)
    content_to_hash = hashlib.md5()
    for logs_file_name in sorted_files:
        content_to_hash.update(logs_file_name)
    return content_to_hash.hexdigest()


def _filter_files(files, file_name_filter):
    if not file_name_filter:
        return files
    temp = list()
    for file in files:
        if file_name_filter in file:
            temp.append(file)
    return temp


def _generate_s3_files_hash(s3, input_path, log_types, date, country):
    files = list()
    for log_type in log_types:
        files_path = path.join(input_path, log_type, fmt_day(date))
        files.extend(list(s3.listdir(files_path)))

    if country and country != "ALL":
        country_files = _filter_files(files, "tracker_%s_" % country)
        return _gen_hash(country_files)
    else:
        return _gen_hash(files)


def _get_old_hash(s3, hash_path):
    if s3.exists(hash_path):
        return s3.get_as_string(hash_path).strip()
    else:
        return None


def _put_new_hash(s3, hash_path, hash_value):
    logging.info("Save hash '%s' to path '%s'" % (hash_value, hash_path))
    s3.put_string(hash_value, hash_path)


def _get_hash_file_path(aggregates_path):
    """
    Hash file for addresses.parquet will be addresses.hash
    """
    parent_path = path.dirname(aggregates_path)
    file_name = path.splitext(path.basename(aggregates_path))[0]
    return path.join(parent_path, '%s.hash' % file_name)


def check_if_hashes_equals(old_aggr_exists, remote_logs_base, log_types, date, country, aggregates_path):
    """
    Compare
        md5 hash stored next to aggregates (for devices.parquet => devices.hash)
            with
        md5 hash generated from list of logs files names from s3
        that should be used to generate those hashes
    If hashes are not equal or hash file does not exist
    method removes aggregates file from s3 (aggregates_path) and return False
    if hashes are equal return TRUE

    :param old_aggr_exists: Aggregates already exists on s3
    :param remote_logs_base: raw roqad logs base path
    :param log_types: arrays with log_types ["pro","det"]
    :param date: date from which logs will be downloaded
    :param country: logs country to filter roqad logs
    :param aggregates_path: results files path where results will be stored

    :return: TRUE if previous hash exists and it is equal with evaluated logs files hash
    """
    s3_client = S3Client()
    eval_hash = _generate_s3_files_hash(s3_client, remote_logs_base, log_types, date, country)
    hash_path = _get_hash_file_path(aggregates_path)
    if not old_aggr_exists:
        _put_new_hash(s3_client, hash_path, eval_hash)
        return False

    old_hash = _get_old_hash(s3_client, hash_path)
    logging.info("Evaluated hash: '{}' - Saved hash: '{}'".format(eval_hash, old_hash))
    if old_hash is not None and old_hash == eval_hash:
        return True

    logging.warn("Logs files have changed. Aggregates from '{}' will be removed.".format(aggregates_path))

    s3_client.remove(aggregates_path)
    if old_hash is not None:
        s3_client.remove(hash_path, recursive=False)
    _put_new_hash(s3_client, hash_path, eval_hash)

    return False


class SparkCopyAggregatesDataTask(SparkSubmitTask):
    """
    A basic task, which is responsible for copying aggregates from s3 to hdfs.
    Task is mark as completed if aggregates are available at DST target and
    aggregates was generated from latest logs files.
    To prove aggregates are generated from latest logs
    hash file saved next to aggregates with "*.hash" extension is checked
    against list of calculated logs files names hash stored on s3.
    """
    app = jar("common-s3-sync-0.0.1-SNAPSHOT.jar")
    entry_class = "pl.roqad.common.s3.sync.RecursiveCopy"

    def app_options(self):
        return ["--from-path", self.src(), "--to-path", self.dst()]

    def complete(self):
        return check_if_hashes_equals(old_aggr_exists=Task.complete(self),
                                      remote_logs_base=self.remote_logs_base,
                                      log_types=self.log_types,
                                      date=self.date,
                                      country=self.country,
                                      aggregates_path=self.dst())

    def output(self):
        return spark.targets.AutoTarget(path.join(self.dst()))

    def src(self):
        """
        A method which must be overridden, and it should return
        source for the copy task.
        :return: the source for the copy task
        """
        raise NotImplementedError()

    def dst(self):
        """
        A method which must be overridden, and it should return
        destination for the copy task.
        :return: the destination for the copy task
        """
        raise NotImplementedError()

    @property
    def num_executors(self):
        return 16
