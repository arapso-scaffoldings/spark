import luigi
from luigi.contrib.spark import SparkSubmitTask
from luigi.local_target import LocalTarget
from luigi.contrib.simulate import RunAnywayTarget
from luigi import Parameter, Task
from luigi.contrib.s3 import S3Client, S3Target
from luigi.util import requires
import os.path
import hashlib
import tempfile
from boto.exception import S3ResponseError


def gen_hash(files):
    a = hashlib.md5()
    for file in files:
        a.update(file)
    return a.hexdigest()


def filter_files(files, file_name_filter):
    temp = list()
    for file in files:
        if file_name_filter in file:
            temp.append(file)
    return sorted(temp)


def get_hash_s3_files(input_path, file_name_filter):
    s3 = S3Client()
    files = s3.listdir(input_path)
    return gen_hash(filter_files(files, file_name_filter))


def get_old_hash(path):
    try:
        s3 = S3Client()
        hash_local_file = tempfile.mktemp()
        s3.get("%s/hash.txt" % path, hash_local_file)
        with open(hash_local_file, 'r') as f:
            return f.readline().strip()
    except S3ResponseError:
        return None


def put_new_hash(path, hash):
    hash_local_file = tempfile.mktemp()
    with open(hash_local_file, 'w') as f:
        f.write(hash + "\n")
    s3 = S3Client()
    s3.put(hash_local_file, "%s/hash.txt" % path)


def remove_old_aggr(path):
    s3_client = S3Client()
    s3_client.remove(path, recursive=True)


class CopyFilesFromS3(SparkSubmitTask):
    s3_logs_path = Parameter(default="s3://roqad-test-de/damian/logs/2017-08-01/")

    s3_logs_filter = Parameter(default="tracker_PL")

    local_logs_path = Parameter(default="processing/")

    app = os.path.expanduser(
        '~/playground/spark/maven-exaple/target/spark-maven-example-0.1-SNAPSHOT-jar-with-dependencies.jar')
    entry_class = 'pl.arapso.examples.spark.CopyS3ToLocal'

    def app_options(self):
        return [self.s3_logs_path, self.local_logs_path, self.s3_logs_filter]

    def output(self):
        return LocalTarget(self.local_logs_path)


@requires(CopyFilesFromS3)
class GrepInputLines(SparkSubmitTask):

    output_path = Parameter(default="output")

    app = os.path.expanduser('~/playground/spark/maven-exaple/target/spark-maven-example-0.1-SNAPSHOT-jar-with-dependencies.jar')
    entry_class = 'pl.arapso.examples.spark.SparkGrepScala'

    def app_options(self):
        return [self.local_logs_path + "*", self.output_path, 'damian']

    def output(self):
        return LocalTarget(self.output_path)


@requires(GrepInputLines)
class PublishGreppedLines(SparkSubmitTask):
    s3_aggr_path = Parameter(default="s3://roqad-test-de/damian/aggr/2017-08-01")

    app = os.path.expanduser(
        '~/playground/spark/maven-exaple/target/spark-maven-example-0.1-SNAPSHOT-jar-with-dependencies.jar')
    entry_class = 'pl.arapso.examples.spark.CopyLocalToS3'

    def app_options(self):
        return [self.output_path, self.s3_aggr_path]

    def complete(self):
        files_uploaded = Task.complete(self)

        if not files_uploaded:
            return False

        eval_hash = get_hash_s3_files(self.s3_logs_path, "tracker_PL_")
        print "Evaluated hash. '%s'" % eval_hash

        old_hash = get_old_hash(self.s3_aggr_path)
        print "Saved files hash. '%s'" % old_hash

        if old_hash is None:
            put_new_hash(self.s3_aggr_path, eval_hash)
            return True

        if old_hash == eval_hash:
            print "Equal"
            return True

        print("Files from {} will be removed".format(self.s3_aggr_path))
        remove_old_aggr(self.s3_aggr_path)

        return False

    def output(self):
        return S3Target(self.s3_aggr_path)


@requires(PublishGreppedLines)
class DownloadGreppedLines(SparkSubmitTask):
    local_aggr_path = Parameter(default="output/")

    app = os.path.expanduser('~/playground/spark/maven-exaple/target/spark-maven-example-0.1-SNAPSHOT-jar-with-dependencies.jar')
    entry_class = 'pl.arapso.examples.spark.CopyS3ToLocal'

    def app_options(self):
        return [self.s3_aggr_path, self.local_aggr_path]

    def output(self):
        return LocalTarget(self.local_aggr_path)


@requires(DownloadGreppedLines)
class CalculateGreppedLinesNo(Task):

    def output(self):
        return RunAnywayTarget(self)


if __name__ == '__main__':
    luigi.run()