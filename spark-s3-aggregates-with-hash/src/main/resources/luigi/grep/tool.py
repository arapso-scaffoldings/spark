import os.path

import luigi
from luigi import Parameter, Task
from luigi.contrib.s3 import S3Target
from luigi.contrib.simulate import RunAnywayTarget
from luigi.contrib.spark import SparkSubmitTask
from luigi.local_target import LocalTarget
from luigi.util import requires
import hash_evaluator


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
    s3_aggr_hash_path = Parameter(default="s3://roqad-test-de/damian/aggr_hashes/2017-08-01/hash.txt")

    app = os.path.expanduser(
        '~/playground/spark/maven-exaple/target/spark-maven-example-0.1-SNAPSHOT-jar-with-dependencies.jar')
    entry_class = 'pl.arapso.examples.spark.CopyLocalToS3'

    def app_options(self):
        return [self.output_path, self.s3_aggr_path]

    def complete(self):
        that = self
        src_files_path = self.s3_logs_path
        src_files_filter = "tracker_PL_"
        hash_path = self.s3_aggr_hash_path
        aggregates_path = self.s3_aggr_path

        return hash_evaluator.\
            check_if_hashes_equals(old_aggr_exists=Task.complete(self),
                                                  src_files_path=src_files_path,
                                                  src_files_filter=src_files_filter,
                                                  aggregates_path=aggregates_path,
                                                  hash_path=hash_path)

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