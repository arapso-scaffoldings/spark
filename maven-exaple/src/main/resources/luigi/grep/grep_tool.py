import luigi
from luigi.contrib.spark import SparkSubmitTask

class MySparkTask(SparkSubmitTask):
    output = luigi.Parameter()

    app = 'spark-maven-example-0.1-SNAPSHOT.jar'
    entry_class = 'pl.arapso.examples.spark.SparkGrepScala'

    def app_options(self):
        return ['input', 'output']

    def requires(self):
        pass

    def output(self):
        return "output"


if __name__ == '__main__':
    luigi.run()