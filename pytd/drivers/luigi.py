import luigi

import logging
logger = logging.getLogger('luigi-interface')


class QueryTask(luigi.Task):
    time = None
    variables = {}

    @property
    def query(self):
        return NotImplemented()

    def output(self):
        return luigi.LocalTarget(self.time.strftime("%Y%m%d-") + str(self))

    def run(self):
        variables = self.variables
        if self.time:
            variables = {'scheduled_time': self.time}
            variables.update(self.variables)
        result = self.query.run(variables, scheduled_time=self.time)
        logger.info("%s: td.job.url: %s", self, result.job.url)
        result.wait()
        with self.output().open('w') as f:
            f.write(str(result.job_id))


class DailyQueryTask(QueryTask):
    time = luigi.DateParameter()


class HourlyQueryTask(QueryTask):
    time = luigi.DateHourParameter()
