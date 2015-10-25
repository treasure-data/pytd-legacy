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

    def run_query(self):
        variables = self.variables
        if self.time:
            variables = {'scheduled_time': self.time}
            variables.update(self.variables)
        return self.query.run(variables, scheduled_time=self.time)

    def run(self):
        result = self.run_query()
        logger.info("%s: td.job.url: %s", self, result.job.url)
        result.wait()
        status = result.status()
        if status != 'success':
            debug = result.job.debug
            if debug and debug['stderr']:
                logger.error(debug['stderr'])
            raise RuntimeError("job {0} {1}".format(result.job_id, status))
        with self.output().open('w') as f:
            f.write(str(result.job_id))


class DailyQueryTask(QueryTask):
    time = luigi.DateParameter()


class HourlyQueryTask(QueryTask):
    time = luigi.DateHourParameter()
