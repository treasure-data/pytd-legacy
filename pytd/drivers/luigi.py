import luigi

import logging
logger = logging.getLogger('luigi-interface')

class QueryTask(luigi.Task):
    time = luigi.DateHourParameter()

    @property
    def variables(self):
        return {'scheduled_time': self.time}

    @property
    def query(self):
        return NotImplemented()

    def output(self):
        return luigi.LocalTarget(self.time.strftime("%Y%m%d-") + str(self))

    def run(self):
        result = self.query.run(self.variables, scheduled_time=self.time)
        logger.info("%s: td.job.url: %s", self, result.job.url)
        result.wait()
        with self.output().open('w') as f:
            f.write(str(result.job_id))
