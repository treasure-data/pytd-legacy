import os
import luigi

import logging
logger = logging.getLogger('luigi-interface')


class QueryTask(luigi.Task):
    def get_output_path(self, context, date=None):
        if date:
            return os.path.join('tmp', date.strftime("%Y-%m-%d"), context.module, str(self))
        else:
            return os.path.join('tmp', context.module, str(self))

    def query(self):
        raise NotImplemented()

    def run(self):
        result = self.query().run(wait=False)
        logger.info("%s: td.job.url: %s", self, result.job.url)
        try:
            result.wait()
        except KeyboardInterrupt:
            logger.info("%s: killing running job %s", self, result.job_id)
            result.job.kill()
            logger.error("%s: job %s killed", self, result.job_id)
            raise
        status = result.status()
        if status != 'success':
            debug = result.job.debug
            if debug and debug['stderr']:
                logger.error(debug['stderr'])
            raise RuntimeError("job {0} {1}".format(result.job_id, status))
        with self.output().open('w') as f:
            f.write(str(result.job_id))
