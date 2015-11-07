import os
import luigi

import logging
logger = logging.getLogger('luigi-interface')


class QueryTask(luigi.Task):
    def get_output_path(self, context, date=None):
        args = ['tmp']
        if date:
            args.append(date.strftime("%Y-%m-%d"))
        if context.module:
            args.append(context.module)
        args.append(str(self))
        return os.path.join(*args)

    def query(self):
        raise NotImplemented()

    def graceful_wait(self, result, stopping=False):
        try:
            result.wait()
        except KeyboardInterrupt:
            if stopping:
                logger.warning("%s: killing running job %s", self, result.job_id)
                result.job.kill()
                raise
            else:
                logger.warning("%s: waiting job %s to finish", self, result.job_id)
                logger.warning("%s: press Control-C to kill the running job", self)
                self.graceful_wait(result, stopping=True)

    def run(self):
        result = self.query().run(wait=False)
        logger.info("%s: td.job.url: %s", self, result.job.url)
        self.graceful_wait(result)
        status = result.status()
        if status != 'success':
            debug = result.job.debug
            if debug and debug['stderr']:
                logger.error(debug['stderr'])
            raise RuntimeError("job {0} {1}".format(result.job_id, status))
        with self.output().open('w') as f:
            f.write(str(result.job_id))
