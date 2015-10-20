
import contextlib
import requests
import msgpack
import zlib

from tdlib.version import __version__

def http_get(uri, **kwargs):
    return requests.get(uri, **kwargs)

class ResultProxy(object):
    def __init__(self, context, job_id, download_callback=None):
        self.context = context
        self.job_id = job_id
        self.download_callback = download_callback

    @property
    def job(self):
        if not hasattr(self, '_job'):
            self._job = self.context.client.job(self.job_id)
        return self._job

    def status(self):
        return self.job.status()

    def wait(self, *args, **kwargs):
        return self.job.wait(*args, **kwargs)

    @property
    def result_size(self):
        if not self.job.finished():
            self.job.wait()
        return self.job.result_size

    @property
    def description(self):
        if not self.job.finished():
            self.job.wait()
        return self.job.result_schema

    def get_result(self):
        headers = {
            'Authorization': 'TD1 {0}'.format(self.context.apikey),
            'Accept-Encoding': 'deflate, gzip',
            'User-Agent': "tdlib/{0} ({1})".format(__version__, requests.utils.default_user_agent()),
        }
        r = http_get('{endpoint}v3/job/result/{job_id}?format={format}'.format(
            endpoint = self.context.endpoint,
            job_id = self.job_id,
            format = 'msgpack.gz',
        ), headers=headers, stream=True)
        return r

    def iter_content(self, chunk_size):
        current_size = 0
        d = zlib.decompressobj(16+zlib.MAX_WBITS)
        with contextlib.closing(self.get_result()) as r:
            for chunk in r.iter_content(chunk_size):
                current_size += len(chunk)
                if self.download_callback:
                    self.download_callback(self, current_size)
                yield d.decompress(chunk)

    def read(self, size=16384):
        if not hasattr(self, '_iter'):
            self._iter = self.iter_content(size)
        try:
            return next(self._iter)
        except StopIteration:
            return ''

    def __iter__(self):
        # NOTE: Defined as a generator because Pandas DataFrame
        # does not support iterators as data.
        # NOTE: msgpack.Unpacker uses self.read as input here.
        # It does not support iterators unfortunately.
        for row in msgpack.Unpacker(self, encoding='utf-8'):
            yield row
