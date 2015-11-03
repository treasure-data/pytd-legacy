import contextlib
import gzip
import io
import time
import uuid
import msgpack
import tdclient

import logging
logger = logging.getLogger(__name__)


def frame_chunks(frame, chunksize):
    def _chunk_records(chunk):
        for _, row in chunk.iterrows():
            row.dropna(inplace=True)
            yield dict(row)
    # split frame into chunks
    for i in range((len(frame) - 1) // chunksize + 1):
        chunk = frame[i * chunksize : i * chunksize + chunksize]
        yield _chunk_records(chunk)


class StreamingUpload(object):
    def __init__(self, context, database, table):
        self.context = context
        self.database = database
        self.table = table

    def _gzip(self, data):
        buff = io.BytesIO()
        with gzip.GzipFile(fileobj=buff, mode='wb') as f:
            f.write(data)
        return buff.getvalue()

    def _upload(self, data):
        client = self.context.client
        data_size = len(data)
        unique_id = uuid.uuid4()
        start_time = time.time()
        elapsed = client.import_data(self.database, self.table, 'msgpack.gz', data, data_size, unique_id)
        end_time = time.time()
        logger.info('uploaded %d bytes in %.2f secs (elapsed %.3f)', data_size, end_time - start_time, elapsed)

    def upload_frame(self, frame, chunksize=100000):
        for chunk in frame_chunks(frame, chunksize):
            packer = msgpack.Packer(autoreset=False)
            for record in chunk:
                packer.pack(record)
            self._upload(self._gzip(packer.bytes()))


class Table(object):
    MODE_READ = 'r'
    MODE_WRITE = 'w'
    MODE_APPEND = 'a'

    def __init__(self, context, name):
        self.context = context
        self.name = name
        self.database_name, self.table_name = name.split('.')
        self.mode = None

    def _ensure_exists(self):
        try:
            self.context.client.table(self.database_name, self.table_name)
        except tdclient.errors.NotFoundError:
            if self.mode in (Table.MODE_WRITE, Table.MODE_APPEND):
                self.context.client.create_log_table(self.database_name, self.table_name)
            else:
                raise

    @contextlib.contextmanager
    def open(self, mode=MODE_READ):
        if self.mode is not None:
            raise RuntimeError('already open')
        self.mode = mode
        try:
            self._ensure_exists()
            yield self
        finally:
            self.mode = None

    def insert(self, data):
        if type(data).__name__ == 'DataFrame':
            # DataFrame
            if data['time'].dtype.name =='datetime64[ns]':
                data['time'] = data['time'].astype('int64') // (10 ** 9)
            # FIXME: use bulk import instead
            session = StreamingUpload(self.context, self.database_name, self.table_name)
            session.upload_frame(data)
        else:
            raise TypeError('unsupported data: {0}'.format(data))
