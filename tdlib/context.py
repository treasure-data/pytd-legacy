import sys

import tdclient
import tdclient.version

from tdlib.version import __version__

import logging
logger = logging.getLogger(__name__)

class Context(object):
    '''High-level wrapper for tdclient.Client.'''

    def __init__(self, apikey=None, endpoint=None, **kwargs):
        if apikey is not None:
            kwargs['apikey'] = apikey
        if endpoint is not None:
            if not endpoint.endswith('/'):
                endpoint = endpoint + '/'
            kwargs['endpoint'] = endpoint
        if 'user_agent' not in kwargs:
            versions = [
                "tdclient/{0}".format(tdclient.version.__version__),
                "Python/{0}.{1}.{2}.{3}.{4}".format(*list(sys.version_info)),
            ]
            kwargs['user_agent'] = "tdlib/{0} ({1})".format(__version__, ' '.join(versions))
        self.client = tdclient.Client(**kwargs)

    @property
    def apikey(self):
        return self.client.api.apikey

    @property
    def endpoint(self):
        return self.client.api.endpoint
