import sys
import jinja2
import tdclient
import tdclient.version

from .version import __version__

import logging
logger = logging.getLogger(__name__)

class Context(object):
    '''High-level wrapper for tdclient.Client.'''

    def __init__(self, module=None, config=None):
        if config is None:
            config = {}
        self.module = module

        # tdclient
        self.client = self.get_client(apikey=config.get('apikey'), endpoint=config.get('endpoint'))

        # jinja2
        if 'template_loader' in config:
            self.template_loader = config['template_loader']
        elif self.module:
            self.template_loader = jinja2.PackageLoader(self.module, 'templates')
        else:
            self.template_loader = jinja2.FileSystemLoader('templates')

    def get_client(self, apikey=None, endpoint=None):
        kwargs = {}
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
            kwargs['user_agent'] = "pytd/{0} ({1})".format(__version__, ' '.join(versions))
        return tdclient.Client(**kwargs)

    @property
    def apikey(self):
        return self.client.api.apikey

    @property
    def endpoint(self):
        return self.client.api.endpoint

    def query(self, *args, **kwargs):
        from pytd.query import Query
        return Query(self, *args, **kwargs)
