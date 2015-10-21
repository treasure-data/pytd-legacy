
import datetime
import tdclient
import jinja2

import logging
logger = logging.getLogger(__name__)

from .result import ResultProxy

class NotFoundError(Exception): pass


class NamedQuery(object):
    source = None

    def __init__(self, context, name=None, cron=None, database=None, query=None, type=None):
        self.context = context
        self._name = name
        self._cron = cron
        self._database = database
        self._query = query
        self._type = type

    @property
    def name(self):
        if self._name is None:
            if self.__class__ == NamedQuery:
                raise TypeError("missing parameter: 'name'")
            return "{0}.{1}".format(self.__class__.__module__, self.__class__.__name__)
        return self._name

    @property
    def cron(self):
        if self._cron is None:
            return ''
        return self._cron

    @property
    def database(self):
        if self._database is None:
            raise TypeError("missing parameter: 'database'")
        return self._database

    @property
    def query(self):
        if self._query is None:
            raise TypeError("missing parameter: 'query'")
        return self._query

    @property
    def type(self):
        if self._type is None:
            return 'hive'
        return self._type

    def get_template(self):
        if self.source:
            env = jinja2.Environment(loader=jinja2.PackageLoader(self.__module__, '.'))
            return env.get_template(self.source)
        else:
            return jinja2.Template(self.query)

    def render(self, variables=None):
        if variables is None:
            variables = {}
        return self.get_template().render(variables)

    def save(self, variables=None):
        api = self.context.client.api
        params = {
            'database': self.database,
            'query': self.render(variables),
            'type': self.type,
            'cron': self.cron,
        }
        try:
            api.update_schedule(self.name, params)
        except tdclient.api.NotFoundError:
            api.create_schedule(self.name, params)

    def rename(self, newname):
        api = self.context.client.api
        params = {
            'newname': newname,
        }
        api.update_schedule(self.name, params)

    def delete(self):
        api = self.context.client.api
        api.delete_schedule(self.name)

    def run(self, time=None, variables=None):
        if time is None:
            time = datetime.datetime.now().replace(microsecond=0)
        # save query before running
        self.save(variables)
        # run query
        api = self.context.client.api
        for job_id, type, scheduled_at in api.run_schedule(self.name, time):
            return ResultProxy(self.context, job_id)
