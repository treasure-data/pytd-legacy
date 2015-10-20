===========
Named Query
===========

"Named Query" is an abstraction of Treasure Data's saved queries (formerly known as "scheduled jobs").  You can create, run, and schedule queries using ``NamedQuery`` class.

Creating a Named Query
======================

You can create a ``NamedQuery`` object by passing a context and query name.  Nothing happens just by creating a ``NamedQuery`` object.  You need to explicitly call ``save()`` method to save the query in the cloud::

  from tdlib import Context, NamedQuery

  ctx = Context()

  # Create a query
  query = "select count(1) from www_access"
  q = NamedQuery(ctx, 'access_count', database='my_db', query=query)
  q.save()

``NamedQuery`` accepts the same parameters as ``td sched:create`` command.  See `Scheduled Jobs (CLI) <http://docs.treasure-data.com/articles/schedule-cli>`_ for details.

You can find your query by running ``td sched:list`` command::

  $ td sched:list
  +--------------+------+----------+---------------+-------+----------+--------+----------+---------------------------------+
  | Name         | Cron | Timezone | Next schedule | Delay | Priority | Result | Database | Query                           |
  +--------------+------+----------+---------------+-------+----------+--------+----------+---------------------------------+
  | access_count |      | UTC      |               | 0     | NORMAL   |        | my_db    | select count(1) from www_access |
  +--------------+------+----------+---------------+-------+----------+--------+----------+---------------------------------+
  1 row in set

``save()`` is a repeatable operation.  You can edit your query and save it again to update it as long as the query uses the same name::

  # Update a query
  query = "select count(1) from www_access where time > 1234567890"
  q = NamedQuery(ctx, 'access_count', database='my_db', query=query)
  q.save()

Running a Query
===============

You can run a query at any time you want, optionally with a specific time value::

  import datetime

  # Define a query
  query = "select count(1) from www_access"
  q = NamedQuery(ctx, 'access_count', database='my_db', query=query)

  # Run the query with a given time
  time = datetime.datetime(2015, 1, 1, 0, 0, 0)
  q.run(time)

The time value is used by ``td_scheduled_time()`` UDF.  See `examples <http://docs.treasuredata.com/articles/schedule#example-daily-kpis>`_ for how to use ``td_scheduled_time()``.

Note: ``run()`` implicitly calls ``save()`` before running a query.  You don't need to call ``save()``.

Templating a Query
==================

A query can be a `Jinja2 <http://jinja.pocoo.org/docs/dev/>`_ template, which allows you to substitute variables in the query::

  # Define query template with Jinja2
  query = '''
  select count(1) cnt
  from www_access
  where td_time_range(time, '{{start_time}}', '{{end_time}}')
  '''
  q = NamedQuery(ctx, 'access_count', database='my_db', query=query)

  # Run a query with variable substitution
  time = datetime.datetime(2015, 1, 1, 0, 0, 0)
  variables = {
    'start_time': '2000-01-01',
    'end_time': '2010-01-01',
  }
  q.run(time, variables)

  # The query is rendered as follows:
  #
  # select count(1) cnt
  # from www_access
  # where td_time_range(time, '2000-01-01', '2010-01-01')

If you want to preview the query before running it, use ``render()``::

  >>> print(q.render(variables))
  select count(1) cnt
  from www_access
  where td_time_range(time, '2000-01-01', '2010-01-01')

Defining Query as Class
=======================

You can define a query as a class by subclassing ``NamedQuery``::

  class AccessCount(tdlib.NamedQuery):
      name = 'access_count'
      database = 'my_db'
      type = 'presto'
      query = "select count(1) cnt from www_access"

  # Create a query object
  q = AccessCount(ctx)
  q.save()

If you are familiar with Python, you can generate a query dynamically by defining ``query`` and other fields as properties of a class::

  class DynamicAccessCount(tdlib.NamedQuery):
      database = 'my_db'
      type = 'presto'

      # Initialize with a custom argument
      def __init__(self, ctx, table):
          # Don't forget to call super().__init__()
          super().__init__(ctx)
          self.table = table

      @property
      def name(self):
          return "access_count.{0}".format(self.table)

      @property
      def query(self):
          return "select count(1) cnt from {0}".format(self.table)

  # This will create a query named "access_count.www_access"
  q = DynamicAccessCount(ctx, 'www_access')
  q.save()

Deleting a Query
================

Use ``delete()`` to delete a named query::

  q = NamedQuery(ctx, 'access_count')
  q.delete()
