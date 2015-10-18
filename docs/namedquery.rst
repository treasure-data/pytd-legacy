===========
Named Query
===========

"Named Query" is an abstraction of Treasure Data's saved queries (formerly known as "Scheduled Jobs").  You can create, run, and schedule queries using ``NamedQuery`` class.

Creating a Named Query
======================

Create a ``NamedQuery`` object and ``save()`` it.  ``NamedQuery`` accepts the same parameters as ``td sched:create`` command.  (See `Scheduled Jobs (CLI) <http://docs.treasure-data.com/articles/schedule-cli>`_ for details.)

::

  from tdlib import Context, NamedQuery

  ctx = Context()

  query = "select count(1) from www_access"
  q = NamedQuery(ctx, 'access_count', database='my_db', query=query)
  q.save()

You will find your query by running ``td sched:list`` command::

  $ td sched:list
  +--------------+------+----------+---------------+-------+----------+--------+----------+---------------------------------+
  | Name         | Cron | Timezone | Next schedule | Delay | Priority | Result | Database | Query                           |
  +--------------+------+----------+---------------+-------+----------+--------+----------+---------------------------------+
  | access_count |      | UTC      |               | 0     | NORMAL   |        | my_db    | select count(1) from www_access |
  +--------------+------+----------+---------------+-------+----------+--------+----------+---------------------------------+
  1 row in set

You can update your query by recreating an object with the same name::

  # Update a query
  query = "select count(1) from www_access where time > 1234567890"
  q = NamedQuery(ctx, 'access_count', database='my_db', query=query)
  q.save()

Running a Query
===============

You can run a query at any time you want, optionally with a specific time value.  This value is used by ``td_scheduled_time()`` UDF and is useful when you want to retry a failed query with past date::

  import datetime

  # Define a query
  query = "select count(1) from www_access"
  q = NamedQuery(ctx, 'access_count', database='my_db', query=query)

  # Run the query with a given time
  time = datetime.datetime(2015, 1, 1, 0, 0, 0)
  q.run(time)

Note: You don't need to call ``save()`` explicitly.  A query is always saved before you run it.

Templating a Query
==================

A query can be a `Jinja2 <http://jinja.pocoo.org/docs/dev/>`_ template, which allows you to substitute variables in the query::

  # Define query template with Jinja2
  query = '''
  select count(*) cnt
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
  # select count(*) cnt
  # from www_access
  # where td_time_range(time, '2000-01-01', '2010-01-01')

If you want to preview the query without running, use ``render()`` to get it rendered::

  >>> print(q.render(variables))
  select count(*) cnt
  from www_access
  where td_time_range(time, '2000-01-01', '2010-01-01')

Defining Query as Class
=======================

::

  class AccessCount(tdlib.NamedQuery):
      name = 'access_count'
      database = 'my_db'
      type = 'presto'

      query = '''
      select count(*) cnt
      from www_access
      where td_time_range(time, '{{start_time}}', '{{end_time}}')
      '''

  # Create a query object
  q = AccessCount(ctx)

  # Run it as usual
  time = datetime.datetime(2015, 1, 1, 0, 0, 0)
  variables = {
    'start_time': '2000-01-01',
    'end_time': '2010-01-01',
  }
  q.run(time, variables)


Deleting a Query
================

Use ``delete()`` to delete a named query::

  q = NamedQuery(ctx, 'access_count')
  q.delete()
