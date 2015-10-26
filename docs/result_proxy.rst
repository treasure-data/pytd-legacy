============
Result Proxy
============

"Result Proxy" is an abstraction of Treasure Data's query result.

Getting a Result
================

::

  from tdlib import Context, NamedQuery

  ctx = Context()

  # Run a query
  result = NamedQuery(ctx, 'access_count').run()

  # Get job_id
  job_id = result.job_id

  # Get the current status
  status = result.status()

  # Wait to finish
  result.wait()

  # Get schema
  schema = result.description

  # Iterate over result rows
  for row in result:
      print(row)
