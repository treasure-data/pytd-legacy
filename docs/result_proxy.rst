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

Convert a Result to Pandas DataFrame
====================================

::

  import pandas as pd

  ctx = Context()

  # Run a query
  result = NamedQuery(ctx, 'access_count').run()

  # Convert to DataFrame
  columns = [c[0] for c in result.description]
  df = pd.DataFrame(iter(result), columns=columns)

Convert a Result to Spark DataFrame
===================================

::

  from pyspark.sql.types import LongType
  from pyspark.sql.types import StringType
  from pyspark.sql.types import StructType
  from pyspark.sql.types import StructField

  ctx = Context()

  # Run a query
  result = NamedQuery(ctx, 'access_count').run()

  # Download the result
  rdd = sc.parallelize([result.job_id])
  rdd = rdd.flatMap(lambda job_id: iter(ResultProxy(ctx, job_id)))

  # IMPORTANT! Cache the result to avoid multiple downloads
  rdd.persist(pyspark.StorageLevel.DISK_ONLY)

  # Build schema
  def get_field(column):
      TYPE_MAP = {
          'bigint': LongType(),
          'varchar': StringType(),
      }
      return StructField(column[0], TYPE_MAP[column[1]], True)
  schema = StructType([get_field(c) for c in result.description])

  # Convert to DataFrame
  df = rdd.toDF(schema=schema)
