====================
DataFrame with Spark
====================

Creating DataFrame from Query Result
====================================

::

  import pytd
  from pyspark.sql.types import LongType
  from pyspark.sql.types import StringType
  from pyspark.sql.types import StructType
  from pyspark.sql.types import StructField

  tdc = pytd.Context(__name__)

  # Run query
  query = "select * from www_access"
  result = pytd.Query(tdc, database='sample_datasets', query=query).run()

  # Download the result
  rdd = sc.parallelize([result.job_id])
  rdd = rdd.flatMap(lambda job_id: iter(pytd.ResultProxy(tdc, job_id)))

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
