=====================
DataFrame with Pandas
=====================

Creating DataFrame from Query Result
====================================

::

  import pytd
  import pandas as pd

  tdc = pytd.Context(__name__)

  # Run query
  query = "select * from www_access"
  result = pytd.Query(tdc, database='sample_datasets', query=query).run()

  # Convert to DataFrame
  columns = [c[0] for c in result.description]
  df = pd.DataFrame(iter(result), columns=columns)
