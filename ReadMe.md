This repository contains various code snippets that I have tried out using Spark.

-------------------------------------------------------------------------------------------------------------------
SparkJsonReadnFormat
-------------------------------------------------------------------------------------------------------------------
1) Read Json File
2) Data Frame Aggregate Functions
3) Mapping RDD to case class
4) Reformatting the Json structure
5) Write output as JSON files

Input File Structure
```
root
 |-- a_id: long (nullable = true)
 |-- b_sum: double (nullable = true)
 |-- m_cd: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- td_cnt: array (nullable = true)
 |    |-- element: double (containsNull = true)
 
 Output File Structure
 
 |-- a_id: long (nullable = false)
 |-- b_sum: double (nullable = false)
 |-- td_cnt: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: string
 |    |    |-- value: double (valueContainsNull = false)
 |-- tdcnt_sum: double (nullable = false)
```
 Databricks URL for the notebook: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/885351266626983/846641709339703/2071506668505937/latest.html
 
-------------------------------------------------------------------------------------------------------------------
WordCountAgain
-------------------------------------------------------------------------------------------------------------------
WordCount with Filter and Sorting 

-------------------------------------------------------------------------------------------------------------------
Map_and_MapValues
-------------------------------------------------------------------------------------------------------------------
Usage of map , group by, mapValues function
occurrence of elements in list 
