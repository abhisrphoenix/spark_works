This repository contains various code snippets that I have tried out using Spark.
-- Apache Spark 2.2.1, Scala 2.11 --

1) SparkJsonReadnFormat
2) WordCountAgain
3) Map_and_MapValues
4) MoreWindowFunctions

-------------------------------------------------------------------------------------------------------------------
1) SparkJsonReadnFormat
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
2) WordCountAgain
-------------------------------------------------------------------------------------------------------------------
WordCount with Filter and Sorting 

-------------------------------------------------------------------------------------------------------------------
3) Map_and_MapValues
-------------------------------------------------------------------------------------------------------------------
Usage of map , group by, mapValues function
occurrence of elements in list 
```
Input File
inf1    inf1, inf2, inf3,inf1, inf2, inf3
inf2    inf1, inf2, inf3,inf1, inf2, inf3
inf3    inf3, inf1, inf4
inf4    inf1, inf2, inf3,inf1, inf2, inf3,inf3, inf1, inf4
inf5    inf3, inf1, inf4


Output Data

(inf1,ArrayBuffer(( inf3,2), (inf1,2), ( inf2,2)))
(inf2,ArrayBuffer(( inf3,2), (inf1,2), ( inf2,2)))
(inf3,ArrayBuffer(( inf1,1), (inf3,1), ( inf4,1)))
(inf4,ArrayBuffer(( inf3,2), ( inf2,2), ( inf1,1), (inf3,1), (inf1,2), ( inf4,1)))
(inf5,ArrayBuffer(( inf1,1), (inf3,1), ( inf4,1)))
```
-------------------------------------------------------------------------------------------------------------------
4) More Window Functions
-------------------------------------------------------------------------------------------------------------------

This sample handles more functions like
Window.partitionBy, Window.orderBy, sum over partition, rank, count, filter, where , ordering ..
