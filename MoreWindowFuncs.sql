val dt = Seq((1,"NJ",300),(2,"IL",340),(3,"FL",290),(3,"FL",310),(4,"IN",300),(5,"WI",300),(6,"IW",285),(6,"IW",305),(7,"GA",305),(8,"CA",360),(9,"NC",355),(10,"SC",304) )
val qDF = dt.toDF("id", "state", "score")

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Rank over partition using Window.partitionBy
val wSpec1 = Window.partitionBy("id").orderBy("score")
qDF.select($"*",rank().over(wSpec1) as "cnt").show()

// Excluding columns  from result drop function and where condition 
qDF.withColumn("rnk",rank().over(wSpec1)).where($"rnk" === 1).drop('rnk).show()

// Using filter instead of where
qDF.withColumn("rnk",rank().over(wSpec1)).filter($"rnk" === 1).show()


//using groupBy and sum , count functions
qDF.groupBy('id,'state).agg(sum('score) as "totalScore", count('score) as "scoreCount").orderBy($"totalScore".desc).show()
//qDF.groupBy('id,'state).agg(sum('score) as "totalScore").orderBy($"totalScore".desc).show()

qDF.withColumn("rnk",rank().over(wSpec1)).filter($"rnk" === 1).show()
qDF.withColumn("rnk",rank().over(wSpec1)).where($"rnk" === 1).select('id,'state,'score).show()


val orderedByID = Window.orderBy('id)
val totalScore = sum('score).over(orderedByID).as('totalScore)
qDF.select('* , totalScore).show()


qDF.groupBy('id,'state).agg(sum('score) as "totalScore").orderBy($"totalScore".desc).show()

//I state data only by partitioning records into two groups stating with I and others
val iStateSpec = Window.partitionBy('state startsWith "I")
qDF.select('*, sum('score) over iStateSpec as "iState_Specs").show()


//usage of first to select from group
qDF.groupBy('id,'state).agg(count("id") as "cnt", first("score") as  "score").filter($"cnt" ===1).orderBy($"state").show()


//Window Functions MAX over - selecting score difference in state
val scoreDesc = Window.partitionBy('id).orderBy('state.desc)
val scoreDiff = max('score).over(scoreDesc) - 'score
qDF.select('*, scoreDiff as 'score_diff).show

//Cumulative Sum over score
val wSpec1 = Window.partitionBy("id").orderBy("score")
qDF.withColumn("cumSum",sum("score").over(wSpec1)).show

//Running Total

val orderedByID = Window.orderBy('id)
val totalQty = sum('score).over(orderedByID).as('running_sum)
val res = qDF.select('*, totalQty).orderBy('id)
.show()

