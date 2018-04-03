import sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.collection.mutable.WrappedArray
import scala.collection.mutable
import scala.collection.Map

val df = sqlContext.read.json("/FileStore/tables/inp.json")
df.printSchema()
val df_ex = df.select($"*", explode($"m_cd") as "mcd").select($"*", explode($"td_cnt") as "tdcnt").select($"a_id",$"b_sum",$"mcd",$"tdcnt").na.drop()
val df_agg = df_ex.groupBy($"a_id",$"b_sum",$"mcd").agg(sum("tdcnt") as "tdcnt_sum").groupBy("a_id","b_sum").agg(collect_list(struct("mcd", "tdcnt_sum") )as "td_cnt", sum("tdcnt_sum") as "tdcnt_sum")
//Forcing data via a single reducer
sqlContext.setConf("spark.sql.shuffle.partitions", "1")
val rows: RDD[Row] = df_agg.rdd
rows.collect.foreach(println)
case class FRow(a_id: Long, b_sum: Double, td_cnt: List[Map[String, Double]],tdcnt_sum: Double)
val q = rows.map(row => {
  val a = row.getAs[Long](0)
  var b = row.getAs[Double](1)
  
  var x = row.getAs[Seq[Row]](2)
  var mp = x.map( wary => {
    Map(wary(0).toString -> wary(1).asInstanceOf[Double])
  })
  var ls = mp.toList
  var d =  row.getAs[Double](3)
  FRow(a,b,ls,d)
})
val dd = q.toDF()
dd.printSchema()
dd.write.mode(SaveMode.Overwrite).format("json").save("/FileStore/tables/output.json")