val textRDD = sc.textFile("/user/data/tmp/data.txt")
//Split the sentence based on space and flatmap 
val splitRDD = textRDD.flatMap(x => (x.split(" ")))
val articles  = List("a", "an", "the")
//Filter the articles and apply map function for every word give (word,1)
val mapedRDD = splitRDD.filter(x => (!articles.contains(x.toLowerCase) && x.length() > 1) ).map(x => (x,1))
val result = mapedRDD.reduceByKey((a,b) => a+b)
//Fetch the top 5 words 
result.sortBy(-_._2).take(5) //alternative way result.sortBy(x =>(-x._2))
Array((the,31), (a,14), (of,9), (which,9), (be,8))

Array((which,9), (policy,7), (for,6), (number,6), (this,5))


val srcdata = sc.textFile("/user/uszanr8/tmp/pvdata.txt")
val srcRDD = sc.textFile("/user/uszanr8/tmp/lease.txt")
case class MClass(typ:String, value:String)
val maped = srcRDD.map(x => MClass(x.split(",")(0),x.split(",")(1) ))

maped.take(7)

Array[MClass] = Array(MClass(SUV,RAV4), MClass(lease,36), MClass(endTime,2018), MClass(SUV,Explorer), MClass(lease,60), MClass(endTime,2019), MClass(SUV,Forrester))


val maped = srcdata.map(x => (x.split(",")))
srcdata.map(_.split(",")).take(5).groupBy(x => x(0))

case class MClass(typ:String, value:String)
srcdata.map(x => MClass(x.split(",")(0),x.split(",")(1) ))
