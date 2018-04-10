//Using of mapValues on list ; count of elements in list
val sRDD = sc.textFile("/user/uszanr8/tmp/freqdata.txt")
case class MClass(typ:String, value:List[String])
val maped = sRDD.map(x => MClass(x.split("\t")(0),x.split("\t")(1).split(",").toList))
maped.map(x => ( x.typ, x.value.groupBy(identity).mapValues(_.size).map(identity))).foreach(println)
//maped.map(x => ( x.typ, x.value.groupBy(identity).mapValues(_.size).toSeq)).foreach(println)

+------------Input File-----------------------------------------------------+
inf1    inf1, inf2, inf3,inf1, inf2, inf3
inf2    inf1, inf2, inf3,inf1, inf2, inf3
inf3    inf3, inf1, inf4
inf4    inf1, inf2, inf3,inf1, inf2, inf3,inf3, inf1, inf4
inf5    inf3, inf1, inf4

+-------------------Output---------------------------------------------------+

(inf1,ArrayBuffer(( inf3,2), (inf1,2), ( inf2,2)))
(inf2,ArrayBuffer(( inf3,2), (inf1,2), ( inf2,2)))
(inf3,ArrayBuffer(( inf1,1), (inf3,1), ( inf4,1)))
(inf4,ArrayBuffer(( inf3,2), ( inf2,2), ( inf1,1), (inf3,1), (inf1,2), ( inf4,1)))
(inf5,ArrayBuffer(( inf1,1), (inf3,1), ( inf4,1)))
