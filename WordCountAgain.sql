val textRDD = sc.textFile("/user/data/tmp/data.txt")
//Split the sentence based on space and flatmap 
val splitRDD = textRDD.flatMap(x => (x.split(" ")))
val articles  = List("a", "an", "the")
//Filter the articles and apply map function for every word give (word,1)
val mapedRDD = splitRDD.filter(x => (!articles.contains(x.toLowerCase) && x.length() > 1) ).map(x => (x,1))
val result = mapedRDD.reduceByKey((a,b) => a+b)
//Fetch the top 5 words in descendig order
result.sortBy(-_._2).take(5) //alternative way result.sortBy(x =>(-x._2))

+------------Result without Filter-------------------+
Array((the,31), (a,14), (of,9), (which,9), (be,8))
+------------Result with Filter-------------------+
Array((which,9), (policy,7), (for,6), (number,6), (this,5))


