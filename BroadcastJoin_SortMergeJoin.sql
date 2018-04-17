```      
	  ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.1
      /_/

```


val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

hiveContext.sql("create table qtest.state_code(id int, code string) partitioned by (year int) clustered by (id) into 4 buckets")
hiveContext.sql("create table qtest.state(id int, name string) partitioned by (year int) clustered by (id) into 4 buckets")

val dt = hiveContext.sql("select id, name, year from qtest.test_tab")
hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
dt.write.mode("overwrite").partitionBy("year").insertInto("qtest.state")

val dt2 = hiveContext.sql("select id, code, year from qtest.test_tab")
dt.write.mode("overwrite").partitionBy("year").insertInto("qtest.state_code")
//Since the tables are bucketted , spark will do a sort emrge join 
val jResult = hiveContext.sql("select *  from qtest.state join qtest.state_code on state.id=state_code.id")
jResult.show(false)
//took 0.381030 s
val state = hiveContext.sql("select * from qtest.state")
val state_cd = hiveContext.sql("select * from qtest.state_code") 
//Doing Broadcast join
state.join(broadcast(state_cd),state("id")===state_cd("id"),"inner").show()
//took 0.211644

/** I used the below sample data to load the table via hive 
insert into table qtest.{table_name} partition(year=201804) values   (1,"Alabama" , "AL") , (2,"Alaska" , "AK") , (3,"Arizona" , "AZ") , (4,"Arkansas" , "AR") , (5,"California" , "CA") , (6,"Colorado" , "CO") , (7,"Connecticut" , "CT") , (8,"Delaware" , "DE") , (9,"Florida" , "FL") , (10 , "Georgia" , "GA") , (11 , "Hawaii" , "HI") , (12 , "Idaho" , "ID") , (13 , "Illinois" , "IL") , (14 , "Indiana" , "IN") , (15 , "Iowa" , "IA") , (16 , "Kansas" , "KS") , (17 , "Kentucky" , "KY") , (18 , "Louisiana" , "LA") , (19 , "Maine" , "ME") , (20 , "Maryland" , "MD") , (21 , "Massachusetts" , "MA") , (22 , "Michigan" , "MI") , (23 , "Minnesota" , "MN") , (24 , "Mississippi" , "MS") , (25 , "Missouri" , "MO") , (26 , "Montana" , "MT") , (27 , "Nebraska" , "NE") , (28 , "Nevada" , "NV") , (29 , "New Hampshire" , "NH") , (30 , "New Jersey" , "NJ") , (31 , "New Mexico" , "NM") , (32 , "New York" , "NY") , (33 , "North Carolina" , "NC") , (34 , "North Dakota" , "ND") , (35 , "Ohio" , "OH") , (36 , "Oklahoma" , "OK") , (37 , "Oregon" , "OR") , (38 , "Pennsylvania" , "PA") , (39 , "Rhode Island" , "RI") , (40 , "South Carolina" , "SC") , (41 , "South Dakota" , "SD") , (42 , "Tennessee" , "TN") , (43 , "Texas" , "TX") , (44 , "Utah" , "UT") , (45 , "Vermont" , "VT") , (46 , "Virginia" , "VA") , (47 , "Washington" , "WA") , (48 , "West Virginia" , "WV") , (49 , "Wisconsin" , "WI") , (50 , "Wyoming" , "WY") , (52 , "American Samoa" , "AS") , (53 , "District of Columbia" , "DC") , (54 , "Federated States of Micronesia" , "FM") , (55 , "Guam" , "GU") , (56 , "Marshall Islands" , "MH") , (57 , "Northern Mariana Islands" , "MP") , (58 , "Palau" , "PW") , (59 , "Puerto Rico" , "PR") , (60 , "Virgin Islands" , "VI");
**/

