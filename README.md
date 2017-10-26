# spark-udwf-session
a spark custom window function example, to generate session IDs

Usage
--------------------

Implements a custom window function to create session IDs on user activity.

Sessionization is a common calculation when processing user activity. We want to mark all events belonging to a session
if between them there's no time gap greater than T.

Will continue existing session if there is one in the data, or create one if the next event is 
outside the session duration interval. 


```
// Window specification
val specs = Window.partitionBy(f.col("user")).orderBy(f.col("ts").asc)
// create the session
val res = df.withColumn( "newsession", 
   calculateSession(f.col("ts"), f.col("session"), 
        f.lit(30*60**1000) over specs)  // window duration in ms

```

result similar to this (UUIDs are randomly generated).

```
+-----+-------------+------------------------------------+------------------------------------+
|user |ts           |session                             |newsession                          |
+-----+-------------+------------------------------------+------------------------------------+
|user1|1509036478537|f237e656-1e53-4a24-9ad5-2b4576a4125d|f237e656-1e53-4a24-9ad5-2b4576a4125d|
|user1|1509037078537|null                                |f237e656-1e53-4a24-9ad5-2b4576a4125d|
|user1|1509037378537|null                                |f237e656-1e53-4a24-9ad5-2b4576a4125d|
|user1|1509044878537|null                                |9b17a92c-9a0b-430a-bf97-41034e5b6c6c|
|user1|1509046078537|null                                |9b17a92c-9a0b-430a-bf97-41034e5b6c6c|
|user2|1509036778537|null                                |5f4f0005-52f2-41f9-ab7b-ffc69ab1353f|
|user2|1509037378537|null                                |5f4f0005-52f2-41f9-ab7b-ffc69ab1353f|
+-----+-------------+------------------------------------+------------------------------------+

```


See http://blog.nuvola-tech.com/2017/10/spark-custom-window-function-for-sessionization/ for 
a detailed explanation.
