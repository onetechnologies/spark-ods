# ASAM-ODS Data Source for Apache Spark

## Requirements

* Spark 2.0+ for 0.4.x.
* ODS API library(ods-5.3.0.jar)

## Build

$ sbt clean package

## Features

Connecting ODS server via NameService with options

* `ns`: CORBA NameService location.
* `name`: registered ODS server name.
* `auth`: ODS server authentication string.

ODS query
* `select`:Multiple application attribute separated by ",".
* `join`: Multiple application relation separated by ",".
* `where`: Multiple query string and operator "AND|OR|OPEN|CLOSE" separated by ",".
* `group by`: Multiple application attribute separated by ",".
* `order by`: Multiple application attribute with option "ASC|DESC" separated by ",".

Measurment values retrieval from SubMatrix
* `submatrix`: id of SubMatrix instance
* `channels`: channel names separated by ","(optional). 


## Examples

### ODS Query

```scala
scala> spark.read.format("jp.co.toyo.spark.ods")
		.option("ns", "corbaloc::1.2@localhost:2809/NameService")
		.option("name", "MDM.ASAM-ODS").option("auth", "USER=sa, PASSWORD=sa")
		.option("select", "MeaResult.Name,SubMatrix.Name,SubMatrix.Id")
		.option("join","MeaResult.SubMatrices=SubMatrix")
		.option("where", "MeaResult.Name EQ Channel, AND, MeaResult.DateCreated GTE 20160101000000")
		.option("order by", "MeaResult.MeasurementBegin DESC")
		.load()
		.show()
+--------------+------------+--------------+
|SubMatrix.Name|SubMatrix.Id|MeaResult.Name|
+--------------+------------+--------------+
|       Channel|        2011|       Channel|
+--------------+------------+--------------+


scala>
```

### Measurment values

```scala
scala> spark.read.format("jp.co.toyo.spark.ods")
		.option("ns", "corbaloc::1.2@localhost:2809/NameService")
		.option("name", "MDM.ASAM-ODS")
		.option("auth", "USER=sa, PASSWORD=sa")
		.option("submatrix", 2011)
		.option("channels", "CHANNEL01, CHANNEL02, CHANNEL03, CHANNEL04, X-Axis")
		.load()
		.show()
+----------+---------+---------+---------+------+
| CHANNEL01|CHANNEL02|CHANNEL03|CHANNEL04|X-Axis|
+----------+---------+---------+---------+------+
|4.38541E-6|  2.02778| -4.44111| -4.51025|     1|
|4.38541E-6|  2.02778| -2.03551| -4.51025|     2|
|4.38541E-6|  2.02778| -4.44111| -4.51025|     3|
|   2.40175|  2.02778| -4.44111|  2.00455|     4|
|4.38541E-6|-0.368683| -4.44111|  2.00455|     5|
|4.38541E-6|-0.368683| -2.03551| -4.51025|     6|
|   2.40175|  2.02778| -4.44111| -4.51025|     7|
|4.38541E-6|-0.368683| -2.03551| -4.51025|     8|
|4.38541E-6|  2.02778| -4.44111| -4.51025|     9|
|4.38541E-6|  2.02778| -2.03551| -4.51025|    10|
|4.38541E-6|-0.368683| -4.44111| -4.51025|    11|
|4.38541E-6|  2.02778| -2.03551| -4.51025|    12|
|4.38541E-6|  2.02778| -2.03551| -4.51025|    13|
|4.38541E-6|  2.02778| -2.03551| -4.51025|    14|
|4.38541E-6|-0.368683| -4.44111| -4.51025|    15|
|4.38541E-6|-0.368683| -2.03551| -4.51025|    16|
|4.38541E-6|  2.02778| -2.03551| -4.51025|    17|
|4.38541E-6|  2.02778| -4.44111| -4.51025|    18|
|4.38541E-6|  2.02778| -4.44111| -4.51025|    19|
|4.38541E-6|-0.368683| -4.44111| -4.51025|    20|
+----------+---------+---------+---------+------+
only showing top 20 rows


scala>
```
