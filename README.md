# spark-demo

[Apache Spark](http://spark.apache.org/) is a cluster computing framework that runs atop distributed storage software HDFS (among others), but which offers substantial performance improvement over Hadoop. To install Spark, first ensure Hadoop is installed on your system. A demo for configuring Hadoop on OS X is given on [https://github.com/jcboyd/hadoop-demo](https://github.com/jcboyd/hadoop-demo). Spark can be installed with

```sh
$ brew install spark
```

by which it will be placed in **/usr/local/Cellar/apache-spark**. Programming in Spark centers on data structures known as Resilient Distributed Datasets (RDD), which are replicated over the cluster. Spark is written primarily in Scala and Java and comes with shell interfaces for Scala, Python, and R. For example, launch the Scala shell with

```sh
$ /usr/local/Cellar/apache-spark/1.6.0/bin/spark-shell
```

Functional language Scala lends itself well to Spark. A MapReduce operation can be accomplished spectacularly with

```scala
val textFile = sc.textFile("file:///usr/local/Cellar/apache-spark/1.6.0/README.md")
val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
```

A number of data processing libraries are provided with the distribution, notably **mllib**, a machine learning library. A linear regression demo using this library is given in LinearRegression.scala. This can be compiled with Scala build tool sbt

```sh
$ brew install sbt
$ sbt package
```

The model can then be run with the command

```sh
$ /usr/local/Cellar/apache-spark/1.6.0/bin/spark-submit --class "LinearRegression" --master local[4] target/scala-2.10/spark-demo_2.10-1.0.jar
```
