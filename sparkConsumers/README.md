# scala-spark-sql-dataframe-etl

####1-Download the latest release and un-tar it (full instruction : https://kafka.apache.org/quickstart).

$ tar -xzf kafka_*.tgz

$ cd kafka_*

####2-Start Servers:

1- bin/zookeeper-server-start.sh config/zookeeper.properties

2- bin/kafka-server-start.sh config/server.properties

####3-Create a topic:

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mytopic

--you can see available topics:

bin/kafka-topics.sh --list --zookeeper localhost:2181

####4-Run Consumer and Producer:

1-Using IDE, Right click on Consumer and click run

2-In IDE, Right click on Producer and click run then check Consumer console
for result
