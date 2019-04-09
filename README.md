#Storm

Create topic<br>
kaftopics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1092 --topic storm-tutorial

Push data to topic<br>
kafka-console-producer --broker-list localhost:9092 --topic storm-tutorial
{"num":5,"status":"SUCCESS"}

Bolt will randomly fail the tuple if time%2 == 0
