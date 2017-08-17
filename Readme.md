使用说明

创建名为 test 的 kinesis 队列，分片数选择 1 （为了省钱）
使用 emr 创建 spark 集群
mvn package 进行打包
将 jar 包上传到 emr 到 master 节点

执行如下命令：
```
spark-example --jars /tmp/kinesis.SparkConsumer-0.1.0-jar-with-dependencies.jar streaming.KinesisWordProducerASL test https://kinesis.us-west-2.amazonaws.com 2 10
```
向 kinesis 的 test 队列里 写一点数据

执行如下命令：
```
sudo -u hdfs spark-example --jars /tmp/kinesis.SparkConsumer-0.1.0-jar-with-dependencies.jar streaming.KinesisWordCountASL KinesisWordCountASL test https://kinesis.us-west-2.amazonaws.com
```
调用 example 执行 wordcount

使用自己的kinesis consumer
执行如下命令：
```
sudo -u hdfs spark-submit --class com.ptmind.aws.kinesis.KinesisKuduSparkConsumer /tmp/kinesis.SparkConsumer-0.1.0-jar-with-dependencies.jar KinesisWordCountASL2 test https://kinesis.us-west-2.amazonaws.com 10.0.0.104:7051 app
```

新增 KinesisProducer
```
java -cp /tmp/kinesis.SparkConsumer-0.1.0-jar-with-dependencies.jar com.ptmind.aws.kinesis.KinesisProducer
```

新增 kudutest
```
java -cp /tmp/kinesis.SparkConsumer-0.1.0-jar-with-dependencies.jar com.ptmind.aws.kinesis.kudutest
```
