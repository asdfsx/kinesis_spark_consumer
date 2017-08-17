package com.sample.aws.kinesis;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.json.JSONObject;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

public final class KinesisKuduSparkConsumer {
    private static final Pattern WORD_SEPARATOR = Pattern.compile(" ");

    public static String getRegionNameByEndpoint(String endpoint) throws Exception{
        URI uri = new java.net.URI(endpoint);
        List<Region> regions = RegionUtils.getRegionsForService("kinesis");

        for (Region tmp : regions){
            if (uri.getHost().contains(tmp.getName())){
                return tmp.getName();
            }
        }
        throw new IllegalArgumentException("Could not resolve region for endpoint: "+endpoint);
    }

    public static void main(String[] args) throws Exception {
        // Check that all required args were passed in.
        if (args.length != 5) {
            System.err.println(
                    "Usage: KinesisKuduSparkConsumer <app-name> <stream-name> <region-name> <kudu-master> <dynamo-table>\n\n" +
                            "    <app-name> is the name of the app, used to track the read data in DynamoDB\n" +
                            "    <stream-name> is the name of the Kinesis stream\n" +
                            "    <endpoint-url> is the region name of the service\n" +
                            "                   (e.g. us-east-1)\n" +
                            "    <kudu-master> is the kudu master\n" +
                            "                   (e.g. kudu-original-master.example:7051,kudu-new-master.example:7051)\n" +
                            "    <dynamo-table> is the name of the dynamodb talbe which store the app information" +
                            "Generate data for the Kinesis stream using the example KinesisWordProducerASL.\n" +
                            "See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more\n" +
                            "details.\n"
            );
            System.exit(1);
        }

        // Populate the appropriate variables from the given args
        String kinesisAppName = args[0];
        String streamName = args[1];
        String endpointUrl = args[2];
        String kuduMaster = args[3];
        String dynamoTable = args[4];

        // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
        // DynamoDB of the same region as the Kinesis stream
        String regionName = getRegionNameByEndpoint(endpointUrl);

        // Scan appinfo from dynamodb
        DynamoAppinfo.scanAppinfo(regionName, dynamoTable);

        // Create a Kinesis client in order to determine the number of shards for the given stream
        AmazonKinesisClient kinesisClient =
                new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
        kinesisClient.setEndpoint(endpointUrl);

        int numShards =
                kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();

        // In this example, we're going to create 1 Kinesis Receiver/input DStream for each shard.
        // This is not a necessity; if there are less receivers/DStreams than the number of shards,
        // then the shards will be automatically distributed among the receivers and each receiver
        // will receive data from multiple shards.
        int numStreams = numShards;

        // Spark Streaming batch interval
        Duration batchInterval = new Duration(2000);

        // Kinesis checkpoint interval.  Same as batchInterval for this example.
        Duration kinesisCheckpointInterval = batchInterval;

        // Setup the Spark config and StreamingContext
        SparkConf sparkConfig = new SparkConf().setAppName(kinesisAppName);
        JavaSparkContext jsc = new JavaSparkContext(sparkConfig);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, batchInterval);

        // Create the Kinesis DStreams
        List<JavaDStream<byte[]>> streamsList = new ArrayList<JavaDStream<byte[]>>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            streamsList.add(
                    KinesisUtils.createStream(jssc, kinesisAppName, streamName,  endpointUrl, regionName,
                            InitialPositionInStream.LATEST, kinesisCheckpointInterval,
                            StorageLevel.MEMORY_AND_DISK_2())
            );
        }

        // Union all the streams if there is more than 1 stream
        JavaDStream<byte[]> unionStreams;
        if (streamsList.size() > 1) {
            unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
        } else {
            // Otherwise, just use the 1 stream
            unionStreams = streamsList.get(0);
        }

        // Convert each line of Array[Byte] to Json Object
        // then translate into a pair structure
        JavaPairDStream<String, ConcurrentLinkedQueue<String>> jsonPairs = unionStreams.flatMapToPair(
                new PairFlatMapFunction<byte[], String, ConcurrentLinkedQueue<String>>() {
                    @Override
                    public Iterator<Tuple2<String, ConcurrentLinkedQueue<String>>> call(byte[] line) {
                        List<Tuple2<String, ConcurrentLinkedQueue<String>>> result =
                                new ArrayList<Tuple2<String, ConcurrentLinkedQueue<String>>>();
                        try {
                            String jsonString = new String(line, StandardCharsets.UTF_8);
                            JSONObject jsonObject = new JSONObject(jsonString);

                            String category = jsonObject.getString("category");

                            ConcurrentLinkedQueue<String> cache = new ConcurrentLinkedQueue<String>();
                            cache.add(jsonString);
                            result.add(new Tuple2<String, ConcurrentLinkedQueue<String>>(category, cache));
                        } catch(Exception e){
                            e.printStackTrace();
                        }
                        return result.iterator();
                    }
                }
        );

        jsonPairs.print();

        // merge by the key
        JavaPairDStream<String, ConcurrentLinkedQueue<String>> jsonPairGroups = jsonPairs.reduceByKey(
                new Function2<ConcurrentLinkedQueue<String>, ConcurrentLinkedQueue<String>, ConcurrentLinkedQueue<String>>() {
                    @Override
                    public ConcurrentLinkedQueue<String> call(ConcurrentLinkedQueue<String> jsonObjects1, ConcurrentLinkedQueue<String> jsonObjects2) throws Exception {
                        jsonObjects1.addAll(jsonObjects2);
                        return jsonObjects1;
                    }
                }
        );

        // Map each word to a (word, 1) tuple so we can reduce by key to count the words
        jsonPairGroups.foreachRDD(
                new VoidFunction<JavaPairRDD<String,ConcurrentLinkedQueue<String>>>(){
                    @Override
                    public void call(JavaPairRDD<String, ConcurrentLinkedQueue<String>> stringConcurrentLinkedQueueJavaPairRDD) throws Exception {
                        if (!stringConcurrentLinkedQueueJavaPairRDD.isEmpty()) {
                            stringConcurrentLinkedQueueJavaPairRDD.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, ConcurrentLinkedQueue<String>>>>() {
                                    @Override
                                    public void call(Iterator<Tuple2<String, ConcurrentLinkedQueue<String>>> tuple2Iterator) throws Exception {
                                        while (tuple2Iterator.hasNext()) {
                                            Tuple2<String, ConcurrentLinkedQueue<String>> tmp = tuple2Iterator.next();
                                            KuduLoader kuduLoader = KuduLoader.getInstance(kuduMaster, 5, 5);
                                            System.out.println("----------" + tmp._1 + "------"+ kuduMaster+"--kuduloader is null:--"+ Boolean.toString(kuduLoader.kuduClient == null));
                                            kuduLoader.loadData(tmp._1, tmp._2);
                                        }
                                    }
                                }
                            );
                        }
                    }
                }
        );

        // Start the streaming context and await termination
        jssc.start();
        jssc.awaitTermination();
    }
}
