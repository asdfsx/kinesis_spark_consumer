package com.sample.aws.kinesis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import org.json.JSONObject;

public class KinesisProducer {

    public static final String STREAM_NAME = "test";

    public static void main(String[] args) throws Exception {
        AmazonKinesisClient kinesisClient =
                new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
                        .withRegion(Regions.US_WEST_2);

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(STREAM_NAME);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", Integer.toString(i));
            jsonObject.put("appid", "kinesisTest");
            jsonObject.put("counter", i);
            PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setPartitionKey("partitionKey1");
            putRecordsRequestEntry.setData(ByteBuffer.wrap(jsonObject.toString().getBytes()));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);

        PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);

        System.out.println("Put Result" + putRecordsResult);
    }
}
