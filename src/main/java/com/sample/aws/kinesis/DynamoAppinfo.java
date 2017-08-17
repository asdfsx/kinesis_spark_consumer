package com.sample.aws.kinesis;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public final class DynamoAppinfo {
    private static AmazonDynamoDB client ;
    private static DynamoDB dynamoDB ;
    private static final Map<String, ConcurrentHashMap<String, String>> appInfo =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, String>>();

    public static void scanAppinfo(String regionName, String tableName){
        client = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain())
                .withRegion(RegionUtils.getRegion(regionName));
        dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        ItemCollection<ScanOutcome> items =table.scan();
        Iterator<Item> iterator = items.iterator();
        while(iterator.hasNext()){
            Item item = iterator.next();
            ConcurrentHashMap<String, String> tmp = new ConcurrentHashMap<String, String>();
            tmp.putIfAbsent("appid", item.getString("appid"));
            appInfo.putIfAbsent(item.getString("appid"), tmp);
        }
    }

    public static void queryAppInfo(String regionName, String tableName, String appid){
        client = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain())
                .withRegion(RegionUtils.getRegion(regionName));
        dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(tableName);
        Item item = table.getItem("appid", appid);
        if (appInfo.containsKey("appid")){
            appInfo.get(appid).putIfAbsent("appid", item.getString("appid"));
        } else {
            ConcurrentHashMap<String, String> tmp = new ConcurrentHashMap<String, String>();
            tmp.putIfAbsent("appid", item.getString("appid"));
            appInfo.putIfAbsent(item.getString("appid"), tmp);
        }
    }
}
