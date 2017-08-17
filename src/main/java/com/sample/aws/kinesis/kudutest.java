package com.sample.aws.kinesis;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.ListTablesResponse;
import org.json.JSONObject;

import java.util.concurrent.ConcurrentLinkedQueue;

public class kudutest {
    public static void main(String[] args) throws Exception {
        JSONObject json = new JSONObject("{\"verified\":false,\"name\":{\"last\":\"Hankcs\",\"first\":\"Joe\"},\"userImage\":\"Rm9vYmFyIQ==\",\"gender\":\"MALE\"}\n");
        System.out.println(json.get("verified") instanceof Boolean);
        System.out.println(json.get("name") instanceof JSONObject);

        json = new JSONObject();
//        json.put("id", "1");
        json.put("appid", "kinesis");
        json.put("counter", 0);
        System.out.println(json.toString());

        ConcurrentLinkedQueue<JSONObject> jsons = new ConcurrentLinkedQueue<JSONObject>();
        jsons.add(json);

        KuduClient kuduClient = new KuduClient.KuduClientBuilder("10.0.0.104:7051").build();
        ListTablesResponse response = kuduClient.getTablesList();

        for (String table: response.getTablesList()){
            System.out.println(table);
        }
        KuduLoader kuduLoader = KuduLoader.getInstance("10.0.0.104:7051", 5, 5);
        kuduLoader.getKuduTable("kinesis1", jsons);
        kuduLoader._load("kinesis", jsons);
    }
}
