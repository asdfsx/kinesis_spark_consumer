package com.sample.aws.kinesis;

import org.json.JSONObject;

public class jsontest {
    public static void main(String[] args) throws Exception {
        JSONObject json = new JSONObject("{\"verified\":false,\"name\":{\"last\":\"Hankcs\",\"first\":\"Joe\"},\"userImage\":\"Rm9vYmFyIQ==\",\"gender\":\"MALE\"}\n");
        System.out.println(json.get("verified") instanceof Boolean);
        System.out.println(json.get("name") instanceof JSONObject);

        json = new JSONObject();
        json.put("appid", "kinesis");
        json.put("counter", 0);
        System.out.println(json.toString());
    }
}
