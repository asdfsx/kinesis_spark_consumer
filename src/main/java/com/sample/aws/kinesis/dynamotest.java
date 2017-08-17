package com.sample.aws.kinesis;

import com.amazonaws.internal.DefaultServiceEndpointBuilder;
import com.amazonaws.regions.RegionUtils;

import java.net.URI;

public class dynamotest {
    public static void main(String[] args) throws Exception {
        DynamoAppinfo.scanAppinfo("us-west-2", "app");
        URI uri = (new DefaultServiceEndpointBuilder("kinesis", "https"))
                .withRegion(RegionUtils.getRegion("us-west-2")).getServiceEndpoint();
        System.out.println(uri.toString());
    }
}
