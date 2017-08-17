package com.sample.aws.kinesis;

import org.apache.spark.Partitioner;

public class AppidPartitioner  extends Partitioner {
    private int numPartitions;

    public AppidPartitioner(int numParts) {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return this.numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        String appid = key.toString();
        int code = appid.hashCode() % this.numPartitions;
        if (code < 0) {
            code += this.numPartitions();
        }
        return code;
    }

    @Override
    public boolean equals(Object obj){

        if (obj instanceof AppidPartitioner){
            AppidPartitioner pObj = (AppidPartitioner)obj;
            return pObj.numPartitions() == this.numPartitions;
        }else{
            return false;
        }
    }

    @Override
    public int hashCode(){
        return this.numPartitions;
    }
}
