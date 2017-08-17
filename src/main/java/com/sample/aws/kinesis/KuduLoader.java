package com.sample.aws.kinesis;

import com.google.common.collect.ImmutableList;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.kudu.client.shaded.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.parquet.io.api.Binary;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.io.Serializable;

/**
 * Created by sunxia on 2017/8/3.
 */
public final class KuduLoader implements Serializable {

    private volatile static KuduLoader instance;

    private String kuduMaster;
    private int batchSize;
    private int intervalSecond;

    public KuduClient kuduClient;
    private ConcurrentMap<String, ConcurrentLinkedQueue<JSONObject>> dataCache;
    private ConcurrentMap<String, KuduTable> kuduTableCache;

    private KuduLoader(String kuduMaster, int batchSize, int intervalSecond){
        this.kuduMaster = kuduMaster;
        this.batchSize = batchSize;
        this.intervalSecond = intervalSecond;
        this.kuduClient = new KuduClientBuilder(this.kuduMaster).build();
        this.dataCache = new ConcurrentHashMap<String, ConcurrentLinkedQueue<JSONObject>>();
        this.kuduTableCache = new ConcurrentHashMap<String, KuduTable>();
    }

    public static KuduLoader getInstance(String kuduMaster, int batchSize, int intervalSecond) {
        if (instance == null) {
            synchronized (KuduLoader.class) {
                if (instance == null) {
                    instance = new KuduLoader(kuduMaster, batchSize, intervalSecond);
                }
            }
        }
        return instance;
    }

    public KuduTable getKuduTable(String appid, ConcurrentLinkedQueue<JSONObject> jsons) throws KuduException{
        KuduTable kuduTable = null;
        if (this.kuduTableCache.containsKey(appid)){
            kuduTable = this.kuduTableCache.get(appid);
        } else if (this.kuduClient.tableExists(appid)){
            kuduTable = this.kuduClient.openTable(appid);
            this.kuduTableCache.put(appid, kuduTable);
        } else {
            Schema schema = this.creatSchema(jsons);
            CreateTableOptions options = this.createOption();

            kuduTable = this.kuduClient.createTable(appid, schema, options);

            this.kuduTableCache.put(appid, kuduTable);
        }

        return kuduTable;
    }

    public void loadData(String appid, ConcurrentLinkedQueue<String> jsons){
        ConcurrentLinkedQueue<JSONObject> jsons2 = new ConcurrentLinkedQueue<JSONObject>();
        for(String js: jsons){
            jsons2.add(new JSONObject(js));
        }
        if (jsons2.size() > this.batchSize) {
            this._loadDataAndCheckTheSchema(appid, jsons2);
        } else if (this.dataCache.containsKey(appid)){
            this.dataCache.get(appid).addAll(jsons2);
        } else {
            this.dataCache.put(appid, jsons2);
        }
    }

    public void loadData2(String appid, ConcurrentLinkedQueue<JSONObject> jsons){
        if (jsons.size() > this.batchSize) {
            this._loadDataAndCheckTheSchema(appid, jsons);
        } else if (this.dataCache.containsKey(appid)){
            this.dataCache.get(appid).addAll(jsons);
        } else {
            this.dataCache.put(appid, jsons);
        }
    }

    public void loadRest() {
        for (Entry<String, ConcurrentLinkedQueue<JSONObject>> tmp: this.dataCache.entrySet()){
            this._loadDataAndCheckTheSchema(tmp.getKey(), tmp.getValue());
        }
    }

    public void _loadDataAndCheckTheSchema(String appid, ConcurrentLinkedQueue<JSONObject> jsons){
        try {
            this._load(appid, jsons);
        } catch (KuduException e){
            e.printStackTrace();
            this._checkSchema(appid, jsons);
            this._loadDataAndCheckTheSchema(appid, jsons);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void _checkSchema(String appid, ConcurrentLinkedQueue<JSONObject> jsons){
        try {
            KuduTable kuduTable = this.kuduClient.openTable(appid);
            AlterTableOptions options = new AlterTableOptions();
            Integer optionCounter = 0;
            for (JSONObject json : jsons) {
                for (String key : json.keySet()) {
                    //如果字段不存在，会抛 IllegalArgumentException 异常
                    try{
                        kuduTable.getSchema().getColumnIndex(key);
                    } catch (IllegalArgumentException e){
                        e.printStackTrace();
                        //增加字段
                        Object object = json.get(key);
                        if (object instanceof Boolean) {
                            options.addColumn(key, Type.BOOL, true);
                        } else if (object instanceof String){
                            options.addColumn(key, Type.STRING, "");
                        } else if (object instanceof Double){
                            options.addColumn(key, Type.DOUBLE, 0.0);
                        } else if (object instanceof Float){
                            options.addColumn(key, Type.FLOAT, 0.0);
                        } else if (object instanceof Short){
                            options.addColumn(key, Type.INT16, 0);
                        } else if (object instanceof Integer){
                            options.addColumn(key, Type.INT32, 0);
                        } else if (object instanceof Long){
                            options.addColumn(key, Type.INT64, 0);
                        } else if (object instanceof Byte){
                            options.addColumn(key, Type.INT8, 0);
                        } else if (object instanceof Binary){
                            options.addColumn(key, Type.BINARY, 0);
                        }
                        optionCounter += 1;
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
            if (optionCounter > 0) {
                kuduClient.alterTable(appid, options);
                while (!kuduClient.isAlterTableDone(appid)){
                    try {
                        TimeUnit.SECONDS.sleep(5L);
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }
                }
                kuduTable = this.kuduClient.openTable(appid);
            }
            this.kuduTableCache.put(appid, kuduTable);
        }catch (KuduException e){
            e.printStackTrace();
        }
    }

    public Boolean isPrimaryKey(String keyName){
        switch (keyName){
            case "category":
            case "timestamp":
            case "id":
                return true;
            default:
                return false;
        }
    }

    public Schema creatSchema(ConcurrentLinkedQueue<JSONObject> jsons) {
        List<ColumnSchema> cols = new ArrayList<>();
        for (JSONObject json : jsons) {
            for (String key : json.keySet()) {
                Boolean isPrimary = this.isPrimaryKey(key);

                Object object = json.get(key);
                if (object instanceof Boolean) {
                    cols.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.BOOL).encoding(
                            ColumnSchema.Encoding.PLAIN_ENCODING).key(isPrimary).defaultValue(true).build());
                } else if (object instanceof String){
                    cols.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.STRING).encoding(
                            ColumnSchema.Encoding.PLAIN_ENCODING).key(isPrimary).defaultValue("").build());
                } else if (object instanceof Double){
                    cols.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.DOUBLE).encoding(
                            ColumnSchema.Encoding.PLAIN_ENCODING).key(isPrimary).defaultValue(0.0).build());
                } else if (object instanceof Float){
                    cols.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.FLOAT).encoding(
                            ColumnSchema.Encoding.PLAIN_ENCODING).key(isPrimary).defaultValue(0.0).build());
                } else if (object instanceof Short){
                    cols.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.INT16).encoding(
                            ColumnSchema.Encoding.PLAIN_ENCODING).key(isPrimary).defaultValue(0).build());
                } else if (object instanceof Integer){
                    cols.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.INT32).encoding(
                            ColumnSchema.Encoding.PLAIN_ENCODING).key(isPrimary).defaultValue(0).build());
                } else if (object instanceof Long){
                    cols.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.INT64).encoding(
                            ColumnSchema.Encoding.PLAIN_ENCODING).key(isPrimary).defaultValue(0).build());
                } else if (object instanceof Byte){
                    cols.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.INT8).encoding(
                            ColumnSchema.Encoding.PLAIN_ENCODING).key(isPrimary).defaultValue(0).build());
                } else if (object instanceof ByteBuffer){
                    cols.add(new ColumnSchema.ColumnSchemaBuilder(key, Type.BINARY).encoding(
                            ColumnSchema.Encoding.PLAIN_ENCODING).key(isPrimary).defaultValue(0).build());
                }
            }
        }
        Schema schema = new Schema(cols);
        return schema;
    }

    public CreateTableOptions createOption(){
        List<String> rangeKeys = new ArrayList<>();

        CreateTableOptions options = new CreateTableOptions();
        options.setRangePartitionColumns(rangeKeys);
        options.setNumReplicas(Common.kuduReplicas);
        options.addHashPartitions(ImmutableList.of("category", "timestamp", "id"), Common.kuduBucket);
        return options;
    }

    public void _load(String appid, ConcurrentLinkedQueue<JSONObject> jsons) throws Exception{
        KuduSession session = this.kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

        KuduTable kudutable = this.getKuduTable(appid, jsons);

        Operation operation;
        for (JSONObject json: jsons){
            operation = kudutable.newInsert();
            for (String key :json.keySet()){
                Object object = json.get(key);

                if (object instanceof Boolean) {
                    operation.getRow().addBoolean(key, (Boolean) object);
                } else if (object instanceof String){
                    operation.getRow().addString(key, (String) object);
                } else if (object instanceof Double){
                    operation.getRow().addDouble(key, (Double) object);
                } else if (object instanceof Float){
                    operation.getRow().addFloat(key, (Float) object);
                } else if (object instanceof Short){
                    operation.getRow().addShort(key, (Short) object);
                } else if (object instanceof Integer){
                    operation.getRow().addInt(key, (Integer) object);
                } else if (object instanceof Long){
                    operation.getRow().addLong(key, (Long) object);
                } else if (object instanceof Byte){
                    operation.getRow().addByte(key, (Byte) object);
                } else if (object instanceof ByteBuffer){
                    operation.getRow().addBinary(key, (ByteBuffer) object);
                }
            }
            session.apply(operation);
        }

        session.flush();
        session.close();
    }
}
