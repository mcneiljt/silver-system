package com.mcneilio.silversystem;

import com.mcneilio.silversystem.format.Firehose;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.*;

public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv("KAFKA_GROUP_ID"));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(System.getenv("KAFKA_TOPIC")));

//        HiveConf hiveConf = new HiveConf();
//        hiveConf.set("hive.metastore.local", "false");
//
//        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, System.getenv("HIVE_URL"));
//        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
//        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
//        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
//        HiveMetaStoreClient hiveMetaStoreClient = null;
//        try {
//            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf, null);
//        } catch (MetaException e) {
//            e.printStackTrace();
//        }

        HashMap<String, Set<String>> schemas = new HashMap<>();

        while (true) {
            ConsumerRecords<String,byte[]> records = consumer.poll(Duration.ofMillis(
                    Integer.parseInt(System.getenv("KAFKA_POLL_DURATION_MS"))));
            for (ConsumerRecord<String,byte[]> record : records) {
                Firehose f = new Firehose(record.value());
                String eventName = f.getTopic();
                if (!schemas.containsKey(eventName)) {
                    //try to get the schema and save it in the map
                    //catch hive no such table error and throw away the record; continue
                }
                JSONObject msg = new JSONObject(f.getMessage());
                List<String> cols = new ArrayList<>();
                reduceSchema(cols, msg, "");
            }
        }
    }

    private static void reduceSchema(List<String> cols, JSONObject obj, String prefix) {
        obj.keys().forEachRemaining(key -> {
            String newKey = prefix + "_" + snakeCase(key);
            if (!checkPredicate(cols, key)) {
                obj.remove(key);
                return;
            }
            if (obj.get(key) instanceof JSONObject) {
                reduceSchema(cols, (JSONObject) obj.get(key), newKey);
            }
        });

    }

    private static boolean checkPredicate(List<String> cols, String predicate) {
        //do any of the cols start with predicate?
        return true;
    }

    private static String snakeCase(String k) {
        //snake casing shenanigans
        return k;
    }
}
