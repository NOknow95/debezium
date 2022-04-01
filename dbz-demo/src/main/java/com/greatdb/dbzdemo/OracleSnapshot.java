package com.greatdb.dbzdemo;

import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONUtil;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/03/22
 */
@Slf4j
public class OracleSnapshot {

    @SneakyThrows
    public static void main(String[] args) {
        // Define the configuration for the Debezium Engine with MySQL connector...
        final Properties props = new Properties();
        props.setProperty("name", "oracle-engine");
        props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "./dbz-demo/tmp/offsets.txt");
        props.setProperty("offset.flush.interval.ms", "60000");
        /* begin connector properties */
        props.setProperty("database.hostname", "tmg");
        props.setProperty("database.port", "1521");
        props.setProperty("database.user", "c##dbzuser");
        props.setProperty("database.password", "dbz");
        props.setProperty("database.dbname", "ORCLCDB");
        props.setProperty("database.server.name", "oracle-server");
        props.setProperty("tasks.max", "1");
        props.setProperty("database.pdb.name", "ORCLPDB1");
        props.setProperty("database.connection.adapter", "logminer");

        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename", "./dbz-demo/tmp/dbhistory.txt");

        props.setProperty("table.include.list", "C##DBZUSER.CUSTOMERS,C##DBZUSER.STU");
        
        props.setProperty("snapshot.mode", "initial");
//        props.setProperty("snapshot.mode", "initial_only");
//        props.setProperty("snapshot.mode", "when_needed");
//        props.setProperty("snapshot.mode", "never");
//        props.setProperty("snapshot.mode", "schema_only");
//        props.setProperty("snapshot.mode", "schema_only_recovery");
//        props.setProperty("converter.schemas.enable", "false"); // don't include schema in message


        // Create the engine with this configuration ...
        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
//                .notifying((records, committer) -> {
//                    for (ChangeEvent<String, String> record : records) {
//                        String destination = record.destination();
//                        System.out.println("destination====>" + destination);
////                        System.out.println("key====>" + (Struct) record.key());
//                        String payload = ((JSONObject) JSONUtil.parse(record.value()).getByPath(".payload")).toStringPretty();
//                        System.out.println("payload====>" + payload);
//                        committer.markProcessed(record);
//                    }
//                    committer.markBatchFinished();
//                })
                .notifying(record -> {
                    log.info("\n--------------------record----------------------\nkey====>{}\nval====>{}", JSONUtil.formatJsonStr(record.key()), JSONUtil.formatJsonStr(record.value()));
                })
                .build();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        addShutdownHook(engine);
        awaitTermination(executor);
        System.out.println("------------main finished.");
    }

    private static void addShutdownHook(Closeable engine) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                engine.close();
                FileUtil.del("../../tmp");
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("------------engine closed");
        }, "shutdown_hook"));
    }

    private static void awaitTermination(ExecutorService executor) {
        if (executor != null) {
            try {
                executor.shutdown();
                while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
//                    log.info("Waiting another 5 seconds for the embedded engine to shut down");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}