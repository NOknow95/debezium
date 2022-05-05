package com.nk.test;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.thread.NamedThreadFactory;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.debezium.chunk.TableOffset;
import io.debezium.chunk.TableOffsets;
import io.debezium.config.Configuration;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.OffsetUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static io.debezium.config.CommonConnectorConfig.SNAPSHOT_FETCH_SIZE;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/14
 */
public class SimpleTest {

    @Test
    public void jdbcTest() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://tmg:3306/inventory?useSSL=false&allowPublicKeyRetrieval=true",
                "mysqluser", "mysqlpw");
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        MapUtil.builder(1000, 1002).put(1003, 1005).put(1006, 1009).build().forEach((k, v) -> {
            executorService.execute(() -> {
                try {
                    ResultSet rs = conn.createStatement().executeQuery(StrUtil.format("select * from customers where id between {} and {}", k, v));
                    int columnCount = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        List<Object> data = new LinkedList<>();
                        for (int i = 0; i < columnCount; i++) {
                            data.add(rs.getObject(i + 1));
                        }
                        System.out.println(Thread.currentThread().getName() + "\t" + data);
                    }
                    System.out.println("------------------------------------------");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        });

        executorService.shutdown();
        while (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
        }
        //conn.close();
    }

    @Test
    public void testOffset() throws Exception {
        ExecutorService chunkThreadPool = Executors.newFixedThreadPool(3, new NamedThreadFactory("chunk-thread-pool-", false));
        CompletionService<Void> chunkTaskService = new ExecutorCompletionService<>(chunkThreadPool);
        for (int i = 0; i < 3; i++) {
            int finalI = i;
            chunkTaskService.submit(() -> {
                ThreadUtil.safeSleep(2000);
                System.out.println(Thread.currentThread().getName() + "\tdone " + finalI);
                return null;
            });
        }
        for (int i = 0; i < 3; i++) {
            chunkTaskService.take();
        }
        chunkThreadPool.shutdown();
        System.out.println("------------------------------------------");
    }

    @Test
    public void offsetSer() throws JsonProcessingException {
        String v = "rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABc3IAHmlvLmRlYmV6aXVtLnJlbGF0aW9uYWwuVGFibGVJZH3Xn+8qnRMlAgAETAALY2F0YWxvZ05hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAJpZHEAfgADTAAKc2NoZW1hTmFtZXEAfgADTAAJdGFibGVOYW1lcQB+AAN4cHQACE9SQ0xQREIxdAAdT1JDTFBEQjEuQyMjREJaVVNFUi5DVVNUT01FUlN0AApDIyNEQlpVU0VSdAAJQ1VTVE9NRVJTc3IAImlvLmRlYmV6aXVtLmJyZWFrcG9pbnQuVGFibGVPZmZzZXTHGYVaEMEZ6QIACFoACWNvbXBsZXRlZEkACXNsaWNlU2l6ZUwABmNodW5rc3QAD0xqYXZhL3V0aWwvTWFwO0wAC2xvZ1Bvc2l0aW9ucQB+AApMAAVtYXhQa3QAG0xpby9kZWJleml1bS9icmVha3BvaW50L1BrO0wABW1pblBrcQB+AAtMAAd0YWJsZUlkcQB+AANMABR1bkNvbXBsZXRlZENodW5rU2l6ZXQAK0xqYXZhL3V0aWwvY29uY3VycmVudC9hdG9taWMvQXRvbWljSW50ZWdlcjt4cAEAAAAQc3EAfgAAP0AAAAAAAAx3CAAAABAAAAADdAAgYjIzOTY2N2U5NTE4NDkzZmI2MjBkYzQ1ZDU4MjgxYzdzcgAcaW8uZGViZXppdW0uYnJlYWtwb2ludC5DaHVua6T5yFnnnZYQAgAJWgAJY29tcGxldGVkSQAIamRiY1R5cGVaAAZudWxsUGtKAARyb3dzTAAFYmVnaW5xAH4AA0wACmNvbHVtbk5hbWVxAH4AA0wAA2VuZHEAfgADTAAJZW5kSm9pbmVycQB+AANMAAJpZHEAfgADeHABAAAAAgAAAAAAAAAAAXQABDEwMDF0AAJJRHQABDEwMDJ0AAE8cQB+AA90ACAyZTg4MjFkODQwNzY0OGVlOTNiYzBjMjk5MDNmOTdjZXNxAH4AEAEAAAACAAAAAAAAAAABdAAEMTAwMnEAfgATdAAEMTAwM3EAfgAVcQB+ABZ0ACBkNWVkZTMxYTNjMjI0YjIxODQxOTE3MGEzYzQ5N2JjY3NxAH4AEAEAAAACAAAAAAAAAAACdAAEMTAwM3EAfgATdAAEMTAwNHQAAjw9cQB+ABp4c3EAfgAAP0AAAAAAAAZ3CAAAAAgAAAABdAADc2NudAAHODQ0Mjk5NHhzcgAZaW8uZGViZXppdW0uYnJlYWtwb2ludC5Qa/oWntO4SbT9AgADSQAIamRiY1R5cGVMAARuYW1lcQB+AANMAAV2YWx1ZXEAfgADeHAAAAACcQB+ABN0AAQxMDA0c3EAfgAiAAAAAnEAfgATdAAEMTAwMXEAfgAGc3IAKWphdmEudXRpbC5jb25jdXJyZW50LmF0b21pYy5BdG9taWNJbnRlZ2VyVj9ezIxsFooCAAFJAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAB4";
        TableOffsets tableOffsets = new TableOffsets();
        tableOffsets.loadFromOffset(MapUtil.of("tab_offset", v));
        System.out.println("tableOffsets = " + tableOffsets);
    }

    @Test
    public void connectionPool() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", "jdbc:mysql://tmg:3306/inventory?useSSL=false");
        properties.setProperty("username", "mysqluser");
        properties.setProperty("password", "mysqlpw");
        properties.setProperty("maximumPoolSize", "30");
        HikariConfig config = new HikariConfig(properties);

        //config.setJdbcUrl("jdbc:mysql://tmg:3306/inventory?useSSL=false");
        //config.setUsername("mysqluser");
        //config.setPassword("mysqlpw");

        HikariDataSource ds = new HikariDataSource(config);
        System.out.println("ds.getMaximumPoolSize() = " + ds.getMaximumPoolSize());

        String sql = "select id_num from user;";
        Set<String> set = new ConcurrentHashSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 3000; i++) {
            executorService.submit(() -> {
                try {
                    Connection connection = ds.getConnection();
                    String connStr = connection.toString();
                    String s;
                    if (set.contains(s = connStr.substring(connStr.indexOf("com.")))) {
                        //System.out.println("reusing the conn:" + connStr);
                    } else {
                        //System.out.println("create new conn:" + connStr);
                    }
                    set.add(s);
                    //System.out.println("connection = " + connStr);
                    try (Statement statement = connection.createStatement();
                         ResultSet rs = statement.executeQuery(sql)) {
                        //while (rs.next()) {
                        //    System.out.println(rs.getObject(1));
                        //}
                        //System.out.println("------------------------------------------");
                    }
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

        }
        executorService.shutdown();
        while (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
        }
        System.out.println("finish----" + set.size());
    }

    @Test
    public void config() {
        Configuration configuration = Configuration.from(new HashMap<>());
        int integer = configuration.getInteger(SNAPSHOT_FETCH_SIZE);
        System.out.println("integer = " + integer);
    }

    @Test
    public void offsetValidation() {
        Map<String, Object> map = new HashMap<>();
        map.put("test1", "test_val");
        map.put("test2", 111L);
        map.put("test3", 4545.6);
        map.put("test4", 88);
        JsonConverter valueConverter = new JsonConverter();
        valueConverter.configure(new HashMap<String, Object>() {{
            put("schemas.enables", "false");
            put("converter.type", "value");
        }});

        byte[] value = valueConverter.fromConnectData("my", null, map);
        ByteBuffer valueBuffer = (value != null) ? ByteBuffer.wrap(value) : null;
        OffsetUtils.validateFormat(map);
    }

    private static final Pattern UUID_PATTERN = Pattern.compile("\\w{8}(-\\w{4}){3}-\\w{12}");
    //private static final Pattern UUID_PATTERN = Pattern.compile("[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}");

    @Test
    public void json() throws JsonProcessingException {
        String str = "[{\"tabId\":{\"catalogName\":\"inventory\",\"schemaName\":null,\"tableName\":\"orders\"},\"minCol\":{\"name\":\"price\",\"value\":\"1\",\"jdbcType\":4},\"maxCol\":{\"name\":\"price\",\"value\":\"793\",\"jdbcType\":4},\"sliceSize\":3,\"completed\":true,\"chunks\":{\"973728646ad549489509e8babc52a2dc\":{\"id\":\"973728646ad549489509e8babc52a2dc\",\"columnName\":\"price\",\"begin\":\"529\",\"end\":\"793\",\"endJoiner\":\"<=\",\"jdbcType\":4,\"completeFlag\":true,\"rows\":2,\"nullValueFlag\":false},\"f1e6286103104afe871cabb2ef6bc5d4\":{\"id\":\"f1e6286103104afe871cabb2ef6bc5d4\",\"columnName\":\"price\",\"begin\":\"265\",\"end\":\"529\",\"endJoiner\":\"<\",\"jdbcType\":4,\"completeFlag\":true,\"rows\":2,\"nullValueFlag\":false},\"264cf50dc27849b4855231a47c012a25\":{\"id\":\"264cf50dc27849b4855231a47c012a25\",\"columnName\":\"price\",\"begin\":\"1\",\"end\":\"265\",\"endJoiner\":\"<\",\"jdbcType\":4,\"completeFlag\":true,\"rows\":4,\"nullValueFlag\":false}},\"logPosition\":{\"binlogFilename\":\"mysql-bin.000009\",\"gtId\":null,\"binlogPosition\":\"3069\"}}]";
        ObjectMapper objectMapper = new ObjectMapper();
        LinkedList<TableOffset> data = objectMapper.readValue(str, new TypeReference<LinkedList<TableOffset>>() {
        });
        System.out.println("data = " + data);
    }


}