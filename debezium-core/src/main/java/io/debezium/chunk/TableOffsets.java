package io.debezium.chunk;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/20
 */
public class TableOffsets {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Map<TabId, TableOffset> tableOffsetMap = new HashMap<>();
    private final LinkedList<TableOffset> tableOffsetData = new LinkedList<>();

    public boolean isCompleted(TableId tableId) {
        TabId tabId = TabId.of(tableId);
        return Optional.ofNullable(tableOffsetMap.get(tabId)).map(TableOffset::getCompleted).orElse(false);
    }

    public void init(Table table, TableOffset tableOffset) {
        TabId tabId = TabId.of(table.id());
        tableOffsetMap.put(tabId, tableOffset);
        tableOffsetData.add(tableOffset);
    }

    public void putLogPosition(TableId tableId, String key, String val) {
        get(tableId, tableOffset -> tableOffset.putLogPosition(key, val));
    }

    public List<Chunk> getUnCompletedChunks(TableId tableId) {
        return Optional.ofNullable(get(tableId)).map(TableOffset::unCompletedChunks).orElse(Collections.emptyList());
    }

    public TableOffset get(TableId tableId) {
        TabId tabId = TabId.of(tableId);
        return tableOffsetMap.get(tabId);
    }

    public void get(TableId tableId, Consumer<TableOffset> existConsumer) {
        TabId tabId = TabId.of(tableId);
        Optional.ofNullable(tableOffsetMap.get(tabId)).ifPresent(existConsumer);
    }

    public boolean initBefore(TableId tableId) {
        TabId tabId = TabId.of(tableId);
        return tableOffsetMap.containsKey(tabId);
    }

    public void completeTable(TableId tableId) {
        get(tableId, TableOffset::complete);
    }

    public synchronized void completeChunk(TableId tableId, String chunkId, long rows) {
        get(tableId, tableOffset -> tableOffset.completeChunk(chunkId, rows));
    }

    public void completeChunkAndTable(TableId tableId, String chunkId, long rows) {
        completeChunk(tableId, chunkId, rows);
        completeTable(tableId);
    }

    public boolean lastChunkOfTable(TableId tableId) {
        TableOffset tableOffset = get(tableId);
        return tableOffset == null || tableOffset.lastChunk();
    }

    public void put2Offset(Map<String, Object> offset) {
        if (CollUtil.isEmpty(tableOffsetData)) {
            return;
        }
        String val;
        try {
            val = MAPPER.writeValueAsString(tableOffsetData);
        } catch (JsonProcessingException e) {
            val = "Offset to Json error";
        }
        offset.put("tab_offset", val);
    }

    public void loadFromOffset(Map<String, ?> offset) {
        tableOffsetData.clear();
        tableOffsetMap.clear();
        Object o = offset.get("tab_offset");
        if (StrUtil.isBlankIfStr(o)) {
            return;
        }
        try {
            LinkedList<TableOffset> data = MAPPER.readValue(o.toString(), new TypeReference<LinkedList<TableOffset>>() {
            });
            Map<TabId, TableOffset> map = data.stream().collect(Collectors.toMap(TableOffset::getTabId, it -> it, (m, n) -> n));
            tableOffsetMap.putAll(map);
        } catch (JsonProcessingException ignored) {
        }
    }
}
