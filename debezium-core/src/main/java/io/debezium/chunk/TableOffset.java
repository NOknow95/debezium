package io.debezium.chunk;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.BooleanUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/20
 */
@JsonIgnoreProperties(value = {"unCompletedChunkSize"}, ignoreUnknown = true)
public class TableOffset implements Serializable {

    private static final long serialVersionUID = -4100099363869484567L;

    private TabId tabId;
    /**
     * Pair:jdbcType({@see java.sql.Types}) - pk value
     */
    private SliceColumn minCol;
    private SliceColumn maxCol;
    private int sliceSize;
    private Boolean completed;
    /**
     * chunkId-Chunk
     */
    private Map<String, Chunk> chunks;

    private Map<String, String> logPosition;
    private final transient AtomicInteger unCompletedChunkSize;

    public TableOffset() {
        this.unCompletedChunkSize = new AtomicInteger();
    }

    public TableOffset(TabId tabId, SliceColumn minCol, SliceColumn maxCol, int sliceSize, List<Chunk> chunks) {
        this.tabId = tabId;
        this.minCol = minCol;
        this.maxCol = maxCol;
        this.sliceSize = sliceSize;
        this.completed = false;
        this.chunks = CollUtil.emptyIfNull(chunks).stream().collect(Collectors.toMap(Chunk::getId, i -> i));
        this.logPosition = new HashMap<>(8);
        this.unCompletedChunkSize = new AtomicInteger(CollUtil.size(chunks));
    }

    public void putLogPosition(String key, String val) {
        logPosition.putIfAbsent(key, val);
    }

    public List<Chunk> unCompletedChunks() {
        return chunks.values().stream().filter(it -> !it.getCompleteFlag()).collect(Collectors.toList());
    }

    public void complete() {
        completed = true;
    }

    public void completeChunk(String chunkId, long rows) {
        Optional.ofNullable(chunks.get(chunkId)).ifPresent(chunk -> {
            chunk.complete(rows);
            unCompletedChunkSize.decrementAndGet();
        });
    }

    public boolean lastChunk() {
        return unCompletedChunkSize.get() == 1;
    }

    // region getter and setter
    public TabId getTabId() {
        return tabId;
    }

    public void setTabId(TabId tabId) {
        this.tabId = tabId;
    }

    public SliceColumn getMinCol() {
        return minCol;
    }

    public void setMinCol(SliceColumn minCol) {
        this.minCol = minCol;
    }

    public SliceColumn getMaxCol() {
        return maxCol;
    }

    public void setMaxCol(SliceColumn maxCol) {
        this.maxCol = maxCol;
    }

    public int getSliceSize() {
        return sliceSize;
    }

    public void setSliceSize(int sliceSize) {
        this.sliceSize = sliceSize;
    }

    public Boolean getCompleted() {
        return completed;
    }

    public void setCompleted(Boolean completed) {
        this.completed = completed;
    }

    public Map<String, Chunk> getChunks() {
        return chunks;
    }

    public void setChunks(Map<String, Chunk> chunks) {
        this.chunks = chunks;
        long count = Optional.ofNullable(chunks).orElse(Collections.emptyMap()).values().stream()
                .filter(chunk -> !BooleanUtil.isTrue(chunk.getCompleteFlag()))
                .count();
        unCompletedChunkSize.set(Math.toIntExact(count));
    }

    public Map<String, String> getLogPosition() {
        return logPosition;
    }

    public void setLogPosition(Map<String, String> logPosition) {
        this.logPosition = logPosition;
    }

    // endregion
}
