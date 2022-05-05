package com.nk.test;

import io.debezium.chunk.Chunk;
import io.debezium.chunk.SliceColumn;
import io.debezium.util.SingleTableSplitUtil;
import org.junit.Test;

import java.sql.Types;
import java.util.List;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/22
 */
public class SingleTableSplitUtilTest {

    @Test
    public void splitChunks() {
        List<Chunk> chunks = SingleTableSplitUtil.splitChunks(new SliceColumn("id", "1000", Types.VARCHAR, Boolean.FALSE), new SliceColumn("id", "1009", Types.VARCHAR, Boolean.FALSE), 11);
        for (Chunk chunk : chunks) {
            System.out.println(chunk);
        }
    }
}
