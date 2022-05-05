package io.debezium.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Pair;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import io.debezium.DebeziumException;
import io.debezium.chunk.Chunk;
import io.debezium.chunk.SliceColumn;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/04/15
 */
public class SingleTableSplitUtil {
    private static final Logger logger = LoggerFactory.getLogger(SingleTableSplitUtil.class);

    private static final Set<Integer> LONG_TYPE = new HashSet<>();
    private static final Set<Integer> STR_TYPE = new HashSet<>();
    private static final Set<Integer> SUPPORT_KEY_JDBC_TYPE = new HashSet<>();

    static {
        LONG_TYPE.add(Types.BIGINT);
        LONG_TYPE.add(Types.INTEGER);
        LONG_TYPE.add(Types.SMALLINT);
        LONG_TYPE.add(Types.TINYINT);
        LONG_TYPE.add(Types.NUMERIC);

        STR_TYPE.add(Types.CHAR);
        STR_TYPE.add(Types.NCHAR);
        STR_TYPE.add(Types.VARCHAR);
        STR_TYPE.add(Types.LONGVARCHAR);
        STR_TYPE.add(Types.NVARCHAR);

        SUPPORT_KEY_JDBC_TYPE.addAll(LONG_TYPE);
        SUPPORT_KEY_JDBC_TYPE.addAll(STR_TYPE);
    }

    private SingleTableSplitUtil() {
    }

    public static boolean supportChunks(Table table, String chunkKey) {
        if (StrUtil.isNotBlank(chunkKey)) {
            return table.columns().stream()
                    .anyMatch(column -> StrUtil.equalsIgnoreCase(column.name(), chunkKey) && SUPPORT_KEY_JDBC_TYPE.contains(column.jdbcType()));
        }
        List<Column> columns = table.primaryKeyColumns();
        if (CollUtil.size(columns) == 1) {
            return SUPPORT_KEY_JDBC_TYPE.contains(columns.get(0).jdbcType());
        }
        return false;
    }

    public static Pair<SliceColumn, SliceColumn> getKeyRange(Table table, String chunkKeyName, JdbcConnection jdbcConnection) {
        String sqlTemplate = "select min({}), max({}) from {}";
        String tableName = jdbcConnection.quotedTableIdString(table.id());
        Column column = resolveChunkColumn(table, chunkKeyName);
        String col = jdbcConnection.quotedColumnIdString(column.name());
        String sql = StrUtil.format(sqlTemplate, col, col, tableName);
        logger.info("Get key range sql:{}", sql);
        try {
            ResultSet rs = jdbcConnection.connection().createStatement().executeQuery(sql);
            if (rs.next()) {
                Boolean primaryColFlag = table.isPrimaryKeyColumn(column.name());
                SliceColumn minVal = new SliceColumn(column.name(), rs.getString(1), column.jdbcType(), primaryColFlag);
                SliceColumn maxVal = new SliceColumn(column.name(), rs.getString(2), column.jdbcType(), primaryColFlag);
                return Pair.of(minVal, maxVal);
            }
        } catch (SQLException e) {
            throw new ConnectException("Get key range error using:" + sql, e);
        }
        throw new ConnectException("No pk bound found by using sql:" + sql);
    }

    private static Column resolveChunkColumn(Table table, String chunkKeyName) {
        if (StrUtil.isBlank(chunkKeyName)) {
            return table.primaryKeyColumns().get(0);
        }
        Optional<Column> key = table.columns().stream()
                .filter(column -> StrUtil.equalsIgnoreCase(chunkKeyName, column.name()))
                .findFirst();
        return key.orElseGet(() -> table.primaryKeyColumns().get(0));
    }

    public static List<Chunk> splitChunks(SliceColumn minPk, SliceColumn maxPk, int sliceSize) {
        final int jdbcType = minPk.getJdbcType();
        final String columnName = minPk.getName();
        Object[] slices;
        boolean stringType = isStringType(jdbcType);
        if (stringType) {
            slices = RangeSplitUtil.doAsciiStringSplit(minPk.getValue(), maxPk.getValue(), sliceSize);
        } else if (isLongType(jdbcType)) {
            slices = RangeSplitUtil.doBigIntegerSplit(new BigInteger(minPk.getValue()), new BigInteger(maxPk.getValue()), sliceSize);
        } else {
            throw new DebeziumException("The type of chunk key is not support");
        }
        List<Chunk> chunks = new ArrayList<>(slices.length);
        for (int i = 0; i < slices.length - 1; i++) {
            String begin = stringType ? StrUtil.wrap(slices[i].toString(), "'") : slices[i].toString();
            String end = stringType ? StrUtil.wrap(slices[i + 1].toString(), "'") : slices[i + 1].toString();
            String id = IdUtil.fastSimpleUUID();
            String endJoiner = i < slices.length - 2 ? "<" : "<=";
            Chunk chunk = new Chunk(id, columnName, begin, end, jdbcType, endJoiner, false);
            chunks.add(chunk);
        }
        final boolean isPrimaryCol = BooleanUtil.isTrue(minPk.getPrimaryColFlag());
        if (!isPrimaryCol) {
            chunks.add(new Chunk(IdUtil.fastSimpleUUID(), columnName, true));
        }
        return chunks;
    }

    private static boolean isLongType(int type) {
        return LONG_TYPE.contains(type);
    }

    private static boolean isStringType(int type) {
        return STR_TYPE.contains(type);
    }
}
