package io.debezium.chunk;

import io.debezium.relational.TableId;

import java.util.Objects;

/**
 * @author wang.jianwen
 * @version 1.0
 * @since 2022/05/05
 */
public class TabId {

    private String catalogName;
    private String schemaName;
    private String tableName;

    public static TabId of(TableId tableId) {
        TabId tabId = new TabId();
        tabId.setCatalogName(tableId.catalog());
        tabId.setSchemaName(tableId.schema());
        tabId.setTableName(tableId.table());
        return tabId;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TabId tabId = (TabId) o;
        return Objects.equals(catalogName, tabId.catalogName) && Objects.equals(schemaName, tabId.schemaName) && Objects.equals(tableName, tabId.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, schemaName, tableName);
    }
}
