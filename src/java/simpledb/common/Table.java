package simpledb.common;

import simpledb.storage.DbFile;

import java.util.Objects;

public class Table {
    private DbFile file;
    private String name; // 表名
    private String pkeyField; // 主键字段名

    public Table(DbFile file, String name, String pkeyField) {
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    public DbFile getFile() {
        return file;
    }

    public void setFile(DbFile file) {
        this.file = file;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPkeyField() {
        return pkeyField;
    }

    public void setPkeyField(String pkeyField) {
        this.pkeyField = pkeyField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Table table = (Table) o;
        return Objects.equals(getFile(), table.getFile()) &&
                Objects.equals(getName(), table.getName()) && Objects.equals(getPkeyField(), table.getPkeyField());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFile(), getName(), getPkeyField());
    }
}
