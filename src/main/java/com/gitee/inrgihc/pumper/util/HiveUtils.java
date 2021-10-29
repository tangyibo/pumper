package com.gitee.inrgihc.pumper.util;

import com.gitee.inrgihc.pumper.model.ColumnDescription;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public final class HiveUtils {

  @SneakyThrows
  public static String genTruncateTableSql(String schemaName, String tableName) {
    return String.format("TRUNCATE TABLE `%s`.`%s`", schemaName, tableName);
  }

  @SneakyThrows
  public static String genDropTableSql(String schemaName, String tableName) {
    return String.format("DROP TABLE IF EXISTS `%s`.`%s`", schemaName, tableName);
  }

  @SneakyThrows
  public static void truncateTable(Connection connection, String schemaName, String tableName) {
    executeHiveql(connection, genTruncateTableSql(schemaName, tableName));
  }

  @SneakyThrows
  public static void dropTable(Connection connection, String schemaName, String tableName) {
    executeHiveql(connection, genDropTableSql(schemaName, tableName));
  }

  @SneakyThrows
  public static void createTable(Connection connection, String schemaName, String tableName,
      List<ColumnDescription> columnDescriptor, String split) {
    executeHiveql(connection, genCreateTableSql(schemaName, tableName, columnDescriptor, split));
  }

  public static void executeHiveql(Connection connection, String hiveql) throws Exception {
    try (Statement stmt = connection.createStatement()) {
      log.info("Connection: [{}] execute sql: \n{}", connection, hiveql);
      stmt.executeUpdate(hiveql);
    }
  }

  @SneakyThrows
  public static String getTableLocationOnHdfs(Connection connection, String schemaName,
      String tableName, String hdfsUrl) {
    String sql = String.format("desc formatted `%s`.`%s`", schemaName, tableName);
    try (Statement stmt = connection.createStatement(); ResultSet res = stmt.executeQuery(sql)) {
      while (res.next()) {
        String name = res.getString(1);
        String value = res.getString(2);
        if (StringUtils.isNotEmpty(name) && name.trim().equalsIgnoreCase("Location:")) {
          URI uri = new URI(value);
          return hdfsUrl + uri.getPath();
        }
      }
    }

    throw new RuntimeException(
        String.format("Obtain table : `%s`.`%s` location on hdfs failed",
            schemaName, tableName));
  }

  public static String genCreateTableSql(String schemaName, String tableName,
      List<ColumnDescription> columnDescriptor, String split) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ");
    sb.append(String.format("`%s`.`%s`", schemaName, tableName));
    sb.append("\n(\n");
    for (int i = 0; i < columnDescriptor.size(); ++i) {
      ColumnDescription description = columnDescriptor.get(i);
      sb.append(String.format(" `%s` %s", description.getFieldName(), description.getFieldType()));
      if (i != columnDescriptor.size() - 1) {
        sb.append(",\n");
      }
    }
    sb.append("\n)\n");
    sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + split + "'\n");
    sb.append("STORED AS TEXTFILE");
    sb.append("\n");
    return sb.toString();
  }

  private HiveUtils() {
  }

  public static void main(String[] args) {
    String schemaName = "demo";
    String tableName = "test";
    List<ColumnDescription> columns = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      ColumnDescription cd = new ColumnDescription();
      cd.setFieldName("col_" + (i + 1));
      if (i <= 3) {
        cd.setFieldType("String");
        cd.setDateType(Types.VARCHAR);
      } else if (i <= 6) {
        cd.setFieldType("int");
        cd.setDateType(Types.INTEGER);
      } else if (i <= 8) {
        cd.setFieldType("DATE");
        cd.setDateType(Types.DATE);
      } else {
        cd.setFieldType("DOUBLE");
        cd.setDateType(Types.DOUBLE);
      }
      columns.add(cd);
    }

    String sql = HiveUtils.genCreateTableSql(schemaName, tableName, columns, ";");
    System.out.println(sql);
  }
}
