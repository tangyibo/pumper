package com.gitee.inrgihc.pumper.util;

import com.gitee.inrgihc.pumper.model.ColumnDescription;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;

public final class JdbcUtils {

  public static Set<String> getSchemaList(Connection connection) throws SQLException {
    Set<String> schemas = new HashSet<>();
    ResultSet rs = connection.getMetaData().getSchemas();
    while (rs.next()) {
      schemas.add(rs.getString("TABLE_SCHEM"));
    }

    return schemas;
  }

  public static Set<String> getTableList(Connection connection, String schemaName)
      throws SQLException {
    Set<String> tables = new HashSet<>();
    ResultSet rs = connection.getMetaData().getTables(null, schemaName, "%", new String[]{"TABLE"});
    while (rs.next()) {
      tables.add(rs.getString("TABLE_NAME"));
    }

    return tables;
  }

  public static List<ColumnDescription> getColumnList(Connection connection, String schemaName,
      String tableName)
      throws SQLException {
    List<ColumnDescription> columns = new ArrayList<>();
    Set<String> unique = new HashSet<>();
    ResultSet rs = connection.getMetaData().getColumns(null, schemaName, tableName, null);
    while (rs.next()) {
      String fieldName = rs.getString("COLUMN_NAME");
      String fieldType = rs.getString("TYPE_NAME");
      int dataType = rs.getInt("DATA_TYPE");
      if (!unique.contains(fieldName)) {
        columns.add(new ColumnDescription(fieldName, fieldType, dataType));
        unique.add(fieldName);
      }
    }

    return columns;
  }

  public static void closeConnection(Connection con) {
    if (con != null) {
      try {
        con.close();
      } catch (Throwable ex) {
      }
    }
  }

  public static void closeStatement(Statement stmt) {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (Throwable ex) {
      }
    }
  }

  public static void closeResultSet(ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (Throwable ex) {
      }
    }
  }

  private JdbcUtils() {
  }

}
