package com.gitee.inrgihc.pumper.util;

import com.gitee.inrgihc.pumper.config.PumperProperties;
import com.gitee.inrgihc.pumper.model.ColumnDescription;
import com.gitee.inrgihc.pumper.type.DatabaseTypeEnum;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DataSourceUtils {

  @SneakyThrows
  public static DatabaseTypeEnum getDatabaseTypeByDatasource(HikariDataSource dataSource) {
    try (Connection connection = dataSource.getConnection()) {
      String productName = connection.getMetaData().getDatabaseProductName();
      return DatabaseTypeEnum.fromProductName(productName);
    }
  }
  
  public static HikariDataSource createSourceDataSource(
      PumperProperties.SourceDataSource description) {
    HikariDataSource ds = new HikariDataSource();
    ds.setPoolName("The_Source_DB_Connection");
    ds.setJdbcUrl(description.getJdbcUrl());
    ds.setDriverClassName(description.getDriverClassName());
    if (Objects.nonNull(description.getAccount())
        && Objects.nonNull(description.getAccount().getEnableAccount())
        && description.getAccount().getEnableAccount()) {
      ds.setUsername(description.getAccount().getUsername());
      ds.setPassword(description.getAccount().getPassword());
    }

    ds.setMaximumPoolSize(8);
    ds.setMinimumIdle(5);
    ds.setConnectionTimeout(60000);
    ds.setIdleTimeout(60000);

    return ds;
  }

  public static HikariDataSource createTargetDataSource(
      PumperProperties.TargetDataSource description) {
    HikariDataSource ds = new HikariDataSource();
    ds.setPoolName("The_Target_DB_Connection");
    ds.setJdbcUrl(description.getJdbcUrl());
    ds.setDriverClassName(description.getDriverClassName());
    if (Objects.nonNull(description.getAccount())
        && Objects.nonNull(description.getAccount().getEnableAccount())
        && description.getAccount().getEnableAccount()) {
      ds.setUsername(description.getAccount().getUsername());
      ds.setPassword(description.getAccount().getPassword());
    }

    ds.setMaximumPoolSize(8);
    ds.setMinimumIdle(5);
    ds.setConnectionTimeout(30000);
    ds.setIdleTimeout(60000);

    return ds;
  }

  public static List<ColumnDescription> getColumnList(DataSource dataSource, String schemaName,
      String tableName) throws SQLException {
    List<ColumnDescription> columns = new ArrayList<>();
    Set<String> unique = new HashSet<>();

    try (Connection connection = dataSource.getConnection()) {
      String productName = connection.getMetaData().getDatabaseProductName();
      DatabaseTypeEnum databaseType = DatabaseTypeEnum.fromProductName(productName);
      String sql = String.format("SELECT * FROM %s%s%s.%s%s%s WHERE 1=2",
          databaseType.getDelimiter(), schemaName, databaseType.getDelimiter(),
          databaseType.getDelimiter(), tableName, databaseType.getDelimiter());
      try (PreparedStatement stmt = connection.prepareStatement(sql);
          ResultSet rs = stmt.executeQuery(sql);
      ) {
        ResultSetMetaData m = rs.getMetaData();
        int count = m.getColumnCount();
        for (int i = 1; i <= count; i++) {
          String fieldName = m.getColumnLabel(i);
          String fieldType = m.getColumnTypeName(i);
          int dataType = m.getColumnType(i);
          if (null == fieldName) {
            fieldName = m.getColumnName(i);
          }

          if (!unique.contains(fieldName)) {
            columns.add(new ColumnDescription(fieldName, fieldType, dataType));
            unique.add(fieldName);
          }
        }

      }

    }

    return columns;
  }

  public static void closeHikariDataSource(HikariDataSource dataSource) {
    try {
      dataSource.close();
    } catch (Exception e) {
      log.warn("Close data source error:", e);
    }
  }

  private DataSourceUtils() {
  }

}
