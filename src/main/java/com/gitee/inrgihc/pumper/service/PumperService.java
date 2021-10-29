package com.gitee.inrgihc.pumper.service;

import com.gitee.inrgihc.pumper.config.PumperProperties;
import com.gitee.inrgihc.pumper.config.PumperProperties.HadoopHdfsProperties;
import com.gitee.inrgihc.pumper.config.PumperProperties.KerberosAuthentication;
import com.gitee.inrgihc.pumper.constant.PumperConstants;
import com.gitee.inrgihc.pumper.model.ColumnDescription;
import com.gitee.inrgihc.pumper.type.DatabaseTypeEnum;
import com.gitee.inrgihc.pumper.util.DataSourceUtils;
import com.gitee.inrgihc.pumper.util.HdfsUtils;
import com.gitee.inrgihc.pumper.util.HdfsUtils.ConfigurationWrapper;
import com.gitee.inrgihc.pumper.util.HdfsUtils.FileSystemWrapper;
import com.gitee.inrgihc.pumper.util.HiveUtils;
import com.gitee.inrgihc.pumper.util.JdbcUtils;
import com.gitee.inrgihc.pumper.util.SerializeUtils;
import com.gitee.inrgihc.pumper.util.TypeUtils;
import com.gitee.inrgihc.pumper.util.UUIDUtils;
import com.zaxxer.hikari.HikariDataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.mapred.RecordWriter;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PumperService implements ApplicationRunner {

  @Data
  private static class StatementResultSet implements AutoCloseable {

    private boolean isAutoCommit;
    private Connection connection;
    private Statement statement;
    private ResultSet resultset;

    @Override
    public void close() {
      try {
        connection.setAutoCommit(isAutoCommit);
      } catch (SQLException e) {
        log.warn("Jdbc Connect setAutoCommit() failed, error: {}", e.getMessage());
      }

      JdbcUtils.closeResultSet(resultset);
      JdbcUtils.closeStatement(statement);
      JdbcUtils.closeConnection(connection);
    }
  }


  private PumperProperties pumperProperties;

  public PumperService(PumperProperties pumperProperties) {
    this.pumperProperties = Objects.requireNonNull(pumperProperties);
  }

  @SneakyThrows
  private List<String> getSourceDatabaseTableLists(HikariDataSource ds, String schemaName) {
    try (Connection connection = ds.getConnection()) {
      return new ArrayList<>(JdbcUtils.getTableList(connection, schemaName));
    }
  }

  @SneakyThrows
  private List<ColumnDescription> getSourceTableColumnLists(HikariDataSource ds, String schemaName,
      String tableName) {
    return new ArrayList<>(DataSourceUtils.getColumnList(ds, schemaName, tableName));
  }

  @SneakyThrows
  private void runTargetExecuteSql(HikariDataSource ds, String sql) {
    try (Connection connection = ds.getConnection()) {
      HiveUtils.executeHiveql(connection, sql);
    }
  }

  @SneakyThrows
  private String getHdfsTablePathLocation(HikariDataSource ds, String schemaName,
      String tableName) {
    String hdfsUrl = pumperProperties.getTarget().getHadoopHdfs().getHdfsUrl();
    try (Connection connection = ds.getConnection()) {
      return HiveUtils.getTableLocationOnHdfs(connection, schemaName, tableName, hdfsUrl);
    }
  }

  @SneakyThrows
  private static StatementResultSet selectTableAllData(HikariDataSource dataSource,
      String schemaName, String tableName, Integer fetchSize) {
    DatabaseTypeEnum databaseType = DataSourceUtils.getDatabaseTypeByDatasource(dataSource);
    Integer localFetchSize = fetchSize;
    if (databaseType.isMySQlProduct()) {
      localFetchSize = Integer.MIN_VALUE;
      log.info("Source datasource is mysql database, and set fetch size to Integer.MIN_VALUE ");
    }

    String sql = String.format("SELECT * FROM %s",
        databaseType.getFullTableName(schemaName, tableName));
    log.info("Query SQL:{}", sql);

    StatementResultSet srs = new StatementResultSet();
    srs.setConnection(dataSource.getConnection());
    srs.setAutoCommit(srs.getConnection().getAutoCommit());
    srs.getConnection().setAutoCommit(false);
    srs.setStatement(srs.getConnection().createStatement(
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)
    );
    srs.getStatement().setQueryTimeout(PumperConstants.DEFAULT_QUERY_TIMEOUT_SECONDS);
    srs.getStatement().setFetchSize(localFetchSize);
    srs.setResultset(srs.getStatement().executeQuery(sql));

    return srs;
  }

  private String buildFilePath(String directoryPrefix) {
    if (directoryPrefix.endsWith(File.separator)) {
      return directoryPrefix + UUIDUtils.generateUUID() + ".textfile";
    } else {
      return directoryPrefix + File.separator + UUIDUtils.generateUUID() + ".textfile";
    }
  }

  @SneakyThrows
  private long transformDataStream(StatementResultSet srs, String tableName, int fetchSize,
      Consumer<List<Object[]>> handler) {
    long total = 0;
    List<Object[]> cache = new LinkedList<>();
    try (ResultSet rs = srs.getResultset();) {
      int columnCount = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        Object[] record = new Object[columnCount];
        for (int i = 1; i <= columnCount; ++i) {
          try {
            record[i - 1] = rs.getObject(i);
          } catch (Exception e) {
            log.warn("Read data with ResultSet.getObject() from table [ {} ] error: ",
                tableName, e);
            record[i - 1] = null;
          }
        }

        total++;
        cache.add(record);
        if (cache.size() >= fetchSize) {
          handler.accept(cache);
          cache.clear();
        }

      }

      if (!cache.isEmpty()) {
        handler.accept(cache);
        cache.clear();
      }

    } finally {
      srs.close();
    }

    return total;
  }

  @SneakyThrows
  private void transportTable(HikariDataSource dsSource, HikariDataSource dsTarget,
      String schemaName, String tableName, ConfigurationWrapper conf, FileSystemWrapper hdfs) {
    log.info("Transport table for table : {}", tableName);
    String targetSchemaName = pumperProperties.getTarget().getSchemaName();
    String splitChar = PumperConstants.HIVE_FIELD_SPLIT_CHAR;

    // 1.读取源端库的表的字段元信息
    List<ColumnDescription> sourceColumns = getSourceTableColumnLists(dsSource, schemaName,
        tableName);
    if (CollectionUtils.isEmpty(sourceColumns)) {
      log.warn("Table not exist , schema-name: {}, table-name:{}", schemaName, tableName);
      return;
    }

    // 2.将源端库的表字段类型转换为目标端(Hive)的字段类型
    List<ColumnDescription> targetColumns = TypeUtils.toHiveType(sourceColumns);

    // 3.在目标端(Hive)对应建表
    runTargetExecuteSql(
        dsTarget,
        HiveUtils.genCreateTableSql(targetSchemaName, tableName, targetColumns, splitChar)
    );
    // 写数据前线清空已有数据
    runTargetExecuteSql(
        dsTarget,
        HiveUtils.genTruncateTableSql(targetSchemaName, tableName)
    );

    // 4.获取目标端(Hive)表所在HDFS上的路径
    String tableLocationDirectory = getHdfsTablePathLocation(dsTarget, targetSchemaName, tableName);

    // 5.将源端表的数据导出为本地的文本文件
    Integer fetchSize = pumperProperties.getSource().getFetchSize();
    StatementResultSet srs = selectTableAllData(dsSource, schemaName, tableName, fetchSize);

    // 6.读取源端表的数据内容，并经文本序列化后写入到远程的HDFS文件系统中
    String tableHdfsPathFileName = buildFilePath(tableLocationDirectory);
    log.info("Table data location: {}", tableHdfsPathFileName);

    RecordWriter recordWriter = HdfsUtils.getHdfsRecordWriter(conf, hdfs, tableHdfsPathFileName);

    Consumer<List<Object[]>> handler = (cache) -> {
      for (Object[] row : cache) {
        String content = SerializeUtils.serialize(row, splitChar);
        HdfsUtils.writeRecord(recordWriter, content);
      }
      log.info("Transform table [{}] data batch size is {}", tableName, cache.size());
    };

    try {
      long total = this.transformDataStream(srs, tableName, fetchSize, handler);
      log.info("Transform table {} total record size is {}", tableName, total);
    } finally {
      HdfsUtils.closeHdfsRecordWriter(recordWriter);
    }

  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    HikariDataSource dsSource = DataSourceUtils
        .createSourceDataSource(pumperProperties.getSource());
    HikariDataSource dsTarget = DataSourceUtils
        .createTargetDataSource(pumperProperties.getTarget());

    try {
      String sourceSchemaName = pumperProperties.getSource().getSchemaName();
      List<String> includeTables = pumperProperties.getSource().getTableIncludes();
      List<String> sourceTableLists = (CollectionUtils.isNotEmpty(includeTables)) ?
          pumperProperties.getSource().getTableIncludes()
          : getSourceDatabaseTableLists(dsSource, sourceSchemaName);

      HadoopHdfsProperties hdfsConfig = pumperProperties.getTarget().getHadoopHdfs();
      KerberosAuthentication kerberosAuthConfig = pumperProperties.getTarget().getKerberos();

      if (Objects.nonNull(kerberosAuthConfig) && kerberosAuthConfig.getEnableKerberos()) {
        HdfsUtils.loginByConfiguration(hdfsConfig, kerberosAuthConfig);
      } else {
        HdfsUtils.loginByConfiguration(hdfsConfig);
      }

      ConfigurationWrapper conf = HdfsUtils.getConfigurationWrapper();
      FileSystemWrapper hdfs = HdfsUtils.getFileSystem(conf);

      if (CollectionUtils.isNotEmpty(sourceTableLists)) {
        for (String tableName : sourceTableLists) {
          List<String> excludeTables = pumperProperties.getSource().getTableExcludes();
          if (CollectionUtils.isNotEmpty(excludeTables)) {
            if (!excludeTables.contains(tableName)) {
              transportTable(dsSource, dsTarget, sourceSchemaName, tableName, conf, hdfs);
            }
          } else {
            transportTable(dsSource, dsTarget, sourceSchemaName, tableName, conf, hdfs);
          }
        }
      }

    } finally {
      DataSourceUtils.closeHikariDataSource(dsSource);
      DataSourceUtils.closeHikariDataSource(dsTarget);
    }

  }

}
