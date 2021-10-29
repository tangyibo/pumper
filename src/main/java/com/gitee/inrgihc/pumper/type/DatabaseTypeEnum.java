package com.gitee.inrgihc.pumper.type;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;

@Getter
public enum DatabaseTypeEnum {
  UNKNOWN("unknown", "\""),
  MYSQL("MySQL", "`"),
  MARIADB("MariaDB", "`"),
  ORACLE("Oracle", "\""),
  POSTGRESQL("PostgreSQL", "\""),
  SQLSERVER("SQLServer", "\""),
  DB2("DB2", "\""),
  DM("DM", "\""),
  KINGBASE("KingbaseES", "\""),
  ;

  private String productName;
  private String delimiter;

  DatabaseTypeEnum(String productName, String delimiter) {
    this.productName = productName;
    this.delimiter = delimiter;
  }

  public boolean isMySQlProduct() {
    return DatabaseTypeEnum.MYSQL == this || DatabaseTypeEnum.MARIADB == this;
  }

  public static DatabaseTypeEnum fromProductName(String source) {
    if (StringUtils.isNotBlank(source)) {
      if (source.startsWith("DB2")) {
        return DB2;
      } else if ("MySQL".equals(source)) {
        return MYSQL;
      } else if ("MariaDB".equals(source)) {
        return MARIADB;
      } else if ("Oracle".equals(source)) {
        return ORACLE;
      } else if ("PostgreSQL".equals(source)) {
        return POSTGRESQL;
      } else if ("Sybase SQL Server".equals(source)
          || "Adaptive Server Enterprise".equals(source)
          || "ASE".equals(source)
          || "sql server".equalsIgnoreCase(source)
          || "Microsoft SQL Server".equalsIgnoreCase(source)) {
        return SQLSERVER;
      } else if (source.equalsIgnoreCase("KingbaseES")) {
        return KINGBASE;
      } else if (source.equalsIgnoreCase("DM DBMS")) {
        return DM;
      }

    }
    return UNKNOWN;
  }

  public String getFullTableName(String schemaName, String tableName) {
    return String.format("%s%s%s.%s%s%s",
        delimiter, schemaName, delimiter,
        delimiter, tableName, delimiter);
  }

}
