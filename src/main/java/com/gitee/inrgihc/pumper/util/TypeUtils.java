package com.gitee.inrgihc.pumper.util;

import com.gitee.inrgihc.pumper.model.ColumnDescription;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public final class TypeUtils {

  /**
   * HIVE的数据类型参考：https://blog.csdn.net/weixin_42702831/article/details/82559372
   */
  public static class HiveDataType {

    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INTEGER = "INTEGER";
    public static final String BIGINT = "BIGINT";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String STRING = "STRING";
    public static final String VARCHAR = "VARCHAR";
    public static final String DATE = "DATE";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String BINARY = "BINARY";
  }

  public static ColumnDescription toHiveType(ColumnDescription cd) {
    int dataType = cd.getDateType();
    switch (dataType) {
      case Types.TINYINT:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.TINYINT, Types.TINYINT);
      case Types.SMALLINT:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.SMALLINT, Types.SMALLINT);
      case Types.INTEGER:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.INTEGER, Types.INTEGER);
      case Types.BIGINT:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.BIGINT, Types.BIGINT);
      case Types.FLOAT:
      case Types.REAL:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.FLOAT, Types.FLOAT);
      case Types.DOUBLE:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.DOUBLE, Types.DOUBLE);
      case Types.DECIMAL:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.DECIMAL, Types.DECIMAL);
      case Types.BOOLEAN:
      case Types.BIT:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.TINYINT, Types.INTEGER);
      case Types.CHAR:
      case Types.NCHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
      case Types.NCLOB:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.STRING, Types.LONGVARCHAR);
      case Types.DATE:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.DATE, Types.DATE);
      case Types.TIME:
      case Types.TIMESTAMP:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.TIMESTAMP, Types.TIMESTAMP);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.BLOB:
      case Types.LONGVARBINARY:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.BINARY, Types.BINARY);
      default:
        return new ColumnDescription(cd.getFieldName(), HiveDataType.STRING, Types.LONGVARCHAR);
    }
  }

  public static List<ColumnDescription> toHiveType(List<ColumnDescription> descriptionList) {
    List<ColumnDescription> ret = new ArrayList<>();
    for (ColumnDescription cd : descriptionList) {
      ret.add(toHiveType(cd));
    }
    return ret;
  }

}
