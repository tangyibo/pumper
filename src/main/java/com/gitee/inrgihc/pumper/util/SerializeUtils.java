package com.gitee.inrgihc.pumper.util;

import cn.hutool.core.codec.Base64;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Objects;
import lombok.SneakyThrows;

public final class SerializeUtils {

  @SneakyThrows
  public static String serialize(Object[] row, String split) {
    StringBuilder sb = new StringBuilder();
    for (Object value : row) {
      if (Objects.isNull(value)) {
        sb.append(split);
      } else if (value instanceof Boolean) {
        sb.append((Boolean) value ? "1" : "0" + split);
      } else if (value instanceof Timestamp) {
        sb.append(DateUtils.formatDateTime((Timestamp) value) + split);
      } else if (value instanceof java.sql.Date) {
        sb.append(DateUtils.formatDate((java.sql.Date) value) + split);
      } else if (value instanceof java.sql.Clob) {
        sb.append(clob2Str((java.sql.Clob) value) + split);
      } else if (value instanceof java.sql.Blob) {
        java.sql.Blob blob = (java.sql.Blob) value;
        sb.append(Base64.encode(toByteArray(blob.getBinaryStream())) + split);
      } else if (value instanceof byte[]) {
        sb.append(Base64.encode((byte[]) value) + split);
      } else {
        sb.append(value.toString() + split);
      }
    }
    return sb.toString();
  }

  @SneakyThrows
  private static String clob2Str(java.sql.Clob input) {
    java.io.Reader is = null;
    java.io.BufferedReader reader = null;
    try {
      is = input.getCharacterStream();
      reader = new java.io.BufferedReader(is);
      String line = reader.readLine();
      StringBuilder sb = new StringBuilder();
      while (line != null) {
        sb.append(line);
        line = reader.readLine();
      }
      return sb.toString();
    } finally {
      try {
        if (null != reader) {
          reader.close();
        }
        if (null != is) {
          is.close();
        }
      } catch (Exception ex) {

      }
    }
  }

  @SneakyThrows
  public static byte[] toByteArray(InputStream input) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int n = 0;
    while (-1 != (n = input.read(buffer))) {
      output.write(buffer, 0, n);
    }
    return output.toByteArray();
  }

  private SerializeUtils() {
  }
}
