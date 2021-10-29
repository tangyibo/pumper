package com.gitee.inrgihc.pumper.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;

public final class DateUtils {

  private static final String DATE_FORMAT = "yyyy-MM-dd";
  private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private static final ThreadLocal<Map<String, DateFormat>> dateFormatThreadLocal = new ThreadLocal<Map<String, DateFormat>>();

  private static DateFormat getDateFormat(String pattern) {
    if (pattern == null || pattern.trim().length() == 0) {
      throw new IllegalArgumentException("pattern cannot be empty.");
    }

    Map<String, DateFormat> dateFormatMap = dateFormatThreadLocal.get();
    if (dateFormatMap != null && dateFormatMap.containsKey(pattern)) {
      return dateFormatMap.get(pattern);
    }

    synchronized (dateFormatThreadLocal) {
      if (dateFormatMap == null) {
        dateFormatMap = new HashMap<>();
      }
      dateFormatMap.put(pattern, new SimpleDateFormat(pattern));
      dateFormatThreadLocal.set(dateFormatMap);
    }

    return dateFormatMap.get(pattern);
  }

  public static String formatDate(Date date) {
    return format(date, DATE_FORMAT);
  }

  public static String formatDateTime(Date date) {
    return format(date, DATETIME_FORMAT);
  }

  public static String format(Date date, String patten) {
    return getDateFormat(patten).format(date);
  }

  public static Date parseDate(String dateString) {
    return parse(dateString, DATE_FORMAT);
  }

  public static Date parseDateTime(String dateString) {
    return parse(dateString, DATETIME_FORMAT);
  }

  @SneakyThrows
  public static Date parse(String dateString, String pattern) {
    return getDateFormat(pattern).parse(dateString);
  }

}
