package com.gitee.inrgihc.pumper.util;

import java.util.UUID;

public class UUIDUtils {

  public static String generateUUID() {
    return UUID.randomUUID().toString().replace("-", "");
  }

}
