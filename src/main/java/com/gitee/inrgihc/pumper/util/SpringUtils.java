package com.gitee.inrgihc.pumper.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public final class SpringUtils implements ApplicationContextAware {

  private static ApplicationContext applicationContext;

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    SpringUtils.applicationContext = applicationContext;
  }

  public static ApplicationContext getApplicationContext() {
    return SpringUtils.applicationContext;
  }

  public static Object getBean(String name) {
    return SpringUtils.applicationContext.getBean(name);
  }

  public static <T> T getBean(Class<T> clazz) {
    return SpringUtils.applicationContext.getBean(clazz);
  }

  public static <T> T getBean(Class<T> clazz, Object... objects) {
    return SpringUtils.applicationContext.getBean(clazz, objects);
  }

  public static <T> T getBean(String name, Class<T> clazz) {
    return SpringUtils.applicationContext.getBean(name, clazz);
  }

  private SpringUtils() {
  }
}
