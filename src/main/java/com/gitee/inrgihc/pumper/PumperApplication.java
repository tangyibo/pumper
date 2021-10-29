package com.gitee.inrgihc.pumper;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class PumperApplication {

  public static void main(String[] args) {
    SpringApplication springApplication = new SpringApplication(PumperApplication.class);
    springApplication.setWebApplicationType(WebApplicationType.NONE);
    springApplication.setBannerMode(Banner.Mode.OFF);
    springApplication.run(args);
  }

}
