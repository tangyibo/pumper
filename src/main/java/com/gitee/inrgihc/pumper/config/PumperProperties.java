package com.gitee.inrgihc.pumper.config;

import java.util.Collections;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "pumper")
public class PumperProperties {

  private SourceDataSource source;
  private TargetDataSource target;

  @Data
  public static class AccountAuthentication {

    private Boolean enableAccount = false;
    private String username;
    private String password;
  }

  @Data
  public static class KerberosAuthentication {

    private Boolean enableKerberos = false;
    private String kerberosPrincipal;
    private String kerberosKeytabFilePath;
    private String kerberosConfFilePath;
  }

  @Data
  public static class HadoopHdfsProperties {

    private String hdfsUrl;
    private String hdfsSiteXmlFilePath;
    private String coreSiteXmlFilePath;
  }

  @Data
  public static class SourceDataSource {

    private String jdbcUrl;
    private String driverClassName;
    private String schemaName;
    private AccountAuthentication account;

    private Integer fetchSize = 10000;
    private List<String> tableIncludes = Collections.emptyList();
    private List<String> tableExcludes = Collections.emptyList();
  }

  @Data
  public static class TargetDataSource {

    private String jdbcUrl;
    private String driverClassName;
    private String schemaName;
    private HadoopHdfsProperties hadoopHdfs;
    private AccountAuthentication account;
    private KerberosAuthentication kerberos;
  }

}
