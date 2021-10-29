package com.gitee.inrgihc.pumper.util;

import com.gitee.inrgihc.pumper.config.PumperProperties.HadoopHdfsProperties;
import com.gitee.inrgihc.pumper.config.PumperProperties.KerberosAuthentication;
import java.text.SimpleDateFormat;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

@Slf4j
public final class HdfsUtils {

  private static UserGroupInformation loginUser = null;
  private static ConfigurationWrapper configurationWrapper = null;

  @Data
  @AllArgsConstructor
  public static class ConfigurationWrapper {

    private Configuration configuration;
    private JobConf jobConf;
  }

  @Data
  @AllArgsConstructor
  public static class FileSystemWrapper {

    private FileSystem fileSystem;
  }

  public static void loginByConfiguration(HadoopHdfsProperties hdfsConfig) {
    loginByConfiguration(hdfsConfig, null);
  }

  @SneakyThrows
  public static void loginByConfiguration(HadoopHdfsProperties hdfsConfig,
      KerberosAuthentication kerberosAuthConfig) {
    String hdfsSiteXmlFilePath = hdfsConfig.getHdfsSiteXmlFilePath();
    String coreSiteXmlFilePath = hdfsConfig.getCoreSiteXmlFilePath();

    boolean haveKerberos = Objects.nonNull(kerberosAuthConfig) ?
        kerberosAuthConfig.getEnableKerberos() : false;

    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", hdfsConfig.getHdfsUrl());

    if (StringUtils.isNotEmpty(hdfsSiteXmlFilePath)) {
      configuration.addResource(new Path(hdfsSiteXmlFilePath));
    }

    if (StringUtils.isNotEmpty(coreSiteXmlFilePath)) {
      configuration.addResource(new Path(coreSiteXmlFilePath));
    }

    if (haveKerberos) {
      if (null == HdfsUtils.loginUser) {
        log.info("No login user. Creating login user");
        String kerberosPrincipal = kerberosAuthConfig.getKerberosPrincipal();
        String kerberosKeytabFilePath = kerberosAuthConfig.getKerberosKeytabFilePath();
        String kerberosConfFilePath = kerberosAuthConfig.getKerberosConfFilePath();

        configuration.set("hadoop.security.authentication", "kerberos");
        //System.setProperty("sun.security.krb5.debug", "true");
        if (StringUtils.isNotEmpty(kerberosConfFilePath)) {
          System.setProperty("java.security.krb5.conf", kerberosConfFilePath);
        }

        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
        HdfsUtils.loginUser = UserGroupInformation.getLoginUser();
        log.info("Logged in with user " + loginUser);

        log.info("HDFS current user: " + UserGroupInformation.getCurrentUser());
        log.info("HDFS login user: " + UserGroupInformation.getLoginUser());
      } else {
        log.info("loginUser (" + loginUser + ") already created, refreshing tgt.");
        HdfsUtils.loginUser.checkTGTAndReloginFromKeytab();
      }
    }

    if (null == HdfsUtils.configurationWrapper) {
      HdfsUtils.configurationWrapper = new ConfigurationWrapper(
          configuration,
          new JobConf(configuration)
      );
    }
  }

  public static ConfigurationWrapper getConfigurationWrapper() {
    if (null == HdfsUtils.configurationWrapper) {
      throw new RuntimeException("Please login first!");
    }

    return HdfsUtils.configurationWrapper;
  }

  @SneakyThrows
  public static FileSystemWrapper getFileSystem(ConfigurationWrapper configuration) {
    return new FileSystemWrapper(FileSystem.get(configuration.getConfiguration()));
  }

  @SneakyThrows
  public static FileOutputFormat getFileOutputFormat(ConfigurationWrapper conf, String fileName) {
    Path outputPath = new Path(fileName);
    FileOutputFormat.setOutputPath(conf.getJobConf(), outputPath);
    FileOutputFormat.setWorkOutputPath(conf.getJobConf(), outputPath);
    return new TextOutputFormat();
  }

  @SneakyThrows
  public static RecordWriter getHdfsRecordWriter(ConfigurationWrapper conf, FileSystemWrapper fs,
      String hdfsFullFilePathName) {
    JobConf jc = conf.getJobConf();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
    String attempt = "attempt_" + dateFormat.format(new java.util.Date()) + "_0001_m_000000_0";
    jc.set(JobContext.TASK_ATTEMPT_ID, attempt);
    FileOutputFormat outFormat = getFileOutputFormat(conf, hdfsFullFilePathName);
    return outFormat.getRecordWriter(fs.getFileSystem(),
        jc,
        FileOutputFormat.getOutputPath(jc).toString(),
        Reporter.NULL
    );
  }

  @SneakyThrows
  public static void writeRecord(RecordWriter writer, String content) {
    writer.write(NullWritable.get(), content);
  }

  @SneakyThrows
  public static void closeHdfsRecordWriter(RecordWriter writer) {
    writer.close(Reporter.NULL);
  }

  @SneakyThrows
  public String[] hdfsDirList(FileSystemWrapper fileSystem, String dir) {
    Path path = new Path(dir);
    FileStatus[] status = fileSystem.getFileSystem().listStatus(path);
    String[] files = new String[status.length];
    for (int i = 0; i < status.length; i++) {
      files[i] = status[i].getPath().toString();
    }

    return files;
  }

  @SneakyThrows
  public boolean isPathExists(FileSystemWrapper fileSystem, String filePath) {
    return fileSystem.getFileSystem().exists(new Path(filePath));
  }

  @SneakyThrows
  public boolean isPathDir(FileSystemWrapper fileSystem, String filePath) {
    return fileSystem.getFileSystem().isDirectory(new Path(filePath));
  }

  @SneakyThrows
  public void deleteFiles(FileSystemWrapper fileSystem, Path[] paths) {
    for (int i = 0; i < paths.length; i++) {
      log.info(String.format("delete file [%s].", paths[i].toString()));
      fileSystem.getFileSystem().delete(paths[i], true);
    }
  }

  @SneakyThrows
  public void deleteDir(FileSystemWrapper fileSystem, Path path) {
    log.info(String.format("start delete tmp dir [%s] .", path.toString()));
    if (isPathExists(fileSystem, path.toString())) {
      fileSystem.getFileSystem().delete(path, true);
    }
  }

  @SneakyThrows
  public void closeFileSystem(FileSystemWrapper fileSystem) {
    fileSystem.getFileSystem().close();
  }

  @SneakyThrows
  public static void uploadFile(ConfigurationWrapper configuration, String localFilepath,
      String remoteHdfsPath) {
    try (FileSystem fs = FileSystem.get(configuration.getConfiguration());) {
      fs.copyFromLocalFile(new Path(localFilepath), new Path(remoteHdfsPath));
    }
  }

  private HdfsUtils() {
  }

  public static void main(String[] args) throws Exception {
    String hdfsUri = "hdfs://tr01:8020";
    String user = "hdfs/tr01@TDH";
    String keytab = "D:/hdfs.keytab";
    System.setProperty("java.security.krb5.conf", "D:/krb5.conf");

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", hdfsUri);
    conf.addResource(new Path("D:/hdfs-site.xml"));
    conf.addResource(new Path("D:/core-site.xml"));
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(user, keytab);

    try (FileSystem fs = FileSystem.get(conf);) {
      RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), false);
      while (iter.hasNext()) {
        LocatedFileStatus next = iter.next();
        System.out.println(next.getPath().getName());
      }
    }
  }

}
