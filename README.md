# Pumper2Hive数据抽取工具

**项目地址：**

- Github: https://github.com/tangyibo/pumper

- Gitee: https://gitee.com/inrgihc/pumper

## 一、功能简述

> 简言之，将常见MySQL、PostgreSQL、Oracle、SQLServer等**可用JDBC访问的关系型数据库**的表结构和数据抽取到目标端为**Hive数据仓库**中，支持**表结构的转换**和**数据的加载**。

## 二、详细功能

- 支持常见关系型数据库到hive库表结构的转换；

- 支持千万级甚至亿级的数据量向hive的迁移；

## 三、实现原理

> 通过读取源端(关系型数据)库的表和字段元信息，根据字段类型映射关系将源端库的字段类型转换为hive对应的字段类型，并构建可以在hive中执行的```create table```SQL语句实现建表操作；

> 然后基于分批次读取源端数据库内的数据，经序列化后写入Hive表对应在HDFS上目录下的文件中，完成数据抽取功能。

## 四、编译打包

- 编译环境

  **JDK**:>=1.8

  **maven**:>=3.6
  
- 编译命令:

```shell
git clone https://gitee.com/inrgihc/pumper.git
cd pumper/
mvn clean package
```

即可在pumper/target目录下生成```pumper-release-<version>.zip```的部署包文件，将该zip包拷贝到装有java环境的机器上解压即可完成部署:

```shell
[root@localhost ~]# unzip pumper-release-1.0.0.zip
[root@localhost ~]# tree pumper-release-0.0.1
pumper-release-0.0.1
├── bin
│   ├── startup.cmd
│   └── startup.sh
├── conf
│   └── application.yml
└── lib
    ├── pumper-0.0.1.jar
    ├── (省略)
    └── zookeeper-3.4.6.jar
```

## 三、配置部署

- 参数配置

配置见```pumper-release-<version>/conf/application.yml```文件内容，参数说明如下：

```shell
pumper:
  # 源端：支持常见的关系数据库，如Oracle/SQLServer/MySQL/PostgreSQL等JDBC访问访问的关系型数据库
  source:
    # 源端的JDBC数据库地址
    jdbc-url: jdbc:mysql://192.168.2.10:3306/test?useSSL=false&nullCatalogMeansCurrent=true
    # 源端的数据库驱动类
    driver-class-name: com.mysql.jdbc.Driver
    # 账号信息
    account:
      # 是否启用账号认证
      enable-account: true
      # 账号名(当enable-account为true时使用)
      username: tangyibo
      # 账号密码(当enable-account为true时使用)
      password: 123456
    # 批次大小
    fetch-size : 10000
    # 源端数据库的Schema名
    schema-name: 'test'
    # 包含的物理表或视图表，为空时，自动提取schema下所有的表
    table-includes:
      - t_test001
      - t_test002
    # 排除的表，可为空
    table-excludes:
  # HIVE库（必须)
  target:
    # Hive库的JDBC地址
    jdbc-url: jdbc:hive2://192.168.1.102:10000/default
    # Hive库的JDBC驱动类
    driver-class-name: org.apache.hive.jdbc.HiveDriver
    # 账号认证信息
    account:
      # 是否启用账号认证
      enable-account: true
      # 账号名(当enable-account为true时使用)
      username: hive
      # 账号密码(当enable-account为true时使用)
      password: 123456
    # Hive库的库名(schema名)
    schema-name: 'demo'
    # HDFS的配置信息
    hadoop-hdfs:
      # HDFS地址
      hdfs-url: hdfs://192.168.1.101:8020
      # HDFS账号(根据实际进行配置，通常为hadoop或hdfs或hive等)
      username: 'hadoop'
      # hdfs-site.xml文件在本地文件系统中的绝对路径,该文件需要到hadoop集群服务器上拷贝到本地
      hdfs-site-xml-file-path: "D:/cdh/hdfs-site.xml"
      # core-site.xml文件在本地文件系统中的路径,该文件需要到hadoop集群服务器上拷贝到本地
      core-site-xml-file-path: "D:/cdh/core-site.xml"
    kerberos:
      # 是否启用kerberos认证
      enable-kerberos: true
      # kerberos的principal(当enable-kerberos为true时使用)
      kerberos-principal : "hdfs/node01@CDH"
      # kerberos的keytab文件在本地文件系统中的绝对路径(当enable-kerberos为true时使用),该文件需要到hadoop集群服务器上拷贝到本
      kerberos-keytab-file-path : "D:/cdh/hdfs.keytab"
      # keytab的krb5.conf文件在本地文件系统中的绝对路径(当enable-kerberos为true时使用),该文件需要到hadoop集群服务器上拷贝到本
      kerberos-conf-file-path : "D:/cdh/krb5.conf"
```

注意：如果在hdfs-site.xml/core-site.xml/krb5.conf等中使用了主机名进行配置，需要**配置hosts的主机名与IP的映射关系**。

- 程序启动

**(1) linux系统**

在linux系统下执行：

```
sh bin/startup.sh
```

**(2) Windows系统**

在Window系统下双击bin目录下```startup.cmd```文件启动。



