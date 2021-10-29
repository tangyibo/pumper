#!/usr/bin/env bash
#
#
#############################################
# !!!!!! Modify here please

APP_MAIN_CLASS="com.gitee.inrgihc.pumper.PumperApplication"

#############################################

APP_HOME="${BASH_SOURCE-$0}"
APP_HOME="$(dirname "${APP_HOME}")"
APP_HOME="$(cd "${APP_HOME}"; pwd)"
APP_HOME="$(cd "$(dirname ${APP_HOME})"; pwd)"
#echo "Base Directory:${APP_HOME}"

APP_BIN_PATH=$APP_HOME/bin
APP_LIB_PATH=$APP_HOME/lib
APP_CONF_PATH=$APP_HOME/conf

# JVMFLAGS JVM参数可以在这里设置
JVMFLAGS="-Xms2g -Xmx2g -Xmn1g -XX:+DisableExplicitGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Dfile.encoding=UTF-8 -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=9889"

if [ "$JAVA_HOME" != "" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi

#把lib下的所有jar都加入到classpath中
CLASSPATH=$APP_CONF_PATH
for i in $APP_LIB_PATH/*.jar
do
	CLASSPATH="$i:$CLASSPATH"
done

$JAVA -cp $CLASSPATH $JVMFLAGS $APP_MAIN_CLASS $APP_CONF_PATH
