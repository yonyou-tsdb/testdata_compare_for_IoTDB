#!/bin/sh

echo ---------------------
echo Starting Export
echo ---------------------

if [ -z "${COMPARE_TOOL_HOME}" ]; then
  export COMPARE_TOOL_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

CLASSPATH=""
for f in ${COMPARE_TOOL_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

MAIN_CLASS=com.yonyou.iotdb.test.SnapshootIoTDBSummary

"$JAVA" -cp "$CLASSPATH" "$MAIN_CLASS" "$@"
exit $?
