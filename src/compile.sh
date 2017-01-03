#!/bin/bash -
pwd=$(cd $(dirname $0);pwd)
HMAPATH="${pwd}/../../"
CLASSPATH="${HMAPATH}/hma/src"

# add libs to CLASSPATH 
for f in ${HMAPATH}/hma/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in ${HMAPATH}/hma/lib/*/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in ${HMAPATH}/hma/lib/*/*/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

${HMAPATH}/java8/bin/javac */*.java -cp ${CLASSPATH}
${HMAPATH}/java8/bin/javac */*/*.java -cp ${CLASSPATH}
${HMAPATH}/java8/bin/javac */*/*/*.java -cp ${CLASSPATH}
${HMAPATH}/java8/bin/javac */*/*/*/*.java -cp ${CLASSPATH}
${HMAPATH}/java8/bin/javac */*/*/*/*/*.java -cp ${CLASSPATH}

${HMAPATH}/java8/bin/jar cf hma.jar hma/
#${HMAPATH}/java8/bin/jar cf json.jar org/

#cp hma.jar json.jar ../lib.http/
mv hma.jar  ../lib/
