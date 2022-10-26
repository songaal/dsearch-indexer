FROM dcr.danawa.io/alpine-k8s-java:8

ENV JAVA_OPTS=""
ARG VERSION

RUN yum -y update && yum install -y wget rsync

RUN useradd danawa
RUN usermod -aG wheel danawa

# 로그 폴더 생성
WORKDIR /data
WORKDIR /data/indexerLog

RUN chmod 777 /data
RUN chmod 777 /data/indexerLog
RUN chown danawa /data
RUN chown danawa /data/indexerLog
RUN chgrp users /data
RUN chgrp users /data/indexerLog

USER danawa

WORKDIR /app

ENV PATH=$PATH:${JAVA_HOME}/bin
ENV spring_logging_level=debug
ENV LANG=ko_KR.euckr
ENV VERSION=1.1.0

COPY lib/Altibase.jar .

EXPOSE 9350
EXPOSE 8080
EXPOSE 9100

COPY branch-desc.txt/ .
COPY target/indexer-${VERSION}.jar indexer.jar

CMD /bin/bash -c "java ${JAVA_OPTS} -Dlogback.configurationFile=logback-prod.xml -Dfile.encoding=utf-8 -classpath indexer.jar:lib/Altibase.jar org.springframework.boot.loader.JarLauncher"
