FROM dcr.danawa.io/alpine-k8s-java:8


RUN yum -y update && yum install -y wget rsync openssh-server openssh-clients openssh-askpass

ENV PATH=$PATH:${JAVA_HOME}/bin
ENV spring_logging_level=debug
ENV LANG=ko_KR.euckr

WORKDIR /app

COPY lib/Altibase.jar .
COPY lib/danawa-product-1.1.1.jar .


EXPOSE 9350
EXPOSE 8080
EXPOSE 9100

