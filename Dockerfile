FROM centos:7.6.1810


ENV DEV_ADOPTED_ENV="DEVELOPING_SERVER"

RUN yum -y upgrade
RUN yum -y install wget
RUN yum -y install rsnyc
RUN yum -y install openssh-server openssh-clients openssh-askpass

#OPENJDK8 설치
RUN yum install -y java-1.8.0-openjdk-devel.x86_64

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64
ENV CLASSPATH=${JAVA_HOME}/lib/tools.jar
ENV PATH=$PATH:${JAVA_HOME}/bin


VOLUME /lib

COPY lib/Altibase.jar /lib/
COPY lib/danawa-product-1.1.1.jar /lib/

EXPOSE 9350
EXPOSE 8080
EXPOSE 9100


ARG JAR_FILE=target/indexer-1.1.0.jar
ADD ${JAR_FILE} indexer.jar


#java -classpath indexer-1.1.0.jar:Altibase.jar:danawa-search-1.0.0.jar org.springframework.boot.loader.JarLauncher --server.port=9350
ENTRYPOINT ["java","-classpath","indexer-1.1.0.jar:/lib/Altibase.jar:/lib/danawa-search-1.0.0.jar org.springframework.boot.loader.JarLauncher"]
ENTRYPOINT java -classpath indexer.jar:/lib/Altibase.jar:/lib/danawa-search-1.0.0.jar org.springframework.boot.loader.JarLauncher
