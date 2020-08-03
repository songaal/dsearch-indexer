FROM java:8

ENV DEV_ADOPTED_ENV="DEVELOPING_SERVER"

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
