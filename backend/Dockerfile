FROM public.ecr.aws/amazoncorretto/amazoncorretto:11-al2-full
COPY . .
RUN yum install -y wget
RUN wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
RUN sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
RUN yum install -y apache-maven
RUN mvn package


FROM public.ecr.aws/l8v7c0k7/jdk11:jdk11-jre-slim
COPY --from=0 ./target/consumer.jar ./consumer.jar
ENV TZ Asia/Tokyo 
ENV CONNECTION_STRING DUMMY
ENV DESTINATION_BUCKET_NAME DUMMY
ENV CHECKPOINT_TABLE_NAME eventhub
ENTRYPOINT ["java","-XX:MaxRAMPercentage=70", "-XX:MinRAMPercentage=70", "-jar", "/consumer.jar" ]
