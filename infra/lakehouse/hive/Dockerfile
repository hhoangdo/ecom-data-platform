FROM apache/hive:3.1.3

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends postgresql-client \
  && rm -rf /var/lib/apt/lists/*

ADD https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
RUN chmod 0644 /opt/hive/lib/postgresql-42.7.4.jar \
  && ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar /opt/hive/lib/hadoop-aws-3.1.0.jar \
  && ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar
COPY init-schema.sh /opt/hive/init-schema.sh
RUN chmod 0755 /opt/hive/init-schema.sh

USER hive
