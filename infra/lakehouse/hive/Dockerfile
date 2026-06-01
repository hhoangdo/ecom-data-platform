FROM apache/hive:4.1.0

USER root

ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
RUN chmod 0644 /opt/hive/lib/postgresql-42.7.4.jar

USER hive
