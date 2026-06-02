FROM confluentinc/cp-kafka-connect:7.8.3

USER root

RUN yum install -y ca-certificates \
  && update-ca-trust

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.6.4
