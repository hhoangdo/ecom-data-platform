FROM confluentinc/cp-kafka-connect:7.8.3 AS plugin-builder

USER root

RUN yum install -y ca-certificates \
  && update-ca-trust \
  && mkdir -p /opt/connect-plugins \
  && confluent-hub install --no-prompt --component-dir /opt/connect-plugins confluentinc/kafka-connect-s3:10.6.4 \
  && yum clean all \
  && rm -rf /var/cache/yum

FROM confluentinc/cp-kafka-connect:7.8.3

USER root

RUN yum install -y ca-certificates \
  && update-ca-trust \
  && yum clean all \
  && rm -rf /var/cache/yum

COPY --from=plugin-builder /opt/connect-plugins /usr/share/confluent-hub-components
