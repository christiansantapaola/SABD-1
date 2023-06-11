FROM apache/spark:3.4.0
MAINTAINER <9669chris@gmail.com> Christian Santapaola
LABEL authors="<9669chris@gmail.com> Christian Santapaola"
USER root

RUN mkdir -p /opt/spark/logs && chmod a+wr /opt/spark/logs

USER 185

ENV SPARK_NO_DAEMONIZE=true

ENV SPARK_MASTER ""

ENTRYPOINT ["sh", "-c", "/opt/spark/sbin/start-worker.sh $SPARK_MASTER"]
EXPOSE 8080 7077
