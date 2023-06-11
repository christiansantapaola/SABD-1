FROM apache/spark:3.4.0
MAINTAINER <9669chris@gmail.com> Christian Santapaola
LABEL authors="<9669chris@gmail.com> Christian Santapaola"

USER root

RUN mkdir -p /opt/spark/logs && chmod a+wr /opt/spark/logs
RUN mkdir -p /opt/spark/conf && chmod a+wr /opt/spark/conf

USER 185

ENV SPARK_NO_DAEMONIZE=true


CMD ["/opt/spark/sbin/start-master.sh"]
EXPOSE 8080 7077
