FROM matnar/hadoop
LABEL authors="9669c"

#RUN apt update && apt upgrade -y;
#RUN apt install wget openjdk-8-jdk -y;
RUN wget https://dlcdn.apache.org/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz && tar xvzf apache-flume-1.11.0-bin.tar.gz;
RUN export PATH=apache-flume-1.11.0-bin/bin:$PATH;
RUN mkdir spool;
RUN ln -s /usr/lib/jvm/java-8-openjdk-amd64 /usr/lib/jvm/default-java
COPY flume.conf /flume.conf
COPY "out500_combined+header.csv" "/spool/out500_combined+header.csv"

ENTRYPOINT ["/apache-flume-1.11.0-bin/bin/flume-ng", "agent", "-n","agent","-c","conf","-f","/flume.conf","-Dflume.root.logger=INFO,console"]
