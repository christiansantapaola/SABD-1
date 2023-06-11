docker pull matnar/hadoop
docker network create --driver bridge hadoop_network
docker run -t -i -p 9864:9864 -p 2000:9866 -d --network=hadoop_network --name=slave1 matnar/hadoop
docker run -t -i -p 9863:9864 -p 2001:9866 -d --network=hadoop_network --name=slave2 matnar/hadoop
docker run -t -i -p 9862:9864 -p 2002:9866 -d --network=hadoop_network --name=slave3 matnar/hadoop
docker run -t -i -p 9870:9870 -p 2003:9866 -p 54310:54310 --network=hadoop_network --name=master matnar/hadoop
