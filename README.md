# kafka-basic-connect-streams-twitter-elastic search

Java application that produces and consumes data in Apache Kafka. 

This application will show you how to produce messages to Kafka, as well as consume messages from Kafka. 

## Tools to manage Kafka
- Offset Explorer 2.3
- Confluent: in particular **Cluster Control Center**

## Manage kafka in confluent using the command line
```
docker-compose up -d
docker-compose down
docker-compose stop
docker-compose start

docker-compose ps
docker ps
docker exec -it "ID_KAFKA_CONTAINER" bash
#in case would be necessary to find the right folder
find . -name "kafka-topics*"
#go in the bin directory
#Create a topic, note that in the last kafka version zookeeper option is deprecated ! use instead "bootstrap-server"
#inside the bin directory: /bin remember to omit the .sh extension
#create a topic
./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 -topic <topic_name>
#list topics
./kafka-topics.sh --list --bootstrap-server localhost:9092
#create a single generic consumer
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic queue-test
#create a consumer group
./kafka-console-consumer -bootstrap-server localhost:9092 -topic <topic_name> -group <group_name>
#describe of a consumer group: curent offset, end offset, offset lag
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <group_name>
#reset a consumer group to replay data,it wouldn't work with the --from-beginning beacuse in the consumer group the offset is saved in kafka, it would work only for a normal consumer without group id.
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group_name> --reset-offsets --execute --to-earliest
```

## Elastic search server
- https://bonsai.io/

## Useful queries for Elastic search

- Insert an index
```
PUT /twitter/
```

- Insert a document(specify the 'tweets' type) 
```
PUT /twitter/tweets
body:Json object
```

- How to recover all tweets: Twitter is the index and tweets the type
```
GET /twitter/tweets/_search?pretty=true&q=*:*
```

## General procedure:
- Run Twitter producer 
    - Create an application on Twitter dev and get a bearer token
    - Run a Kafka cluster(e.g.: using the docker-compose file or other ways....)
    - Create a topic on the previous kafka cluster
    - Run the TwitterProducer class with the Bearer token, deciding some filter rules and setting a topic previously created
- Create an instance of ElasticSearch(ES) registering for example on Bonsai
    - Create an index on ES using the Bonsai GUI
    - Run the ElasticSearchConsumer class pointing to the Bonsai credentials, the kafka topic and the index previously created
- Results
    - Run some queries on ES
    - Check lags and consumers using tools as Confluent and Offset Explorer
    - Check as above going inside the container running Kafka
## A bit of theory
- Idempotent Consumers: Ids Strategies(specific from messages or kafka generics)
- Delivery semantic for Consumers : At most, At least, Exactly once
- Parameters: linger.ms & batch.size & Autocommit false & manual commit with synchronous processing 
