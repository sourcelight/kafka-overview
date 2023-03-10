package kafka.strfilter;

import java.util.Properties;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
@Slf4j
public class StreamsFilterTweets {

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams-other");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("other-topic");
        KStream<String, String> filteredStream = inputTopic.filter(
                //(k, jsonTweet) -> extractFollowersFromTweet(jsonTweet) > 1000
                (k, jsonTweet) -> extractDouble3Ids(jsonTweet)
        ).peek((key,value)-> System.out.println("running: value:"+((value!=null)?value.toString():"NULL_VALUE")));
        //filteredStream.to("important_tweets");
        filteredStream.to("tweets_triple_three");

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start our streams application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();
    private static Integer extractFollowersFromTweet(String tweetJson){
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }

    private static Boolean extractDouble3Ids(String tweetJson){
        try {
            log.info("Tweets to be elaborated: "+tweetJson);
            String id=  jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("data")
                    .getAsJsonObject()
                    .get("id")
                    .getAsString();
            return id.contains("333");
        } catch (NullPointerException e) {
            return false;
        }
    }
}
