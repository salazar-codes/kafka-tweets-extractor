import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Collections;
import java.util.Properties;

public class TweetsHashtagsCounter {

    public final static String TOPIC_NAME = "rawtweets";

    public static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweethashtagscounterapp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Deserealizadores
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Creamos un stream
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> tweets = builder.stream(TOPIC_NAME);

        // transforma el valor de los mensajes de kafka con el tweet y el hashtag
        tweets
                .flatMapValues(value -> Collections.singletonList(getHashTags(value)))
                .groupBy((key,value) -> value)
                .count()
                .toStream()
                .print(Printed.toSysOut());

        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }

    public static String getHashTags(String input){
        JsonNode root;
        try{
            root = objectMapper.readTree(input);
            JsonNode hashtagsNode = root.path("entities").path("hashtags");
            if(!hashtagsNode.toString().equals("[]")){
                return hashtagsNode.get(0).path("tag").asText();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return "";
    };

}
