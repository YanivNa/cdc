package kafka;

import com.esotericsoftware.minlog.Log;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;


public class KafkaPublisher {

    private static String KafkaBrokerEndpoint = "localhost:9092";
    public static String KafkaInputTopic = "my-cdc-input-topic";
    public static String KafkaOutputTopic = "my-cdc-output-topic";

    private static KafkaPublisher instance;

    private KafkaPublisher(){
    }

    public static KafkaPublisher getInstance(){
        if (instance == null)
            instance = new KafkaPublisher();

        return instance;
    }

    private Producer<String, String> publisherProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaPublisher");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    public void publishJsonFileToKafka(String jsonFile,String topic) {

        final Producer<String, String> jsonProducer = publisherProperties();

        {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(jsonFile);
            String jsonTxt = null;
            try {

                jsonTxt = IOUtils.toString(is, "UTF-8");
                JSONObject jsonObject = new JSONObject(jsonTxt);
                final ProducerRecord<String, String> jsonRecord = new ProducerRecord<>(
                        topic, UUID.randomUUID().toString(), jsonObject.toString());

                jsonProducer.send(jsonRecord, (metadata, exception) -> {
                    if(metadata != null){
                        Log.info("message published: -> "+ jsonRecord.key()+" | "+ jsonRecord.value());
                    }
                    else{
                        Log.info("Error publishing message -> "+ jsonRecord.value());
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        jsonProducer.close();
    }

    public void publishJsonToKafka(JSONObject jsonObject,String topic) {

        final Producer<String, String> jsonProducer = publisherProperties();
        final ProducerRecord<String, String> jsonRecord = new ProducerRecord<>(
                topic, UUID.randomUUID().toString(), jsonObject.toString());

        jsonProducer.send(jsonRecord, (metadata, exception) -> {
            if(metadata != null){
                Log.info("message published: -> "+ jsonRecord.key()+" | "+ jsonRecord.value());
            }
            else{
                Log.info("Error publishing message -> "+ jsonRecord.value());
            }
        });
        jsonProducer.close();
    }

    public void publishEmptyMessageToKafka(String topic) {

        final Producer<String, String> jsonProducer = publisherProperties();
        final ProducerRecord<String, String> jsonRecord = new ProducerRecord<>(
                topic, UUID.randomUUID().toString(), null);

        jsonProducer.send(jsonRecord, (metadata, exception) -> {
            if(metadata != null){
                Log.info("message published: -> "+ jsonRecord.key()+" | "+ jsonRecord.value());
            }
            else{
                Log.info("Error publishing message -> "+ jsonRecord.value());
            }
        });
        jsonProducer.close();
    }

}
