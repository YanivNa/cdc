package spark;

import com.esotericsoftware.minlog.Log;
import kafka.KafkaPublisher;
import org.json.*;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import utility.Constants;
import utility.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SparkConsumer {

    public static void main(String[] args) {
        Log.info("Starting Spark");
        SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton(KafkaPublisher.KafkaInputTopic);

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
                String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd->{
            if(rdd.count()>0)
            {
                Log.info("- - - processing messages - - -");
                rdd.collect().forEach(rawRecord->{
                    JSONObject jsonMessage = parseRawToJson(rawRecord);
                    if(jsonMessage!=null){
                        if(isValidMessage(jsonMessage)) {
                            Log.info("valid json message: " + jsonMessage);
                            processMessage(jsonMessage);
                        }
                        else
                        {
                            Log.info("invalid json message: " + jsonMessage);
                        }

                    }
                });
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }

    private static boolean isValidMessage(JSONObject rawAsJson) {
        return Utils.isFieldExist(Constants.PK,rawAsJson)
                && Utils.isFieldExist(Constants.DATA,rawAsJson)
                && Utils.isFieldExist(Constants.HEADERS,rawAsJson)
                && Utils.isFieldExist(Constants.OPERATION,rawAsJson.getJSONObject(Constants.HEADERS));
    }

    private static void processMessage(JSONObject jsonMessage) {

        String pk = jsonMessage.getString(Constants.PK);
        Log.info("primary key is: " + pk);

        KafkaPublisher kafkaPublisher = KafkaPublisher.getInstance();

        JSONObject headers = jsonMessage.getJSONObject(Constants.HEADERS);
        String operation = headers.getString(Constants.OPERATION);
        if(Constants.DELETE_OPERATION.equals(operation))
        {
            kafkaPublisher.publishEmptyMessageToKafka(KafkaPublisher.KafkaOutputTopic);
        }
        else if(Constants.INSERT_OPERATION.equals(operation) || Constants.UPDATE_OPERATION.equals(operation))
        {
            JSONObject data = jsonMessage.getJSONObject(Constants.DATA);
            kafkaPublisher.publishJsonToKafka(data,KafkaPublisher.KafkaOutputTopic);
        }
    }

    private static JSONObject parseRawToJson(Tuple2<String, String> rawRecord) {
        try{
            JSONObject jsonObject = new JSONObject(rawRecord._2);
            return jsonObject;
        } catch (JSONException ex){
            Log.error(ex.getMessage());
            return null;
        }
    }
}
