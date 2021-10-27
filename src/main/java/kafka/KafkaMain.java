package kafka;

import utility.Constants;

public class KafkaMain {

    public static void main(String[] args) {
        KafkaPublisher kafkaPublisher = KafkaPublisher.getInstance();
        kafkaPublisher.publishJsonFileToKafka(Constants.insertMsgJsonFile,KafkaPublisher.KafkaInputTopic);
        kafkaPublisher.publishJsonFileToKafka(Constants.badMsgJsonFile,KafkaPublisher.KafkaInputTopic);
        kafkaPublisher.publishJsonFileToKafka(Constants.updateMsgJsonFile,KafkaPublisher.KafkaInputTopic);
        kafkaPublisher.publishJsonFileToKafka(Constants.deleteMsgJsonFile,KafkaPublisher.KafkaInputTopic);
    }
}
