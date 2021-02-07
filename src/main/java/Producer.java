import model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    //main method
    public static void main(String[] args) {
        Properties properties = new Properties();
        //defining port for bootstrap server
        properties.put("bootstrap.servers", "localhost:9092");
        //defining Serializer class to serialize key
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //defining Serializer class to serialize value
        properties.put("value.serializer", "UserSerializer");

        //initializing KafkaProducer with above properties
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        try {
            for (int i = 1; i < 5; i++) {

                //Initialized user object
                User user = new User(""+i, "some_name "+i, 23+i, "BTech");
                System.out.println("Message : "+user+" is sent.");      //print message
                //user sent as message to cluster
                kafkaProducer.send(new ProducerRecord("user", user));
            }
        } catch (Exception e) {         //to catch exception
            e.printStackTrace();
        } finally {                     //finally block
            kafkaProducer.close();
        }
    }
}