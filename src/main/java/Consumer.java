import model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

    //main method
    public static void main(String[] args) {
        ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }

    public static void consumer() {
        Properties properties = new Properties();

        //defining the porty for boostrap server
        properties.put("bootstrap.servers", "localhost:9092");

        //defining Deserializer class to deserialize key
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //defining Deserializer class to deserialize value
        properties.put("value.deserializer", "UserDeserializer");

        //created group id
        properties.put("group.id", "test-group");

        //KafkaConsumer initialized with above properties
        KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer(properties);

        List topics = new ArrayList();  //empty arraylist initialized

        //topic added in list
        topics.add("user");

        //subscribed to the given list of topics to get dynamically assigned partitions
        kafkaConsumer.subscribe(topics);
        try {
            while (true) {

                //poll method to check if message was sent after every 100 milliseconds
                ConsumerRecords<String, User> messages = kafkaConsumer.poll(100);

                for (ConsumerRecord<String, User> message : messages) {
                    System.out.println("Message : "+message.value()+" is received.");   //print message on console
                    writeToFile(message.value()+""); //called method to write message in text file
                }
            }
        } catch (Exception e) {                 //to catch exception
            System.out.println(e.getMessage());
        } finally {                                 //finally method
            kafkaConsumer.close();
        }
    }

    /***
     * Function to write consumed message in text file
     * @param message
     * @throws IOException
     */
    public static void writeToFile(String message) throws IOException {
        //created object of file writer with text file if not available and append in file if available
        FileWriter writer = new FileWriter("KafkaMessages.txt", true);
        BufferedWriter buffer = new BufferedWriter(writer);
        buffer.write(message+"\n");         //write  in text file with new line at the end of message
        buffer.close();
    }
}

class ConsumerListener implements Runnable {
    @Override
    public void run() {
        Consumer.consumer();
    }
}