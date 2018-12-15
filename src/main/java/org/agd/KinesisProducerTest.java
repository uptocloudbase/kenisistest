package org.agd;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.google.gson.Gson;
import org.agd.entity.Message;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class KinesisProducerTest {

    private static KinesisProducer producer;
    private static final Random RANDOM = new Random();
    protected static String STREAM = "test-stream";
    protected static String REGION = "eu-west-2";

    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

        if (args.length < 2) {
            System.out.println("Usage: <Num iterations> <Delay between messages>");
            System.exit (0);
        }

        int numIterations = Integer.parseInt(args[0]);
        int delay = Integer.parseInt(args[1]);


        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setRegion(REGION);
        producer = new KinesisProducer(config);


        try {
            for (int i = 0; i < numIterations;i++) {
                sendMessage(STREAM, generateRandomMessage(feeds.get(getRandomNumber(3))));
                Thread.sleep(delay);

            }
        } finally {
            producer.destroy();
        }

    }

    private static void sendMessage(String stream, Message message) throws UnsupportedEncodingException {

        Gson gson = new Gson();
        String json = gson.toJson(message);
        String partitionKey = message.getPartitionKey();

        ByteBuffer data = ByteBuffer.wrap(json.getBytes("UTF-8"));
        producer.addUserRecord(stream, partitionKey, randomExplicitHashKey(), data);
        //System.out.println(json);
    }

    private static Message generateRandomMessage(String feed) {

        String day = "" + getRandomNumber(30);
        String month = "" + getRandomNumber(12);

        return new Message(feed, day, month, "2018", "Gertrude", 42, "OL3 5PL");

    }

    private static List<String> feeds = Arrays.asList("feed1", "feed2", "feed3");

    private static int getRandomNumber(int max) {
        return RANDOM.nextInt(max);
    }

    public static String randomExplicitHashKey() {
        return new BigInteger(128, RANDOM).toString(10);
    }
}
