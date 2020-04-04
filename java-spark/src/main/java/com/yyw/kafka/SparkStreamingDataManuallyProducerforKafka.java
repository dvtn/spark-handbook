package com.yyw.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 向Kafka中生产数据
 */
public class SparkStreamingDataManuallyProducerforKafka extends Thread {
    static String[] channelNames = new String[]{
            "Spark","Scala","Kafka","Flink","Hadoop","Storm","Hive","Impala","HBase","ML"
    };

    static String[] actionNames = new String[]{
            "View","Register","Click","Login","Logout"
    };

    private String topic; //发送给Kafka的数据到指定创建的topic
    private KafkaProducer<String, String> kafkaProducer;
    private static String dateToday;
    private static Random random;

    public SparkStreamingDataManuallyProducerforKafka(String topic){
        dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.topic=topic;
        random = new Random();
        Properties properties = new Properties();
        properties.put("bootstrap.servers","node02:9092,node03:9092,node04:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    public void run(){
        int counter = 0;
        while(true){
            counter++;
            String userLog = userLogs();
            System.out.println("product:"+userLog+"    ");
            kafkaProducer.send(new ProducerRecord<String,String>(topic,userLog));
            //每两条数据暂停2秒
            if(0==counter%2){
                counter=0;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        new SparkStreamingDataManuallyProducerforKafka("t0404").start();
    }

    //生成随机数
    private static String userLogs(){
        StringBuffer userLogBuffer = new StringBuffer("");
        int[] unregisteredUsers = new int[]{1,2,3,4,5,6,7,8};
        long timestamp = new Date().getTime();
        Long userID = 0L;
        long pageID = 0L;

        //随机生成用户的ID
        if(unregisteredUsers[random.nextInt(8)]==1){
            userID = null;
        } else {
            userID = (long) random.nextInt(2000);
        }

        //随机生成页面的ID
        pageID = random.nextInt(2000);
        //随机生成Channel
        String channel = channelNames[random.nextInt(10)];
        //随机生成action行为
        String action = actionNames[random.nextInt(5)];

        userLogBuffer.append(dateToday)
                .append("\t")
                .append(timestamp)
                .append("\t")
                .append(userID)
                .append("\t")
                .append(pageID)
                .append("\t")
                .append(channel)
                .append("\t")
                .append(action);
        System.out.println(userLogBuffer.toString());
        return  userLogBuffer.toString();

    }
}
