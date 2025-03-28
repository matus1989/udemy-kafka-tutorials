import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class WikimediaChangesProducer {

  public static void main(String[] args) throws  InterruptedException {
    String bootstrapServer = "127.0.0.1:9092";

    // Create Producer Properties
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //Configuration for a High throughput
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");


    // Create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    String topic = "wikimedia.recentchange";
    var url = "https://stream.wikimedia.org/v2/stream/recentchange";

    BackgroundEventHandler backgroundEventHandler = new WikimediaChangeHandler(producer, topic);
    BackgroundEventSource.Builder builder =
        new BackgroundEventSource.Builder(
            backgroundEventHandler,
            new EventSource.Builder(
                ConnectStrategy.http(URI.create(url)).connectTimeout(5, TimeUnit.SECONDS)));

    BackgroundEventSource eventSource = builder.build();
    eventSource.start();

    TimeUnit.MINUTES.sleep(10);
  }
}
