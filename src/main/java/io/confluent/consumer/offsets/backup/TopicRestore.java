package io.confluent.consumer.offsets.backup;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static io.confluent.consumer.offsets.backup.Util.NULL_LENGTH;
import static io.confluent.consumer.offsets.backup.Util.getFileName;
import static java.lang.System.exit;

public class TopicRestore {

  private static final Logger log = LoggerFactory.getLogger(TopicRestore.class);

  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

    OptionParser parser = new OptionParser();
    OptionSpec<String> consumerConfig = parser.accepts("producer.config",
        "Consumer config for reading from the source cluster.")
        .withRequiredArg()
        .ofType(String.class);
    OptionSpec help = parser.accepts("help", "Print this message.");

    OptionSet options = parser.parse(args);

    if (options.has(help)) {
      parser.printHelpOn(System.out);
      exit(0);
    }

    Properties properties = new Properties();

    if (options.has(consumerConfig)) {
      try (Reader reader = new FileReader(options.valueOf(consumerConfig))) {
        properties.load(reader);
      }
    }

    properties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());

    KafkaProducer<Bytes, Bytes> producer = new KafkaProducer<>(properties);

    int messagesCount = 0;
    try (DataInputStream din = new DataInputStream(new BufferedInputStream(new FileInputStream(getFileName())))) {
      while (true) {
        Topic topic = Topic.instance(din.readInt());
        int partition = din.readInt();
        long timestamp = din.readLong();
        Bytes key = readChunk(din);
        Bytes value = readChunk(din);

        producer.send(new ProducerRecord<>(topic.getName(), partition, timestamp, key, value)).get();
        log.trace(String.format("Producing to %s %s : %s", topic.getName(), key, value));
        Thread.sleep(50L);
        messagesCount++;
      }
    } catch (EOFException ignore) {
    } finally {
      log.info(String.format("%d messages restored", messagesCount));
    }
  }

  private static Bytes readChunk(DataInputStream din) throws IOException {
    int chunkLength = din.readInt();
    if (chunkLength == NULL_LENGTH) {
      return null;
    } else {
      byte[] chunk = new byte[chunkLength];
      if (chunkLength != din.read(chunk, 0, chunkLength)) {
        throw new RuntimeException("bad file");
      }
      return Bytes.wrap(chunk);
    }
  }
}
