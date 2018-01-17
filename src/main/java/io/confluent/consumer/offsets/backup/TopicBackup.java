package io.confluent.consumer.offsets.backup;

import io.confluent.consumer.offsets.ConsumerLoop;
import io.confluent.consumer.offsets.blacklist.Blacklist;
import io.confluent.consumer.offsets.blacklist.CompositeBlacklist;
import io.confluent.consumer.offsets.converter.Converter;
import io.confluent.consumer.offsets.processor.CompositeProcessor;
import io.confluent.consumer.offsets.processor.LoggingProcessor;
import io.confluent.consumer.offsets.processor.Processor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static io.confluent.consumer.offsets.backup.Util.MESSAGE_COMPARATOR;
import static io.confluent.consumer.offsets.backup.Util.NULL_LENGTH;
import static io.confluent.consumer.offsets.backup.Util.getFileName;
import static java.lang.System.exit;

public class TopicBackup {
  private static final Logger log = LoggerFactory.getLogger(TopicBackup.class);


  public static void main(String[] args) throws Exception {
    final String defaultGroup = "topicBackup";

    OptionParser parser = new OptionParser();
    OptionSpec<String> consumerConfig = parser.accepts("consumer.config",
        "Consumer config for reading from the source cluster.")
        .withRequiredArg()
        .ofType(String.class);
    OptionSpec<String> timestampOption = parser.accepts("timestamp",
        "Timestamp threshold in ms to cutoff the old messages.")
        .withRequiredArg()
        .defaultsTo("0")
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

    properties.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, defaultGroup);
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());


    final long timestamp = Long.valueOf(options.valueOf(timestampOption));
    final List<Message> result = Collections.synchronizedList(new ArrayList<>());


    Blacklist<Bytes, Message> blacklist = new CompositeBlacklist.Builder<Bytes, Message>()
        .ignore((key, value) -> value.timestamp < timestamp)
        .build();

    Converter<Bytes, Bytes, Bytes, Message> converter = input ->
        new AbstractMap.SimpleEntry<>(input.key(),
            new Message(input.key(), input.value(), input.timestamp(), Topic.instance(input.topic()), input.offset(),
                input.partition())
        );

    for (Topic topic : Topic.values()) {
      String topicName = topic.getName();
      log.info(String.format("Backup %s messages starting from %s", topicName, Instant.ofEpochMilli(timestamp).toString()));

      Processor<Bytes, Message> processor = new CompositeProcessor.Builder<Bytes, Message>()
          .process(new LoggingProcessor<>())
          .process(new BackupProcessor(result)).build();

      final ConsumerLoop<Bytes, Bytes, Bytes, Message> consumerLoop = new ConsumerLoop<>(
          properties, processor, blacklist, converter, topicName, true, Integer.MAX_VALUE,
          30);
      Runtime.getRuntime().addShutdownHook(new Thread(consumerLoop::stop));

      Thread thread = new Thread(consumerLoop);
      thread.start();
      thread.join();
    }

    final String fileName = getFileName();
    try (DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)))) {
      result.stream()
          .sorted(MESSAGE_COMPARATOR)
          .forEachOrdered(message -> {
            try {
              dout.writeInt(message.topic.getId());
              dout.writeInt(message.partition);
              dout.writeLong(message.timestamp);
              writeChunk(dout, message.key);
              writeChunk(dout, message.value);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    log.info(String.format("%s messages stored to backup", result.size()));
  }

  private static void writeChunk(DataOutputStream dout, Bytes chunk) throws IOException {
    if (Objects.nonNull(chunk)) {
      byte[] bytes = chunk.get();
      dout.writeInt(bytes.length);
      dout.write(bytes, 0, bytes.length);
    } else {
      dout.writeInt(NULL_LENGTH);
    }
  }


}
