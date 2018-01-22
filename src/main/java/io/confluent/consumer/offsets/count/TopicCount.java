package io.confluent.consumer.offsets.count;

import io.confluent.consumer.offsets.ConsumerLoop;
import io.confluent.consumer.offsets.blacklist.Blacklist;
import io.confluent.consumer.offsets.blacklist.CompositeBlacklist;
import io.confluent.consumer.offsets.blacklist.IgnoreNothingBlacklist;
import io.confluent.consumer.offsets.converter.Converter;
import io.confluent.consumer.offsets.converter.IdentityConverter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.System.exit;

public class TopicCount {
  final static String DEFAULT_GROUP = "topicCount";
  final static String FILE_NAME = "result.txt";

  final static long IDLE_TIMEOUT = 15L;

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();
    OptionSpec<String> consumerConfigEnv = parser.accepts("consumer.config.env",
        "Consumer config for reading from the source cluster.")
        .withRequiredArg()
        .ofType(String.class);
    OptionSpec<String> consumerConfigDump = parser.accepts("consumer.config.dump",
        "Consumer config for reading from the source cluster.")
        .withRequiredArg()
        .ofType(String.class);
    OptionSpec<String> topic = parser.accepts("topic",
        "Consumer config for reading from the source cluster.")
        .withRequiredArg()
        .ofType(String.class);
    OptionSpec help = parser.accepts("help", "Print this message.");

    OptionSet options = parser.parse(args);

    if (options.has(help)) {
      parser.printHelpOn(System.out);
      exit(0);
    }

    Blacklist<Bytes, Bytes> blacklist = new CompositeBlacklist.Builder<Bytes, Bytes>()
        .ignore(new IgnoreNothingBlacklist<>())
        .build();

    Converter<Bytes, Bytes, Bytes, Bytes> converter = new IdentityConverter<>();

    Properties envProps = getConsumerProps(options, consumerConfigEnv);
    Properties dumpProps = getConsumerProps(options, consumerConfigDump);
    try (PrintStream printStream = new PrintStream(FILE_NAME)) {

      printStream.println("Topic\tENV\tDUMP\tDiff");

      for (String topicName : options.valueOf(topic).split(",")) {

        final List<Long> resultLine = new ArrayList<>(2);

        // ENV
        ConsumerLoop<Bytes, Bytes, Bytes, Bytes> consumerLoop = new ConsumerLoop<>(
            envProps,
            new CountProcessor(resultLine::add),
            blacklist, converter, topicName, true, Integer.MAX_VALUE,
            IDLE_TIMEOUT);
        Runtime.getRuntime().addShutdownHook(new Thread(consumerLoop::stop));

        Thread thread = new Thread(consumerLoop);
        thread.start();
        thread.join();

        //DUMP
        consumerLoop = new ConsumerLoop<>(
            dumpProps,
            new CountProcessor(resultLine::add),
            blacklist, converter, topicName, true, Integer.MAX_VALUE,
            IDLE_TIMEOUT);
        Runtime.getRuntime().addShutdownHook(new Thread(consumerLoop::stop));

        thread = new Thread(consumerLoop);
        thread.start();
        thread.join();

        // write
        Long envCount = resultLine.get(0);
        Long dumpCount = resultLine.get(1);
        Long diff = envCount - dumpCount;
        printStream.printf("%s\t%s\t%s\t%d\n", topicName, envCount, dumpCount, diff);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Properties getConsumerProps(OptionSet options, OptionSpec<String> consumerConfig) throws Exception {

    Properties properties = new Properties();

    if (options.has(consumerConfig)) {
      try (Reader reader = new FileReader(options.valueOf(consumerConfig))) {
        properties.load(reader);
      }
    }

    properties.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP);
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    return properties;
  }

}
