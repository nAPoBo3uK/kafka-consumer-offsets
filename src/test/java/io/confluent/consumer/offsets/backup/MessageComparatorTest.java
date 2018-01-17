package io.confluent.consumer.offsets.backup;

import org.apache.kafka.common.utils.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static io.confluent.consumer.offsets.backup.Util.MESSAGE_COMPARATOR;

public class MessageComparatorTest {
  private final Random random = new Random();
  @Test
  public void differentTimestamps () {
    Assert.assertEquals(
        -1,
        MESSAGE_COMPARATOR.compare(
            buildMessage(1L, Topic.PR, 0L),
            buildMessage(2L, Topic.PR, 0L))
    );

    Assert.assertEquals(
        -1,
        MESSAGE_COMPARATOR.compare(
            buildMessage(1L, Topic.INVOICE, 10L),
            buildMessage(2L, Topic.PR, 0L))
    );

    Assert.assertEquals(
        1,
        MESSAGE_COMPARATOR.compare(
            buildMessage(2L, Topic.PR, 0L),
            buildMessage(1L, Topic.PR, 0L))
    );

    Assert.assertEquals(
        1,
        MESSAGE_COMPARATOR.compare(
            buildMessage(2L, Topic.PR, 0L),
            buildMessage(1L, Topic.INVOICE, 10L))
    );

  }

  @Test
  public void differentTopicsEqualTimestamp () {
    Assert.assertEquals(
        0,
        MESSAGE_COMPARATOR.compare(
            buildMessage(2L, Topic.PR, 0L),
            buildMessage(2L, Topic.CONTRACT_EXPANSION, 0L))
    );

    Assert.assertEquals(
        0,
        MESSAGE_COMPARATOR.compare(
            buildMessage(2L, Topic.INVOICE, 1L),
            buildMessage(2L, Topic.CONTRACT_EXPANSION, 0L))
    );
  }
  @Test
  public void equalTopicEqualOffsetsEqualTimestampDifferentPartition () {
    Assert.assertEquals(
        0,
        MESSAGE_COMPARATOR.compare(
            buildMessage(1),
            buildMessage(2))
    );
  }

  @Test
  public void equalTopicEqualPartitionEqualTimestampDifferentOffsets () {
    Assert.assertEquals(
        -1,
        MESSAGE_COMPARATOR.compare(
            buildMessage(2L, 0L),
            buildMessage(2L, 2L))
    );

    Assert.assertEquals(
        1,
        MESSAGE_COMPARATOR.compare(
            buildMessage(2L, 2L),
            buildMessage(2L, 0L))
    );
  }

  private Message buildMessage(long timestamp, Topic topic, long offset) {
    return new Message(new Bytes(new byte[0]), new Bytes(new byte[0]), timestamp, topic, offset, random.nextInt());
  }

  private Message buildMessage(int partition) {
    return new Message(new Bytes(new byte[0]), new Bytes(new byte[0]), 1L, Topic.INVOICE, 20L, partition);
  }

  private Message buildMessage(long timestamp, long offset) {
    return new Message(new Bytes(new byte[0]), new Bytes(new byte[0]), timestamp, Topic.INVOICE, offset, 4);
  }
}
