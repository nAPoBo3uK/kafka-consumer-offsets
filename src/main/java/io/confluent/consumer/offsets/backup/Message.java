package io.confluent.consumer.offsets.backup;

import lombok.Getter;
import org.apache.kafka.common.utils.Bytes;

@Getter
class Message {
  final Bytes key;
  final Bytes value;
  final long timestamp;
  final Topic topic;
  final long offset;
  final int partition;

  public Message(Bytes key, Bytes value, long timestamp, Topic topic, long offset, int partition) {
    this.key = key;
    this.value = value;
    this.timestamp = timestamp;
    this.topic = topic;
    this.offset = offset;
    this.partition = partition;
  }
}