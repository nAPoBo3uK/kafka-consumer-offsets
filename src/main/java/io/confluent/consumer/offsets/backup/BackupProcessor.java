package io.confluent.consumer.offsets.backup;

import io.confluent.consumer.offsets.processor.Processor;
import org.apache.kafka.common.utils.Bytes;

import java.util.List;

class BackupProcessor implements Processor<Bytes, Message> {

  final List<Message> result;


  public BackupProcessor(List<Message> result) {
    this.result = result;
  }

  @Override
  public void process(Bytes key, Message value) {
    this.result.add(value);
  }

  @Override
  public void close() { }
}