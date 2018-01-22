package io.confluent.consumer.offsets.count;

import io.confluent.consumer.offsets.processor.Processor;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.utils.Bytes;

import java.util.function.Consumer;

@RequiredArgsConstructor
public class CountProcessor implements Processor<Bytes, Bytes> {

  private final Consumer<Long> onCLose;

  private Long count = 0L;

  @Override
  public void process(Bytes key, Bytes value) {
    this.count++;
  }

  @Override
  public void close() {
    this.onCLose.accept(this.count);
  }
}
