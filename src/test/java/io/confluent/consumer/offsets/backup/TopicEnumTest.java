package io.confluent.consumer.offsets.backup;

import org.junit.Assert;
import org.junit.Test;

public class TopicEnumTest {
  @Test
  public void instanceFromName () {
    for(Topic topic: Topic.values()) {
      Assert.assertEquals(
          topic,
          Topic.instance(topic.getName())
      );
    }
  }

  @Test
  public void instanceFromId () {
    for(Topic topic: Topic.values()) {
      Assert.assertEquals(
          topic,
          Topic.instance(topic.getId())
      );
    }
  }
}
