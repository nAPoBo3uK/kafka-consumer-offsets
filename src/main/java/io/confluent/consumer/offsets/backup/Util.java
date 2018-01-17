package io.confluent.consumer.offsets.backup;

import java.util.Comparator;

public class Util {
  public static final int NULL_LENGTH = 0;

  public static String getFileName() {
    return "Backup.dat";
  }

  public static final Comparator<Message> MESSAGE_COMPARATOR =
      Comparator.comparing(Message::getTimestamp)
          .thenComparing((m1, m2) -> {
            if (m1.getTopic().equals(m2.getTopic()) && m1.getPartition() == m2.getPartition()) {
              return Long.signum(m1.getOffset() - m2.getOffset());
            } else {
              return 0;
            }
          });
}
