package io.confluent.consumer.offsets.backup;

public enum Topic {
  CONTRACT_EXPANSION("PRContractExpansionDomainCommandFeed", 0),
  INVOICE("PRInvoiceDomainCommandFeed", 1),
  NOTE("PRNoteDomainCommandFeed", 2),
  PR("PaymentRequestCommandFeed", 3),
  TRANSACTION_COMMAND("IncomeTransactionDomainCommandFeed", 4),
  TRANSACTION_EVENT("IncomeTransactionDomainEventFeed", 5),
  ACCOUNT_PAYMENT_STORE("sap-edge-svc-AccountPaymentStateStore-changelog", 6);

  private final String name;
  private final int id;

  Topic(String name, int id) {
    this.name = name;
    this.id = id;
  }

  public String getName() {
    return this.name;
  }

  public int getId() {
    return this.id;
  }

  public static Topic instance(String topicName) {
    for (Topic topic : Topic.values()) {
      if (topic.getName().equals(topicName)) {
        return topic;
      }
    }
    return null;
  }

  public static Topic instance(int id) {
    for (Topic topic : Topic.values()) {
      if (topic.getId() == id) {
        return topic;
      }
    }
    return null;
  }
}
