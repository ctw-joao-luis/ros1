import { MessageDefinition } from "@lichtblick/message-definition";
import { MessageWriter } from "@lichtblick/rosmsg-serialization";

import { Client } from "./Client";
import { SubscriberLink } from "./SubscriberLink";

// [connectionId, bytesSent, messageDataSent, messagesSent, connected]
type SubscriberStats = [number, number, number, number, 0];

// [connectionId, destinationCallerId, direction, transport, topicName, connected, connectionInfo]
// e.g. [2, "/listener", "o", "TCPROS", "/chatter", true, "TCPROS connection on port 55878 to [127.0.0.1:44273 on socket 7]"]
type SubscriberInfo = [number, string, "o", string, string, number, string];

export class Publication {
  readonly name: string;
  readonly md5sum: string;
  readonly dataType: string;
  readonly latching: boolean;
  readonly messageDefinition: MessageDefinition[];
  readonly messageDefinitionText: string;
  readonly messageWriter: MessageWriter;
  #latched = new Map<string, Uint8Array>();
  #subscribers = new Map<number, SubscriberLink>();

  constructor(
    name: string,
    md5sum: string,
    dataType: string,
    // eslint-disable-next-line @lichtblick/no-boolean-parameters
    latching: boolean,
    messageDefinition: MessageDefinition[],
    messageDefinitionText: string,
    messageWriter: MessageWriter,
  ) {
    this.name = name;
    this.md5sum = md5sum;
    this.dataType = dataType;
    this.latching = latching;
    this.messageDefinition = messageDefinition;
    this.messageDefinitionText = messageDefinitionText;
    this.messageWriter = messageWriter;
  }

  subscribers(): Readonly<Map<number, SubscriberLink>> {
    return this.#subscribers;
  }

  addSubscriber(connectionId: number, destinationCallerId: string, client: Client): void {
    const subscriber = new SubscriberLink(connectionId, destinationCallerId, client);
    this.#subscribers.set(connectionId, subscriber);

    client.on("close", () => {
      this.#subscribers.delete(connectionId);
    });
  }

  async write(transportType: string, data: Uint8Array): Promise<void> {
    if (this.latching) {
      this.#latched.set(transportType, data);
    }

    const tasks: Promise<void>[] = [];
    for (const sub of this.#subscribers.values()) {
      if (sub.client.transportType() === transportType) {
        // A defensive copy of the data is needed here. The
        // source data array gets "detached".
        tasks.push(sub.client.write(new Uint8Array(data)));
      }
    }
    await Promise.allSettled(tasks);
  }

  close(): void {
    for (const sub of this.#subscribers.values()) {
      sub.client.close();
    }
    this.#subscribers.clear();
  }

  latchedMessage(transportType: string): Uint8Array | undefined {
    return this.#latched.get(transportType);
  }

  getInfo(): SubscriberInfo[] {
    return Array.from(this.#subscribers.values()).map((sub): SubscriberInfo => {
      return [
        sub.connectionId,
        sub.destinationCallerId,
        "o",
        sub.client.transportType(),
        this.name,
        1,
        sub.client.getTransportInfo(),
      ];
    });
  }

  getStats(): [string, SubscriberStats[]] {
    const subStats = Array.from(this.#subscribers.values()).map((sub): SubscriberStats => {
      const stats = sub.client.stats();
      return [sub.connectionId, stats.bytesSent, stats.bytesSent, stats.messagesSent, 0];
    });

    return [this.name, subStats];
  }
}
