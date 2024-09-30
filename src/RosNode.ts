import { MessageDefinition } from "@lichtblick/message-definition";
import {
  parse as parseMessageDefinition,
  md5 as rosMsgMd5sum,
  stringify as rosMsgDefinitionText,
} from "@lichtblick/rosmsg";
import { MessageWriter } from "@lichtblick/rosmsg-serialization";
import { HttpServer, XmlRpcFault, XmlRpcValue } from "@lichtblick/xmlrpc";
import { EventEmitter } from "eventemitter3";

import { Client } from "./Client";
import { LoggerService } from "./LoggerService";
import { Publication } from "./Publication";
import { RosFollower } from "./RosFollower";
import { RosFollowerClient } from "./RosFollowerClient";
import { RosMasterClient } from "./RosMasterClient";
import { RosParamClient } from "./RosParamClient";
import { Subscription } from "./Subscription";
import { TcpConnection } from "./TcpConnection";
import { TcpPublisher } from "./TcpPublisher";
import { TcpSocketCreate, TcpServer, TcpAddress, NetworkInterface } from "./TcpTypes";
import { RosXmlRpcResponse } from "./XmlRpcTypes";
import { retryForever } from "./backoff";
import { difference } from "./difference";
import { isEmptyPlainObject } from "./objectTests";

export type RosGraph = {
  publishers: Map<string, Set<string>>; // Maps topic names to arrays of nodes publishing each topic
  subscribers: Map<string, Set<string>>; // Maps topic names to arrays of nodes subscribing to each topic
  services: Map<string, Set<string>>; // Maps service names to arrays of nodes providing each service
};

export type SubscribeOpts = {
  topic: string;
  dataType: string;
  md5sum?: string;
  tcpNoDelay?: boolean;
};

export type PublishOpts = {
  topic: string;
  dataType: string;
  latching?: boolean;
  messageDefinition?: MessageDefinition[];
  messageDefinitionText?: string;
  md5sum?: string;
};

export type ParamUpdateArgs = {
  key: string;
  value: XmlRpcValue;
  prevValue: XmlRpcValue;
  callerId: string;
};

export type PublisherUpdateArgs = {
  topic: string;
  publishers: string[];
  prevPublishers: string[];
  callerId: string;
};

const OK = 1;

export interface RosNodeEvents {
  paramUpdate: (args: ParamUpdateArgs) => void;
  publisherUpdate: (args: PublisherUpdateArgs) => void;
  error: (err: Error) => void;
}

export class RosNode extends EventEmitter<RosNodeEvents> {
  readonly name: string;
  readonly hostname: string;
  readonly pid: number;

  rosMasterClient: RosMasterClient;
  rosParamClient: RosParamClient;
  rosFollower: RosFollower;
  subscriptions = new Map<string, Subscription>();
  publications = new Map<string, Publication>();
  parameters = new Map<string, XmlRpcValue>();

  #running = true;
  #tcpSocketCreate: TcpSocketCreate;
  #connectionIdCounter = 0;
  #tcpPublisher?: TcpPublisher;
  #localApiUrl?: string;
  #log?: LoggerService;

  constructor(options: {
    name: string;
    hostname: string;
    pid: number;
    rosMasterUri: string;
    httpServer: HttpServer;
    tcpSocketCreate: TcpSocketCreate;
    tcpServer?: TcpServer;
    log?: LoggerService;
  }) {
    super();
    this.name = options.name;
    this.hostname = options.hostname;
    this.pid = options.pid;
    this.rosMasterClient = new RosMasterClient(options.rosMasterUri);
    this.rosParamClient = new RosParamClient(options.rosMasterUri);
    this.rosFollower = new RosFollower(this, options.httpServer);
    this.#tcpSocketCreate = options.tcpSocketCreate;
    if (options.tcpServer != undefined) {
      this.#tcpPublisher = new TcpPublisher({
        server: options.tcpServer,
        nodeName: this.name,
        getConnectionId: this.#newConnectionId,
        getPublication: this.#getPublication,
        log: options.log,
      });
      this.#tcpPublisher.on("connection", this.#handleTcpClientConnection);
      this.#tcpPublisher.on("error", this.#handleTcpPublisherError);
    }
    this.#log = options.log;

    this.rosFollower.on("paramUpdate", this.#handleParamUpdate);
    this.rosFollower.on("publisherUpdate", this.#handlePublisherUpdate);
  }

  async start(port?: number): Promise<void> {
    return await this.rosFollower
      .start(this.hostname, port)
      .then(() => this.#log?.debug?.(`rosfollower listening at ${this.rosFollower.url()}`));
  }

  shutdown(_msg?: string): void {
    this.#log?.debug?.("shutting down");
    this.#running = false;
    this.#tcpPublisher?.close();
    this.rosFollower.close();

    if (this.parameters.size > 0) {
      this.unsubscribeAllParams().catch((unk) => {
        const err = unk instanceof Error ? unk : new Error(unk as string);
        this.#log?.warn?.(err.message, "shutdown");
        this.emit("error", err);
      });
    }

    for (const subTopic of Array.from(this.subscriptions.keys())) {
      this.unsubscribe(subTopic);
    }

    for (const pubTopic of Array.from(this.publications.keys())) {
      this.unadvertise(pubTopic);
    }

    this.subscriptions.clear();
    this.publications.clear();
    this.parameters.clear();
  }

  subscribe(options: SubscribeOpts): Subscription {
    const { topic, dataType } = options;
    const md5sum = options.md5sum ?? "*";
    const tcpNoDelay = options.tcpNoDelay ?? false;

    // Check if we are already subscribed
    let subscription = this.subscriptions.get(topic);
    if (subscription != undefined) {
      this.#log?.debug?.(`reusing existing subscribtion to ${topic} (${dataType})`);
      return subscription;
    }

    subscription = new Subscription({ name: topic, md5sum, dataType, tcpNoDelay });
    this.subscriptions.set(topic, subscription);

    this.#log?.debug?.(`subscribing to ${topic} (${dataType})`);

    // Asynchronously register this subscription with rosmaster and connect to
    // each publisher
    this.#registerSubscriberAndConnect(subscription).catch((err) => {
      // This should never be called, this._registerSubscriberAndConnect() is not expected to throw
      this.#log?.warn?.(
        `subscribe registration and connection unexpectedly failed: ${err}`,
        "subscribe",
      );
    });

    return subscription;
  }

  async advertise(options: PublishOpts): Promise<Publication> {
    const { topic, dataType } = options;

    const addr = await this.tcpServerAddress();
    if (addr == undefined) {
      throw new Error(`Cannot publish ${topic} without a listening tcpServer`);
    }

    // Check if we are already publishing
    let publication = this.publications.get(topic);
    if (publication != undefined) {
      this.#log?.debug?.(`reusing existing publication for ${topic} (${dataType})`);
      return publication;
    }

    const messageDefinition =
      options.messageDefinition ?? parseMessageDefinition(options.messageDefinitionText ?? "");
    const canonicalMsgDefText = rosMsgDefinitionText(messageDefinition);
    const messageDefinitionText = options.messageDefinitionText ?? canonicalMsgDefText;
    const md5sum = options.md5sum ?? rosMsgMd5sum(messageDefinition);
    const messageWriter = new MessageWriter(messageDefinition);

    publication = new Publication(
      topic,
      md5sum,
      dataType,
      options.latching ?? false,
      messageDefinition,
      messageDefinitionText,
      messageWriter,
    );
    this.publications.set(topic, publication);

    this.#log?.debug?.(`publishing ${topic} (${dataType})`);

    // Register with with rosmaster as a publisher for the requested topic. If
    // this request fails, an exception is thrown
    const subscribers = await this.#registerPublisher(publication);

    this.#log?.info?.(
      `registered as a publisher for ${topic}, ${subscribers.length} current subscriber(s)`,
    );

    return publication;
  }

  async publish(topic: string, message: unknown): Promise<void> {
    if (this.#tcpPublisher == undefined) {
      throw new Error(`Cannot publish without a tcpServer`);
    }

    const publication = this.publications.get(topic);
    if (publication == undefined) {
      throw new Error(`Cannot publish to unadvertised topic "${topic}"`);
    }

    await this.#tcpPublisher.publish(publication, message);
  }

  isSubscribedTo(topic: string): boolean {
    return this.#running && this.subscriptions.has(topic);
  }

  isAdvertising(topic: string): boolean {
    return this.#running && this.publications.has(topic);
  }

  unsubscribe(topic: string): boolean {
    const subscription = this.subscriptions.get(topic);
    if (subscription == null) {
      return false;
    }

    this.#unregisterSubscriber(topic).catch((err) => {
      // This should never happen
      this.#log?.warn?.(`unregisterSubscriber failed for ${topic}: ${err}`);
    });

    subscription.close();
    this.subscriptions.delete(topic);
    return true;
  }

  unadvertise(topic: string): boolean {
    const publication = this.publications.get(topic);
    if (publication == null) {
      return false;
    }

    this.#unregisterPublisher(topic).catch((err) => {
      // This should never happen
      this.#log?.warn?.(`_unregisterPublisher failed for ${topic}: ${err}`);
    });

    publication.close();
    this.publications.delete(topic);
    return true;
  }

  async getParamNames(): Promise<string[]> {
    const [status, msg, names] = await this.rosParamClient.getParamNames(this.name);
    if (status !== OK) {
      throw new Error(`getParamNames returned failure (status=${status}): ${msg}`);
    }
    if (!Array.isArray(names)) {
      throw new Error(`getParamNames returned unrecognized data (${msg})`);
    }
    return names as string[];
  }

  async setParameter(key: string, value: XmlRpcValue): Promise<void> {
    const [status, msg] = await this.rosParamClient.setParam(this.name, key, value);
    if (status !== OK) {
      throw new Error(`setParam returned failure (status=${status}): ${msg}`);
    }

    // Also do a local update because ROS param server won't notify us if
    // we initiated the parameter update.
    this.#handleParamUpdate(key, value, this.name);
  }

  async subscribeParam(key: string): Promise<XmlRpcValue> {
    const callerApi = this.#callerApi();
    const [status, msg, value] = await this.rosParamClient.subscribeParam(
      this.name,
      callerApi,
      key,
    );
    if (status !== OK) {
      throw new Error(`subscribeParam returned failure (status=${status}): ${msg}`);
    }
    // rosparam server returns an empty object ({}) if the parameter has not been set yet
    const adjustedValue = isEmptyPlainObject(value) ? undefined : value;
    this.parameters.set(key, adjustedValue);

    this.#log?.debug?.(`subscribed ${callerApi} to param "${key}" (${String(adjustedValue)})`);
    return adjustedValue;
  }

  async unsubscribeParam(key: string): Promise<boolean> {
    const callerApi = this.#callerApi();
    const [status, msg, value] = await this.rosParamClient.unsubscribeParam(
      this.name,
      callerApi,
      key,
    );
    if (status !== OK) {
      throw new Error(`unsubscribeParam returned failure (status=${status}): ${msg}`);
    }
    this.parameters.delete(key);
    const didUnsubscribe = (value as number) === 1;

    this.#log?.debug?.(
      `unsubscribed ${callerApi} from param "${key}" (didUnsubscribe=${didUnsubscribe})`,
    );
    return didUnsubscribe;
  }

  async subscribeAllParams(): Promise<Readonly<Map<string, XmlRpcValue>>> {
    let keys = await this.getParamNames();
    const curKeys = Array.from(this.parameters.keys());
    const callerApi = this.#callerApi();

    // Remove any local parameters the rosparam server didn't return
    const removedKeys = difference(curKeys, keys);
    if (removedKeys.length > 0) {
      this.#log?.debug?.(`removing missing parameters ${JSON.stringify(removedKeys)}`);
      for (const key of removedKeys) {
        this.parameters.delete(key);
      }
    }

    // Check if there are any parameters we don't already have locally
    keys = difference(keys, curKeys);
    if (keys.length === 0) {
      return this.parameters;
    }

    const res = await this.rosParamClient.subscribeParams(this.name, callerApi, keys);
    if (res.length !== keys.length) {
      throw new Error(
        `subscribeAllParams returned ${res.length} entries, expected ${
          keys.length
        }: ${JSON.stringify(res)}`,
      );
    }

    // Update the local map of all subscribed parameters
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i]!;
      const entry = res[i];
      if (entry instanceof XmlRpcFault) {
        const entryJson = JSON.stringify(entry);
        this.#log?.warn?.(`subscribeAllParams faulted on "${key}" (${entryJson})`);
        this.emit("error", new Error(`subscribeAllParams faulted on "${key}" (${entryJson})`));
        continue;
      }
      const [status, msg, value] = entry!;
      if (status !== OK) {
        this.#log?.warn?.(`subscribeAllParams not ok for "${key}" (status=${status}): ${msg}`);
        this.emit(
          "error",
          new Error(`subscribeAllParams not ok for "${key}" (status=${status}): ${msg}`),
        );
        continue;
      }
      // rosparam server returns an empty object ({}) if the parameter has not been set yet
      const adjustedValue = isEmptyPlainObject(value) ? undefined : value;
      this.parameters.set(key, adjustedValue);
    }

    this.#log?.debug?.(`subscribed ${callerApi} to parameters (${keys})`);
    return this.parameters;
  }

  async unsubscribeAllParams(): Promise<void> {
    const keys = Array.from(this.parameters.keys());
    const callerApi = this.#callerApi();
    const res = await this.rosParamClient.unsubscribeParams(this.name, callerApi, keys);
    if (res.length !== keys.length) {
      throw new Error(`unsubscribeAllParams returned unrecognized data: ${JSON.stringify(res)}`);
    }
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i]!;
      const [status, msg] = res[i] as RosXmlRpcResponse;
      if (status !== OK) {
        this.#log?.warn?.(`unsubscribeAllParams failed for "${key}" (status=${status}): ${msg}`);
        this.emit(
          "error",
          new Error(`unsubscribeAllParams failed for "${key}" (status=${status}): ${msg}`),
        );
        continue;
      }
    }

    this.#log?.debug?.(`unsubscribed ${callerApi} from all parameters (${String(keys)})`);
    this.parameters.clear();
  }

  async getPublishedTopics(subgraph?: string): Promise<[topic: string, dataType: string][]> {
    const [status, msg, topicsAndTypes] = await this.rosMasterClient.getPublishedTopics(
      this.name,
      subgraph,
    );
    if (status !== OK) {
      throw new Error(`getPublishedTopics returned failure (status=${status}): ${msg}`);
    }
    return topicsAndTypes as [string, string][];
  }

  async getSystemState(): Promise<RosGraph> {
    const [status, msg, systemState] = await this.rosMasterClient.getSystemState(this.name);
    if (status !== OK) {
      throw new Error(`getPublishedTopics returned failure (status=${status}): ${msg}`);
    }
    if (!Array.isArray(systemState) || systemState.length !== 3) {
      throw new Error(`getPublishedTopics returned unrecognized data (${msg})`);
    }

    // Each of these has the form [ [topic, [node1...nodeN]] ... ]
    type SystemStateEntry = [topic: string, nodes: string[]];
    type SystemStateResponse = [SystemStateEntry[], SystemStateEntry[], SystemStateEntry[]];
    const [pubs, subs, srvs] = systemState as SystemStateResponse;

    const createMap = (entries: SystemStateEntry[]) =>
      new Map<string, Set<string>>(entries.map(([topic, nodes]) => [topic, new Set(nodes)]));

    return {
      publishers: createMap(pubs),
      subscribers: createMap(subs),
      services: createMap(srvs),
    };
  }

  async tcpServerAddress(): Promise<TcpAddress | undefined> {
    return await this.#tcpPublisher?.address();
  }

  receivedBytes(): number {
    let bytes = 0;
    for (const sub of this.subscriptions.values()) {
      bytes += sub.receivedBytes();
    }
    return bytes;
  }

  static async RequestTopic(
    name: string,
    topic: string,
    apiClient: RosFollowerClient,
  ): Promise<{ address: string; port: number }> {
    let res: RosXmlRpcResponse;
    try {
      res = await apiClient.requestTopic(name, topic, [["TCPROS"]]);
    } catch (err) {
      throw new Error(`requestTopic("${topic}") from ${apiClient.url()} failed. err=${err}`);
    }
    const [status, msg, protocol] = res;

    if (status !== OK) {
      throw new Error(
        `requestTopic("${topic}") from ${apiClient.url()} failed. status=${status}, msg=${msg}`,
      );
    }
    if (!Array.isArray(protocol) || protocol.length < 3 || protocol[0] !== "TCPROS") {
      throw new Error(`TCP not supported by ${apiClient.url()} for topic "${topic}"`);
    }

    return { port: protocol[2] as number, address: protocol[1] as string };
  }

  #newConnectionId = (): number => {
    return this.#connectionIdCounter++;
  };

  #getPublication = (topic: string): Publication | undefined => {
    return this.publications.get(topic);
  };

  #callerApi(): string {
    if (this.#localApiUrl != undefined) {
      return this.#localApiUrl;
    }

    this.#localApiUrl = this.rosFollower.url();
    if (this.#localApiUrl == undefined) {
      throw new Error("Local XMLRPC server was not started");
    }

    return this.#localApiUrl;
  }

  #handleParamUpdate = (key: string, value: XmlRpcValue, callerId: string) => {
    const prevValue = this.parameters.get(key);
    this.parameters.set(key, value);
    this.emit("paramUpdate", { key, value, prevValue, callerId });
  };

  #handlePublisherUpdate = (topic: string, publishers: string[], callerId: string) => {
    const sub = this.subscriptions.get(topic);
    if (sub == undefined) {
      return;
    }

    const prevPublishers = Array.from(sub.publishers().values()).map((v) => v.publisherXmlRpcUrl());
    const removed = difference(prevPublishers, publishers);
    const added = difference(publishers, prevPublishers);

    // Remove all publishers that have disappeared
    for (const removePub of removed) {
      for (const [connectionId, pub] of sub.publishers().entries()) {
        if (pub.publisherXmlRpcUrl() === removePub) {
          this.#log?.info?.(`publisher ${removePub} for ${sub.name} went offline, disconnecting`);
          sub.removePublisher(connectionId);
          break;
        }
      }
    }

    // Add any new publishers that have appeared
    for (const addPub of added) {
      this.#log?.info?.(`publisher ${addPub} for ${sub.name} came online, connecting`);
      this.#subscribeToPublisher(addPub, sub).catch((err) => {
        // This should never be called, this._subscribeToPublisher() is not expected to throw
        this.#log?.warn?.(`subscribe to publisher unexpectedly failed: ${err}`);
      });
    }

    this.emit("publisherUpdate", { topic, publishers, prevPublishers, callerId });
  };

  #handleTcpClientConnection = (
    topic: string,
    connectionId: number,
    destinationCallerId: string,
    client: Client,
  ) => {
    const publication = this.publications.get(topic);
    if (publication == undefined) {
      this.#log?.warn?.(`${client.toString()} connected to non-published topic ${topic}`);
      this.emit(
        "error",
        new Error(`${client.toString()} connected to non-published topic ${topic}`),
      );
      client.close();
      return;
    }

    this.#log?.info?.(
      `adding subscriber ${client.toString()} (${destinationCallerId}) to topic ${topic}, connectionId ${connectionId}`,
    );
    publication.addSubscriber(connectionId, destinationCallerId, client);
  };

  #handleTcpPublisherError = (err: Error) => {
    this.emit("error", err);
  };

  async #registerSubscriber(subscription: Subscription): Promise<string[]> {
    if (!this.#running) {
      return [];
    }

    const callerApi = this.#callerApi();

    // Register with rosmaster as a subscriber to this topic
    const [status, msg, publishers] = await this.rosMasterClient.registerSubscriber(
      this.name,
      subscription.name,
      subscription.dataType,
      callerApi,
    );

    if (status !== OK) {
      throw new Error(`registerSubscriber() failed. status=${status}, msg="${msg}"`);
    }

    if (!Array.isArray(publishers)) {
      throw new Error(
        `registerSubscriber() did not receive a list of publishers. value=${publishers}`,
      );
    }

    this.#log?.debug?.(`registered subscriber to ${subscription.name} (${subscription.dataType})`);
    return publishers as string[];
  }

  async #registerPublisher(publication: Publication): Promise<string[]> {
    if (!this.#running) {
      return [];
    }

    const callerApi = this.#callerApi();

    const [status, msg, subscribers] = await this.rosMasterClient.registerPublisher(
      this.name,
      publication.name,
      publication.dataType,
      callerApi,
    );

    if (status !== OK) {
      throw new Error(`registerPublisher() failed. status=${status}, msg="${msg}"`);
    }

    this.#log?.debug?.(`registered publisher for ${publication.name} (${publication.dataType})`);
    if (!Array.isArray(subscribers)) {
      throw new Error(
        `registerPublisher() did not receive a list of subscribers. value=${String(subscribers)}`,
      );
    }

    return subscribers as string[];
  }

  async #unregisterSubscriber(topic: string): Promise<void> {
    try {
      const callerApi = this.#callerApi();

      // Unregister with rosmaster as a subscriber to this topic
      const [status, msg] = await this.rosMasterClient.unregisterSubscriber(
        this.name,
        topic,
        callerApi,
      );

      if (status !== OK) {
        throw new Error(`unregisterSubscriber() failed. status=${status}, msg="${msg}"`);
      }

      this.#log?.debug?.(`unregistered subscriber to ${topic}`);
    } catch (unk) {
      // Warn and carry on, the rosmaster graph will be out of sync but there's
      // not much we can do (it may already be offline)
      const err = unk instanceof Error ? unk : new Error(unk as string);
      this.#log?.warn?.(err.message, "unregisterSubscriber");
      this.emit("error", err);
    }
  }

  async #unregisterPublisher(topic: string): Promise<void> {
    try {
      const callerApi = this.#callerApi();

      // Unregister with rosmaster as a publisher of this topic
      const [status, msg] = await this.rosMasterClient.unregisterPublisher(
        this.name,
        topic,
        callerApi,
      );

      if (status !== OK) {
        throw new Error(`unregisterPublisher() failed. status=${status}, msg="${msg}"`);
      }

      this.#log?.debug?.(`unregistered publisher for ${topic}`);
    } catch (unk) {
      // Warn and carry on, the rosmaster graph will be out of sync but there's
      // not much we can do (it may already be offline)
      const err = unk instanceof Error ? unk : new Error(unk as string);
      this.#log?.warn?.(err.message, "unregisterPublisher");
      this.emit("error", err);
    }
  }

  async #registerSubscriberAndConnect(subscription: Subscription): Promise<void> {
    // Register with rosmaster as a subscriber to the requested topic. Continue
    // retrying until the XML-RPC call to roscore succeeds, or we are no longer
    // subscribed
    const publishers = await retryForever(async () => {
      if (!this.isSubscribedTo(subscription.name)) {
        return [];
      }
      return await this.#registerSubscriber(subscription);
    });

    // Register with each publisher. Any failures communicating with individual node XML-RPC servers
    // or TCP sockets will be caught and retried
    await Promise.allSettled(
      publishers.map(async (pubUrl) => {
        await this.#subscribeToPublisher(pubUrl, subscription);
      }),
    );
  }

  async #subscribeToPublisher(pubUrl: string, subscription: Subscription): Promise<void> {
    const topic = subscription.name;
    const dataType = subscription.dataType;
    const md5sum = subscription.md5sum;
    const tcpNoDelay = subscription.tcpNoDelay;

    if (!this.isSubscribedTo(topic)) {
      return;
    }

    let connection: TcpConnection;
    let address: string;
    let port: number;

    try {
      // Create an XMLRPC client to talk to this publisher
      const rosFollowerClient = new RosFollowerClient(pubUrl);

      // Call requestTopic on this publisher to register ourselves as a subscriber
      const socketInfo = await RosNode.RequestTopic(this.name, topic, rosFollowerClient);
      ({ address, port } = socketInfo);
      const uri = TcpConnection.Uri(address, port);
      this.#log?.debug?.(
        `registered with ${pubUrl} as a subscriber to ${topic}, connecting to ${uri}`,
      );

      if (!this.isSubscribedTo(topic)) {
        return;
      }

      // Create a TCP socket connecting to this publisher
      const socket = await this.#tcpSocketCreate({ host: address, port });
      connection = new TcpConnection(
        socket,
        address,
        port,
        new Map<string, string>([
          ["topic", topic],
          [
            "md5sum",
            md5sum ?? "*" /* eslint-disable-line @typescript-eslint/no-unnecessary-condition */,
          ],
          ["callerid", this.name],
          ["type", dataType],
          ["tcp_nodelay", tcpNoDelay ? "1" : "0"],
        ]),
        this.#log,
      );

      if (!this.isSubscribedTo(topic)) {
        socket.close().catch((err) => {
          this.#log?.warn?.(
            `closing connection to ${address}:${port} for topic "${topic}" failed: ${err}`,
          );
        });
        return;
      }

      // Hold a reference to this publisher
      const connectionId = this.#newConnectionId();
      subscription.addPublisher(connectionId, rosFollowerClient, connection);
    } catch (err) {
      // Consider tracking failed RosFollower connections (node XML-RPC servers) and entering a
      // retry loop
      this.#log?.warn?.(
        `subscribing to ${topic} at ${pubUrl} failed (${err}), this connection will be dropped`,
      );
      this.emit(
        "error",
        new Error(
          `Subscribing to ${topic} at ${pubUrl} failed (${err}), this connection will be dropped`,
        ),
      );
      return;
    }

    // Asynchronously initiate the socket connection. This will enter a retry loop on failure
    connection.connect().catch((err) => {
      // This should never happen, connect() is assumed not to throw
      this.#log?.warn?.(
        `connecting to ${address}:${port} for topic "${topic}" unexpectedly failed: ${err}`,
      );
    });
  }

  static GetRosHostname(
    getEnvVar: (envVar: string) => string | undefined,
    getHostname: () => string | undefined,
    getNetworkInterfaces: () => NetworkInterface[],
  ): string {
    // Prefer ROS_HOSTNAME, then ROS_IP env vars
    let hostname = getEnvVar("ROS_HOSTNAME") ?? getEnvVar("ROS_IP");
    if (hostname != undefined && hostname.length > 0) {
      return hostname;
    }

    // Try to get the operating system hostname
    hostname = getHostname();
    if (hostname != undefined && hostname.length > 0) {
      return hostname;
    }

    // Fall back to iterating network interfaces looking for an IP address
    let bestAddr: NetworkInterface | undefined;
    const ifaces = getNetworkInterfaces();
    for (const iface of ifaces) {
      if (
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
        (iface.family !== "IPv4" && iface.family !== "IPv6") ||
        iface.internal ||
        iface.address.length === 0
      ) {
        continue;
      }

      if (bestAddr == undefined) {
        // Use the first non-internal interface we find
        bestAddr = iface;
      } else if (RosNode.IsPrivateIP(bestAddr.address) && !RosNode.IsPrivateIP(iface.address)) {
        // Prefer public IPs over private
        bestAddr = iface;
      } else if (bestAddr.family !== "IPv6" && iface.family === "IPv6") {
        // Prefer IPv6
        bestAddr = iface;
      }
    }
    if (bestAddr != undefined) {
      return bestAddr.address;
    }

    // Last resort, return IPv4 loopback
    return "127.0.0.1";
  }

  static IsPrivateIP(ip: string): boolean {
    // Logic based on isPrivateIP() in ros_comm network.cpp
    return ip.startsWith("192.168") || ip.startsWith("10.") || ip.startsWith("169.254");
  }
}
