import { HttpServer, XmlRpcServer, XmlRpcValue } from "@lichtblick/xmlrpc";

import { LoggerService } from "./LoggerService";
import { RosFollowerClient } from "./RosFollowerClient";
import { RosXmlRpcResponse } from "./XmlRpcTypes";
import { isPlainObject } from "./objectTests";

function CheckArguments(args: XmlRpcValue[], expected: string[]): Error | undefined {
  if (args.length !== expected.length) {
    return new Error(`Expected ${expected.length} arguments, got ${args.length}`);
  }

  for (let i = 0; i < args.length; i++) {
    if (expected[i] !== "*" && typeof args[i] !== expected[i]) {
      return new Error(`Expected "${expected[i]}" for arg ${i}, got "${typeof args[i]}"`);
    }
  }

  return undefined;
}

// A server implementing the <http://wiki.ros.org/ROS/Master_API> and
// <http://wiki.ros.org/ROS/Parameter%20Server%20API> APIs. This can be used as
// an alternative server implementation than roscore provided by the ros_comm
// library.
export class RosMaster {
  #server: XmlRpcServer;
  #log?: LoggerService;
  #url?: string;

  #nodes = new Map<string, string>();
  #services = new Map<string, Map<string, string>>();
  #topics = new Map<string, string>();
  #publications = new Map<string, Set<string>>();
  #subscriptions = new Map<string, Set<string>>();

  #parameters = new Map<string, XmlRpcValue>();
  #paramSubscriptions = new Map<string, Map<string, string>>();

  constructor(httpServer: HttpServer, log?: LoggerService) {
    this.#server = new XmlRpcServer(httpServer);
    this.#log = log;
  }

  async start(hostname: string, port?: number): Promise<void> {
    await this.#server.listen(port, undefined, 10);
    this.#url = `http://${hostname}:${this.#server.port()}/`;

    this.#server.setHandler("registerService", this.registerService);
    this.#server.setHandler("unregisterService", this.unregisterService);
    this.#server.setHandler("registerSubscriber", this.registerSubscriber);
    this.#server.setHandler("unregisterSubscriber", this.unregisterSubscriber);
    this.#server.setHandler("registerPublisher", this.registerPublisher);
    this.#server.setHandler("unregisterPublisher", this.unregisterPublisher);
    this.#server.setHandler("lookupNode", this.lookupNode);
    this.#server.setHandler("getPublishedTopics", this.getPublishedTopics);
    this.#server.setHandler("getTopicTypes", this.getTopicTypes);
    this.#server.setHandler("getSystemState", this.getSystemState);
    this.#server.setHandler("getUri", this.getUri);
    this.#server.setHandler("lookupService", this.lookupService);
    this.#server.setHandler("deleteParam", this.deleteParam);
    this.#server.setHandler("setParam", this.setParam);
    this.#server.setHandler("getParam", this.getParam);
    this.#server.setHandler("searchParam", this.searchParam);
    this.#server.setHandler("subscribeParam", this.subscribeParam);
    this.#server.setHandler("unsubscribeParam", this.unsubscribeParam);
    this.#server.setHandler("hasParam", this.hasParam);
    this.#server.setHandler("getParamNames", this.getParamNames);
  }

  close(): void {
    this.#server.close();
  }

  url(): string | undefined {
    return this.#url;
  }

  // <http://wiki.ros.org/ROS/Master_API> handlers

  registerService = async (
    _methodName: string,
    args: XmlRpcValue[],
  ): Promise<RosXmlRpcResponse> => {
    // [callerId, service, serviceApi, callerApi]
    const err = CheckArguments(args, ["string", "string", "string", "string"]);
    if (err != null) {
      throw err;
    }

    const [callerId, service, serviceApi, callerApi] = args as [string, string, string, string];

    if (!this.#services.has(service)) {
      this.#services.set(service, new Map<string, string>());
    }
    const serviceProviders = this.#services.get(service)!;

    serviceProviders.set(callerId, serviceApi);
    this.#nodes.set(callerId, callerApi);

    return [1, "", 0];
  };

  unregisterService = async (
    _methodName: string,
    args: XmlRpcValue[],
  ): Promise<RosXmlRpcResponse> => {
    // [callerId, service, serviceApi]
    const err = CheckArguments(args, ["string", "string", "string"]);
    if (err != null) {
      throw err;
    }

    const [callerId, service, _serviceApi] = args as [string, string, string];
    const serviceProviders = this.#services.get(service);
    if (serviceProviders == undefined) {
      return [1, "", 0];
    }

    const removed = serviceProviders.delete(callerId);
    if (serviceProviders.size === 0) {
      this.#services.delete(service);
    }

    return [1, "", removed ? 1 : 0];
  };

  registerSubscriber = async (
    _methodName: string,
    args: XmlRpcValue[],
  ): Promise<RosXmlRpcResponse> => {
    // [callerId, topic, topicType, callerApi]
    const err = CheckArguments(args, ["string", "string", "string", "string"]);
    if (err != null) {
      throw err;
    }

    const [callerId, topic, topicType, callerApi] = args as [string, string, string, string];

    const dataType = this.#topics.get(topic);
    if (dataType != undefined && dataType !== topicType) {
      return [0, `topic_type "${topicType}" for topic "${topic}" does not match "${dataType}"`, []];
    }

    if (!this.#subscriptions.has(topic)) {
      this.#subscriptions.set(topic, new Set<string>());
    }
    const subscribers = this.#subscriptions.get(topic)!;
    subscribers.add(callerId);

    this.#nodes.set(callerId, callerApi);

    const publishers = Array.from((this.#publications.get(topic) ?? new Set<string>()).values());
    const publisherApis = publishers
      .map((p) => this.#nodes.get(p))
      .filter((a) => a != undefined) as string[];
    return [1, "", publisherApis];
  };

  unregisterSubscriber = async (
    _methodName: string,
    args: XmlRpcValue[],
  ): Promise<RosXmlRpcResponse> => {
    // [callerId, topic, callerApi]
    const err = CheckArguments(args, ["string", "string", "string"]);
    if (err != null) {
      throw err;
    }

    const [callerId, topic, _callerApi] = args as [string, string, string];

    const subscribers = this.#subscriptions.get(topic);
    if (subscribers == undefined) {
      return [1, "", 0];
    }

    const removed = subscribers.delete(callerId);
    if (subscribers.size === 0) {
      this.#subscriptions.delete(topic);
    }

    return [1, "", removed ? 1 : 0];
  };

  registerPublisher = async (
    _methodName: string,
    args: XmlRpcValue[],
  ): Promise<RosXmlRpcResponse> => {
    // [callerId, topic, topicType, callerApi]
    const err = CheckArguments(args, ["string", "string", "string", "string"]);
    if (err != null) {
      throw err;
    }

    const [callerId, topic, topicType, callerApi] = args as [string, string, string, string];

    const dataType = this.#topics.get(topic);
    if (dataType != undefined && dataType !== topicType) {
      return [0, `topic_type "${topicType}" for topic "${topic}" does not match "${dataType}"`, []];
    }

    if (!this.#publications.has(topic)) {
      this.#publications.set(topic, new Set<string>());
    }
    const publishers = this.#publications.get(topic)!;
    publishers.add(callerId);

    this.#topics.set(topic, topicType);
    this.#nodes.set(callerId, callerApi);

    const subscribers = Array.from((this.#subscriptions.get(topic) ?? new Set<string>()).values());
    const subscriberApis = subscribers
      .map((s) => this.#nodes.get(s))
      .filter((a) => a != undefined) as string[];

    // Inform all subscribers of the new publisher
    const publisherApis = Array.from(publishers.values())
      .sort()
      .map((p) => this.#nodes.get(p))
      .filter((a) => a != undefined) as string[];
    for (const api of subscriberApis) {
      new RosFollowerClient(api)
        .publisherUpdate(callerId, topic, publisherApis)
        .catch((apiErr) => this.#log?.warn?.(`publisherUpdate call to ${api} failed: ${apiErr}`));
    }

    return [1, "", subscriberApis];
  };

  unregisterPublisher = async (
    _methodName: string,
    args: XmlRpcValue[],
  ): Promise<RosXmlRpcResponse> => {
    // [callerId, topic, callerApi]
    const err = CheckArguments(args, ["string", "string", "string"]);
    if (err != null) {
      throw err;
    }

    const [callerId, topic, _callerApi] = args as [string, string, string];

    const publishers = this.#publications.get(topic);
    if (publishers == undefined) {
      return [1, "", 0];
    }

    const removed = publishers.delete(callerId);
    if (publishers.size === 0) {
      this.#publications.delete(topic);
    }

    return [1, "", removed ? 1 : 0];
  };

  lookupNode = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId, nodeName]
    const err = CheckArguments(args, ["string", "string"]);
    if (err != null) {
      throw err;
    }

    const [_callerId, nodeName] = args as [string, string];

    const nodeApi = this.#nodes.get(nodeName);
    if (nodeApi == undefined) {
      return [0, `node "${nodeName}" not found`, ""];
    }
    return [1, "", nodeApi];
  };

  getPublishedTopics = async (
    _methodName: string,
    args: XmlRpcValue[],
  ): Promise<RosXmlRpcResponse> => {
    // [callerId, subgraph]
    const err = CheckArguments(args, ["string", "string"]);
    if (err != null) {
      throw err;
    }

    // Subgraph filtering would need to be supported to become a fully compatible implementation
    const [_callerId, _subgraph] = args as [string, string];

    const entries: [string, string][] = [];
    for (const topic of this.#publications.keys()) {
      const dataType = this.#topics.get(topic);
      if (dataType != undefined) {
        entries.push([topic, dataType]);
      }
    }

    return [1, "", entries];
  };

  getTopicTypes = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId]
    const err = CheckArguments(args, ["string"]);
    if (err != null) {
      throw err;
    }

    const entries = Array.from(this.#topics.entries());
    return [1, "", entries];
  };

  getSystemState = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId]
    const err = CheckArguments(args, ["string"]);
    if (err != null) {
      throw err;
    }

    const publishers: [string, string[]][] = Array.from(this.#publications.entries()).map(
      ([topic, nodeNames]) => [topic, Array.from(nodeNames.values()).sort()],
    );

    const subscribers: [string, string[]][] = Array.from(this.#subscriptions.entries()).map(
      ([topic, nodeNames]) => [topic, Array.from(nodeNames.values()).sort()],
    );

    const services: [string, string[]][] = Array.from(this.#services.entries()).map(
      ([service, nodeNamesToServiceApis]) => [
        service,
        Array.from(nodeNamesToServiceApis.keys()).sort(),
      ],
    );

    return [1, "", [publishers, subscribers, services]];
  };

  getUri = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId]
    const err = CheckArguments(args, ["string"]);
    if (err != null) {
      throw err;
    }

    const url = this.#url;
    if (url == undefined) {
      return [0, "", "not running"];
    }

    return [1, "", url];
  };

  lookupService = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId, service]
    const err = CheckArguments(args, ["string", "string"]);
    if (err != null) {
      throw err;
    }

    const [_callerId, service] = args as [string, string];

    const serviceProviders = this.#services.get(service);
    if (serviceProviders == undefined || serviceProviders.size === 0) {
      return [0, `no providers for service "${service}"`, ""];
    }

    const serviceUrl = serviceProviders.values().next().value as string;
    return [1, "", serviceUrl];
  };

  // <http://wiki.ros.org/ROS/Parameter%20Server%20API> handlers

  deleteParam = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId, key]
    const err = CheckArguments(args, ["string", "string"]);
    if (err != null) {
      throw err;
    }

    const [_callerId, key] = args as [string, string];

    this.#parameters.delete(key);

    return [1, "", 0];
  };

  setParam = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId, key, value]
    const err = CheckArguments(args, ["string", "string", "*"]);
    if (err != null) {
      throw err;
    }

    const [callerId, key, value] = args as [string, string, XmlRpcValue];
    const allKeyValues: [string, XmlRpcValue][] = isPlainObject(value)
      ? objectToKeyValues(key, value as Record<string, XmlRpcValue>)
      : [[key, value]];

    for (const [curKey, curValue] of allKeyValues) {
      this.#parameters.set(curKey, curValue);

      // Notify any parameter subscribers about this new value
      const subscribers = this.#paramSubscriptions.get(curKey);
      if (subscribers != undefined) {
        for (const api of subscribers.values()) {
          new RosFollowerClient(api)
            .paramUpdate(callerId, curKey, curValue)
            .catch((apiErr) => this.#log?.warn?.(`paramUpdate call to ${api} failed: ${apiErr}`));
        }
      }
    }

    return [1, "", 0];
  };

  getParam = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId, key]
    const err = CheckArguments(args, ["string", "string"]);
    if (err != null) {
      throw err;
    }

    // This endpoint needs to support namespace retrieval to fully match the rosparam server
    // behavior
    const [_callerId, key] = args as [string, string];

    const value = this.#parameters.get(key);
    const status = value != undefined ? 1 : 0;
    return [status, "", value ?? {}];
  };

  searchParam = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId, key]
    const err = CheckArguments(args, ["string", "string"]);
    if (err != null) {
      throw err;
    }

    // This endpoint would have to take into account the callerId namespace, partial matching, and
    // returning undefined keys to fully match the rosparam server behavior
    const [_callerId, key] = args as [string, string];

    const value = this.#parameters.get(key);
    const status = value != undefined ? 1 : 0;
    return [status, "", value ?? {}];
  };

  subscribeParam = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId, callerApi, key]
    const err = CheckArguments(args, ["string", "string", "string"]);
    if (err != null) {
      throw err;
    }

    const [callerId, callerApi, key] = args as [string, string, string];

    if (!this.#paramSubscriptions.has(key)) {
      this.#paramSubscriptions.set(key, new Map<string, string>());
    }
    const subscriptions = this.#paramSubscriptions.get(key)!;

    subscriptions.set(callerId, callerApi);

    const value = this.#parameters.get(key) ?? {};
    return [1, "", value];
  };

  unsubscribeParam = async (
    _methodName: string,
    args: XmlRpcValue[],
  ): Promise<RosXmlRpcResponse> => {
    // [callerId, callerApi, key]
    const err = CheckArguments(args, ["string", "string", "string"]);
    if (err != null) {
      throw err;
    }

    const [callerId, _callerApi, key] = args as [string, string, string];

    const subscriptions = this.#paramSubscriptions.get(key);
    if (subscriptions == undefined) {
      return [1, "", 0];
    }

    const removed = subscriptions.delete(callerId);
    return [1, "", removed ? 1 : 0];
  };

  hasParam = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId, key]
    const err = CheckArguments(args, ["string", "string"]);
    if (err != null) {
      throw err;
    }

    const [_callerId, key] = args as [string, string];
    return [1, "", this.#parameters.has(key)];
  };

  getParamNames = async (_methodName: string, args: XmlRpcValue[]): Promise<RosXmlRpcResponse> => {
    // [callerId]
    const err = CheckArguments(args, ["string"]);
    if (err != null) {
      throw err;
    }

    const keys = Array.from(this.#parameters.keys()).sort();
    return [1, "", keys];
  };
}

function objectToKeyValues(
  prefix: string,
  object: Record<string, XmlRpcValue>,
): [string, XmlRpcValue][] {
  let entries: [string, XmlRpcValue][] = [];
  for (const curKey in object) {
    const key = `${prefix}/${curKey}`;
    const value = object[curKey];
    if (isPlainObject(value)) {
      entries = entries.concat(objectToKeyValues(key, value as Record<string, XmlRpcValue>));
    } else {
      entries.push([key, value]);
    }
  }
  return entries;
}
