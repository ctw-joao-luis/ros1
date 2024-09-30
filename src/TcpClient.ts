import EventEmitter from "eventemitter3";

import { Client, ClientStats } from "./Client";
import { LoggerService } from "./LoggerService";
import { Publication } from "./Publication";
import { RosTcpMessageStream } from "./RosTcpMessageStream";
import { TcpConnection } from "./TcpConnection";
import { TcpSocket } from "./TcpTypes";

export type PublicationLookup = (topic: string) => Publication | undefined;

type TcpClientOpts = {
  socket: TcpSocket;
  nodeName: string;
  getPublication: PublicationLookup;
  log?: LoggerService;
};

export interface TcpClientEvents {
  close: () => void;
  subscribe: (topic: string, destinationCallerId: string) => void;
  error: (err: Error) => void;
}

export class TcpClient extends EventEmitter<TcpClientEvents> implements Client {
  #socket: TcpSocket;
  #address?: string;
  #port?: number;
  #transportInfo: string;
  #nodeName: string;
  #connected = true;
  #receivedHeader = false;
  #stats: ClientStats = { bytesSent: 0, bytesReceived: 0, messagesSent: 0 };
  #getPublication: PublicationLookup;
  #log?: LoggerService;
  #transformer: RosTcpMessageStream;

  constructor({ socket, nodeName, getPublication, log }: TcpClientOpts) {
    super();
    this.#socket = socket;
    this.#nodeName = nodeName;
    this.#getPublication = getPublication;
    this.#log = log;
    this.#transformer = new RosTcpMessageStream();
    this.#transportInfo = `TCPROS establishing connection`;

    socket.on("close", this.#handleClose);
    socket.on("error", this.#handleError);
    socket.on("data", this.#handleData);

    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    this.#transformer.on("message", this.#handleMessage);

    void this.#updateTransportInfo();

    // Wait for the client to send the initial connection header
  }

  transportType(): string {
    return "TCPROS";
  }

  connected(): boolean {
    return this.#connected;
  }

  stats(): ClientStats {
    return this.#stats;
  }

  async write(data: Uint8Array): Promise<void> {
    try {
      await this.#socket.write(data);
      this.#stats.messagesSent++;
      this.#stats.bytesSent += data.length;
    } catch (err) {
      this.#log?.warn?.(`failed to write ${data.length} bytes to ${this.toString()}: ${err}`);
    }
  }

  close(): void {
    this.#socket
      .close()
      .catch((err) => this.#log?.warn?.(`error closing client socket ${this.toString()}: ${err}`));
  }

  getTransportInfo(): string {
    return this.#transportInfo;
  }

  override toString(): string {
    return TcpConnection.Uri(this.#address ?? "<unknown>", this.#port ?? 0);
  }

  #updateTransportInfo = async (): Promise<void> => {
    let fd = -1;
    try {
      fd = (await this.#socket.fd()) ?? -1;
      const localPort = (await this.#socket.localAddress())?.port ?? -1;
      const addr = await this.#socket.remoteAddress();
      if (addr == undefined) {
        throw new Error(`Socket ${fd} on local port ${localPort} could not resolve remoteAddress`);
      }

      const { address, port } = addr;
      const host = address.includes(":") ? `[${address}]` : address;
      this.#address = address;
      this.#port = port;
      this.#transportInfo = `TCPROS connection on port ${localPort} to [${host}:${port} on socket ${fd}]`;
    } catch (err) {
      this.#transportInfo = `TCPROS not connected [socket ${fd}]`;
      this.#log?.warn?.(`Cannot resolve address for tcp connection: ${err}`);
      this.emit("error", new Error(`Cannot resolve address for tcp connection: ${err}`));
    }
  };

  async #writeHeader(header: Map<string, string>): Promise<void> {
    const data = TcpConnection.SerializeHeader(header);

    // Write the serialized header payload
    const buffer = new ArrayBuffer(4 + data.length);
    const payload = new Uint8Array(buffer);
    const view = new DataView(buffer);
    view.setUint32(0, data.length, true);
    payload.set(data, 4);

    try {
      await this.#socket.write(payload);
      this.#stats.bytesSent += payload.length;
    } catch (err) {
      this.#log?.warn?.(
        `failed to write ${data.length + 4} byte header to ${this.toString()}: ${err}`,
      );
    }
  }

  #handleClose = () => {
    this.#connected = false;
    this.emit("close");
  };

  #handleError = (err: Error) => {
    this.#log?.warn?.(`tcp client ${this.toString()} error: ${err}`);
    this.emit("error", err);
  };

  #handleData = (chunk: Uint8Array) => {
    try {
      this.#transformer.addData(chunk);
    } catch (unk) {
      const err = unk instanceof Error ? unk : new Error(unk as string);
      this.#log?.warn?.(
        `failed to decode ${chunk.length} byte chunk from tcp client ${this.toString()}: ${err}`,
      );
      // Close the socket, the stream is now corrupt
      void this.#socket.close();
      this.emit("error", err);
    }
  };

  #handleMessage = async (msgData: Uint8Array) => {
    // Check if we have already received the connection header from this client
    if (this.#receivedHeader) {
      this.#log?.warn?.(`tcp client ${this.toString()} sent ${msgData.length} bytes after header`);
      this.#stats.bytesReceived += msgData.byteLength;
      return;
    }

    const header = TcpConnection.ParseHeader(msgData);
    const topic = header.get("topic");
    const destinationCallerId = header.get("callerid");
    const dataType = header.get("type");
    const md5sum = header.get("md5sum") ?? "*";
    const tcpNoDelay = header.get("tcp_nodelay") === "1";

    this.#receivedHeader = true;

    void this.#socket.setNoDelay(tcpNoDelay);

    if (topic == undefined || dataType == undefined || destinationCallerId == undefined) {
      this.#log?.warn?.(
        `tcp client ${this.toString()} sent incomplete header. topic="${topic}", type="${dataType}", callerid="${destinationCallerId}"`,
      );
      this.close();
      return;
    }

    // Check if we are publishing this topic
    const pub = this.#getPublication(topic);
    if (pub == undefined) {
      this.#log?.warn?.(
        `tcp client ${this.toString()} attempted to subscribe to unadvertised topic ${topic}`,
      );
      this.close();
      return;
    }

    this.#stats.bytesReceived += msgData.byteLength;

    // Check the dataType matches
    if (dataType !== "*" && pub.dataType !== dataType) {
      this.#log?.warn?.(
        `tcp client ${this.toString()} attempted to subscribe to topic ${topic} with type "${dataType}", expected "${
          pub.dataType
        }"`,
      );
      this.close();
      return;
    }

    // Check the md5sum matches
    if (md5sum !== "*" && pub.md5sum !== md5sum) {
      this.#log?.warn?.(
        `tcp client ${this.toString()} attempted to subscribe to topic ${topic} with md5sum "${md5sum}", expected "${
          pub.md5sum
        }"`,
      );
      this.close();
      return;
    }

    // Write the response header
    void this.#writeHeader(
      new Map<string, string>([
        ["callerid", this.#nodeName],
        ["latching", pub.latching ? "1" : "0"],
        ["md5sum", pub.md5sum],
        ["message_definition", pub.messageDefinitionText],
        ["topic", pub.name],
        ["type", pub.dataType],
      ]),
    );

    // Immediately send the last published message if latching is enabled
    const latched = pub.latchedMessage(this.transportType());
    if (latched != undefined) {
      void this.write(latched);
    }

    this.emit("subscribe", topic, destinationCallerId);
  };
}
