import { MessageDefinition } from "@lichtblick/message-definition";
import { parse as parseMessageDefinition } from "@lichtblick/rosmsg";
import { MessageReader } from "@lichtblick/rosmsg-serialization";
import { EventEmitter } from "eventemitter3";

import { Connection, ConnectionStats } from "./Connection";
import { LoggerService } from "./LoggerService";
import { RosTcpMessageStream } from "./RosTcpMessageStream";
import { TcpAddress, TcpSocket } from "./TcpTypes";
import { backoff } from "./backoff";

export interface TcpConnectionEvents {
  header: (
    header: Map<string, string>,
    messageDefinition: MessageDefinition[],
    messageReader: MessageReader,
  ) => void;
  message: (msg: unknown, msgData: Uint8Array) => void;
  error: (err: Error) => void;
}

// Implements a subscriber for the TCPROS transport. The actual TCP transport is
// implemented in the passed in `socket` (TcpSocket). A transform stream is used
// internally for parsing the TCPROS message format (4 byte length followed by
// message payload) so "message" events represent one full message each without
// the length prefix. A transform class that meets this requirements is
// implemented in `RosTcpMessageStream`.
export class TcpConnection extends EventEmitter<TcpConnectionEvents> implements Connection {
  retries = 0;

  #socket: TcpSocket;
  #address: string;
  #port: number;
  #connected = false;
  #shutdown = false;
  #transportInfo = "TCPROS not connected [socket -1]";
  #readingHeader = true;
  #requestHeader: Map<string, string>;
  #header = new Map<string, string>();
  #stats = {
    bytesSent: 0,
    bytesReceived: 0,
    messagesSent: 0,
    messagesReceived: 0,
    dropEstimate: -1,
  };
  #transformer = new RosTcpMessageStream();
  #msgDefinition: MessageDefinition[] = [];
  #msgReader: MessageReader | undefined;
  #log?: LoggerService;

  constructor(
    socket: TcpSocket,
    address: string,
    port: number,
    requestHeader: Map<string, string>,
    log?: LoggerService,
  ) {
    super();
    this.#socket = socket;
    this.#address = address;
    this.#port = port;
    this.#requestHeader = requestHeader;
    this.#log = log;

    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    socket.on("connect", this.#handleConnect);
    socket.on("close", this.#handleClose);
    socket.on("error", this.#handleError);
    socket.on("data", this.#handleData);

    this.#transformer.on("message", this.#handleMessage);
  }

  transportType(): string {
    return "TCPROS";
  }

  async remoteAddress(): Promise<TcpAddress | undefined> {
    return await this.#socket.remoteAddress();
  }

  async connect(): Promise<void> {
    if (this.#shutdown) {
      return;
    }

    this.#log?.debug?.(`connecting to ${this.toString()} (attempt ${this.retries})`);

    try {
      await this.#socket.connect();
      this.#log?.debug?.(`connected to ${this.toString()}`);
    } catch (err) {
      this.#log?.warn?.(`${this.toString()} connection failed: ${err}`);
      // #handleClose() will be called, triggering a reconnect attempt
    }
  }

  #retryConnection(): void {
    if (!this.#shutdown) {
      backoff(++this.retries)
        // eslint-disable-next-line @typescript-eslint/promise-function-async
        .then(() => this.connect())
        .catch((err) => {
          // This should never be called, this.connect() is not expected to throw
          this.#log?.warn?.(`${this.toString()} unexpected retry failure: ${err}`);
        });
    }
  }

  connected(): boolean {
    return this.#connected;
  }

  header(): Map<string, string> {
    return new Map<string, string>(this.#header);
  }

  stats(): ConnectionStats {
    return this.#stats;
  }

  messageDefinition(): MessageDefinition[] {
    return this.#msgDefinition;
  }

  messageReader(): MessageReader | undefined {
    return this.#msgReader;
  }

  close(): void {
    this.#log?.debug?.(`closing connection to ${this.toString()}`);

    this.#shutdown = true;
    this.#connected = false;
    this.removeAllListeners();
    this.#socket.close().catch((err) => {
      this.#log?.warn?.(`${this.toString()} close failed: ${err}`);
    });
  }

  async writeHeader(): Promise<void> {
    const serializedHeader = TcpConnection.SerializeHeader(this.#requestHeader);
    const totalLen = 4 + serializedHeader.byteLength;
    this.#stats.bytesSent += totalLen;

    const data = new Uint8Array(totalLen);

    // Write the 4-byte length
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    view.setUint32(0, serializedHeader.byteLength, true);

    // Copy the serialized header into the final buffer
    data.set(serializedHeader, 4);

    // Write the length and serialized header payload
    await this.#socket.write(data);
  }

  // e.g. "TCPROS connection on port 59746 to [host:34318 on socket 11]"
  getTransportInfo(): string {
    return this.#transportInfo;
  }

  override toString(): string {
    return TcpConnection.Uri(this.#address, this.#port);
  }

  #getTransportInfo = async (): Promise<string> => {
    const localPort = (await this.#socket.localAddress())?.port ?? -1;
    const addr = await this.#socket.remoteAddress();
    const fd = (await this.#socket.fd()) ?? -1;
    if (addr != null) {
      const { address, port } = addr;
      const host = address.includes(":") ? `[${address}]` : address;
      return `TCPROS connection on port ${localPort} to [${host}:${port} on socket ${fd}]`;
    }
    return `TCPROS not connected [socket ${fd}]`;
  };

  #handleConnect = async (): Promise<void> => {
    if (this.#shutdown) {
      this.close();
      return;
    }

    this.#connected = true;
    this.retries = 0;
    this.#transportInfo = await this.#getTransportInfo();

    try {
      // Write the initial request header. This prompts the publisher to respond
      // with its own header then start streaming messages
      await this.writeHeader();
    } catch (err) {
      this.#log?.warn?.(`${this.toString()} failed to write header. reconnecting: ${err}`);
      this.emit("error", new Error(`Header write failed: ${err}`));
      this.#retryConnection();
    }
  };

  #handleClose = (): void => {
    this.#connected = false;
    if (!this.#shutdown) {
      this.#log?.warn?.(`${this.toString()} closed unexpectedly. reconnecting`);
      this.emit("error", new Error("Connection closed unexpectedly"));
      this.#retryConnection();
    }
  };

  #handleError = (err: Error): void => {
    if (!this.#shutdown) {
      this.#log?.warn?.(`${this.toString()} error: ${err}`);
      this.emit("error", err);
    }
  };

  #handleData = (chunk: Uint8Array): void => {
    if (this.#shutdown) {
      return;
    }

    try {
      this.#transformer.addData(chunk);
    } catch (unk) {
      const err = unk instanceof Error ? unk : new Error(unk as string);
      this.#log?.warn?.(
        `failed to decode ${chunk.length} byte chunk from tcp publisher ${this.toString()}: ${err}`,
      );
      // Close the socket, the stream is now corrupt
      this.#socket.close().catch((closeErr) => {
        this.#log?.warn?.(`${this.toString()} close failed: ${closeErr}`);
      });
      this.emit("error", err);
    }
  };

  #handleMessage = (msgData: Uint8Array): void => {
    if (this.#shutdown) {
      this.close();
      return;
    }

    this.#stats.bytesReceived += msgData.byteLength;

    if (this.#readingHeader) {
      this.#readingHeader = false;

      this.#header = TcpConnection.ParseHeader(msgData);
      this.#msgDefinition = parseMessageDefinition(this.#header.get("message_definition") ?? "");
      this.#msgReader = new MessageReader(this.#msgDefinition);
      this.emit("header", this.#header, this.#msgDefinition, this.#msgReader);
    } else {
      this.#stats.messagesReceived++;

      if (this.#msgReader != null) {
        try {
          const bytes = new Uint8Array(msgData.buffer, msgData.byteOffset, msgData.length);
          const msg = this.#msgReader.readMessage(bytes);
          this.emit("message", msg, msgData);
        } catch (unk) {
          const err = unk instanceof Error ? unk : new Error(unk as string);
          this.emit("error", err);
        }
      }
    }
  };

  static Uri(address: string, port: number): string {
    // RFC2732 requires IPv6 addresses that include ":" characters to be wrapped in "[]" brackets
    // when used in a URI
    const host = address.includes(":") ? `[${address}]` : address;
    return `tcpros://${host}:${port}`;
  }

  static SerializeHeader(header: Map<string, string>): Uint8Array {
    const encoder = new TextEncoder();
    const encoded = Array.from(header).map(([key, value]) => encoder.encode(`${key}=${value}`));
    const payloadLen = encoded.reduce((sum, str) => sum + str.length + 4, 0);
    const buffer = new ArrayBuffer(payloadLen);
    const array = new Uint8Array(buffer);
    const view = new DataView(buffer);

    let idx = 0;
    encoded.forEach((strData) => {
      view.setUint32(idx, strData.length, true);
      idx += 4;
      array.set(strData, idx);
      idx += strData.length;
    });

    return new Uint8Array(buffer);
  }

  static ParseHeader(data: Uint8Array): Map<string, string> {
    const decoder = new TextDecoder();
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const result = new Map<string, string>();

    let idx = 0;
    while (idx + 4 < data.length) {
      const len = Math.min(view.getUint32(idx, true), data.length - idx - 4);
      idx += 4;
      const str = decoder.decode(new Uint8Array(data.buffer, data.byteOffset + idx, len));
      let equalIdx = str.indexOf("=");
      if (equalIdx < 0) {
        equalIdx = str.length;
      }
      const key = str.substr(0, equalIdx);
      const value = str.substr(equalIdx + 1);
      result.set(key, value);
      idx += len;
    }

    return result;
  }
}
