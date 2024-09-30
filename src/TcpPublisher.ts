import { EventEmitter } from "eventemitter3";

import { Client } from "./Client";
import { LoggerService } from "./LoggerService";
import { Publication } from "./Publication";
import { Publisher } from "./Publisher";
import { PublicationLookup, TcpClient } from "./TcpClient";
import type { TcpAddress, TcpServer, TcpSocket } from "./TcpTypes";

type TcpPublisherOpts = {
  server: TcpServer;
  nodeName: string;
  getConnectionId: () => number;
  getPublication: PublicationLookup;
  log?: LoggerService;
};

interface TcpPublisherEvents {
  connection: (
    topic: string,
    connectionId: number,
    destinationCallerId: string,
    client: Client,
  ) => void;
  error: (err: Error) => void;
}

// Implements publishing support for the TCPROS transport. The actual TCP server
// is implemented in the passed in `server` (TcpServer). A `RosNode` instance
// uses a single `TcpPublisher` instance for all published topics, each incoming
// TCP connection sends a connection header that specifies which topic that
// connection is subscribing to.
export class TcpPublisher extends EventEmitter<TcpPublisherEvents> implements Publisher {
  #server: TcpServer;
  #nodeName: string;
  #getConnectionId: () => number;
  #getPublication: PublicationLookup;
  #pendingClients = new Map<number, TcpClient>();
  #shutdown = false;
  #log?: LoggerService;

  constructor({ server, nodeName, getConnectionId, getPublication, log }: TcpPublisherOpts) {
    super();
    this.#server = server;
    this.#nodeName = nodeName;
    this.#getConnectionId = getConnectionId;
    this.#getPublication = getPublication;
    this.#log = log;

    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    server.on("connection", this.#handleConnection);
    server.on("close", this.#handleClose);
    server.on("error", this.#handleError);
  }

  async address(): Promise<TcpAddress | undefined> {
    return await this.#server.address();
  }

  async publish(publication: Publication, message: unknown): Promise<void> {
    const msgSize = publication.messageWriter.calculateByteSize(message);
    const dataSize = 4 + msgSize;
    const buffer = new ArrayBuffer(dataSize);

    // Write the 4-byte size integer
    new DataView(buffer, 0, 4).setUint32(0, msgSize, true);

    // Write the serialized message data
    const msgData = new Uint8Array(buffer, 4, dataSize - 4);
    publication.messageWriter.writeMessage(message, msgData);

    const data = new Uint8Array(buffer, 0, dataSize);
    await publication.write(this.transportType(), data);
  }

  transportType(): string {
    return "TCPROS";
  }

  listening(): boolean {
    return !this.#shutdown;
  }

  close(): void {
    this.#log?.debug?.(`stopping tcp publisher for ${this.#nodeName}`);

    this.#shutdown = true;
    this.removeAllListeners();
    this.#server.close();

    for (const client of this.#pendingClients.values()) {
      client.removeAllListeners();
      client.close();
    }
    this.#pendingClients.clear();
  }

  // TcpServer handlers ////////////////////////////////////////////////////////

  #handleConnection = async (socket: TcpSocket): Promise<void> => {
    const noOp = () => {
      // no-op
    };

    if (this.#shutdown) {
      socket.close().catch(noOp);
      return;
    }

    // TcpClient must be instantiated before any async calls since it registers event handlers
    // that may not be setup in time otherwise
    const client = new TcpClient({
      socket,
      nodeName: this.#nodeName,
      getPublication: this.#getPublication,
      log: this.#log,
    });

    const connectionId = this.#getConnectionId();
    this.#pendingClients.set(connectionId, client);

    client.on("subscribe", (topic, destinationCallerId) => {
      this.#pendingClients.delete(connectionId);
      if (!this.#shutdown) {
        this.emit("connection", topic, connectionId, destinationCallerId, client);
      }
    });
    client.on("error", (err) => {
      if (!this.#shutdown) {
        this.#log?.warn?.(`tcp client ${client.toString()} error: ${err}`);
        this.emit("error", new Error(`TCP client ${client.toString()} error: ${err}`));
      }
    });
  };

  #handleClose = (): void => {
    if (!this.#shutdown) {
      this.#log?.warn?.(`tcp server closed unexpectedly. shutting down tcp publisher`);
      this.emit("error", new Error("TCP publisher closed unexpectedly"));
      this.#shutdown = true;
    }
  };

  #handleError = (err: Error): void => {
    if (!this.#shutdown) {
      this.#log?.warn?.(`tcp publisher error: ${err}`);
      this.emit("error", err);
    }
  };
}
