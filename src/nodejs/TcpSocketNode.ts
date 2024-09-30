import EventEmitter from "eventemitter3";
import net from "net";

import { TcpAddress, TcpSocket, TcpSocketEvents } from "../TcpTypes";

type MaybeHasFd = {
  _handle?: {
    fd?: number;
  };
};

export class TcpSocketNode extends EventEmitter<TcpSocketEvents> implements TcpSocket {
  #host: string;
  #port: number;
  #socket: net.Socket;

  constructor(host: string, port: number, socket: net.Socket) {
    super();
    this.#host = host;
    this.#port = port;
    this.#socket = socket;

    socket.on("connect", () => this.emit("connect"));
    socket.on("close", () => this.emit("close"));
    socket.on("data", (chunk) => this.emit("data", chunk));
    socket.on("end", () => this.emit("end"));
    socket.on("timeout", () => this.emit("timeout"));
    socket.on("error", (err) => this.emit("error", err));
  }

  async remoteAddress(): Promise<TcpAddress | undefined> {
    return {
      port: this.#port,
      family: this.#socket.remoteFamily,
      address: this.#host,
    };
  }

  async localAddress(): Promise<TcpAddress | undefined> {
    if (this.#socket.destroyed) {
      return undefined;
    }
    const port = this.#socket.localPort;
    const family = this.#socket.remoteFamily; // There is no localFamily
    const address = this.#socket.localAddress;
    return port != undefined && family != undefined && address != undefined
      ? { port, family, address }
      : undefined;
  }

  async fd(): Promise<number | undefined> {
    // There is no public node.js API for retrieving the file descriptor for a
    // socket. This is the only way of retrieving it from pure JS, on platforms
    // where sockets have file descriptors. See
    // <https://github.com/nodejs/help/issues/1312>
    // eslint-disable-next-line no-underscore-dangle
    return (this.#socket as unknown as MaybeHasFd)._handle?.fd;
  }

  async connected(): Promise<boolean> {
    return !this.#socket.destroyed && this.#socket.localAddress != undefined;
  }

  async connect(): Promise<void> {
    await new Promise((resolve, reject) => {
      const KEEPALIVE_MS = 60 * 1000;

      this.#socket.on("error", reject).connect(this.#port, this.#host, () => {
        this.#socket.removeListener("error", reject);
        this.#socket.setKeepAlive(true, KEEPALIVE_MS);
        resolve(undefined);
      });
    });
  }

  async close(): Promise<void> {
    this.#socket.destroy();
  }

  async write(data: Uint8Array): Promise<void> {
    await new Promise((resolve, reject) => {
      this.#socket.write(data, (err) => {
        if (err != undefined) {
          reject(err);
          return;
        }
        resolve(undefined);
      });
    });
  }

  // eslint-disable-next-line @lichtblick/no-boolean-parameters
  async setNoDelay(noDelay?: boolean): Promise<void> {
    this.#socket.setNoDelay(noDelay);
  }

  static async Create(
    this: void,
    { host, port }: { host: string; port: number },
  ): Promise<TcpSocket> {
    return new TcpSocketNode(host, port, new net.Socket());
  }
}
