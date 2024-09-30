import { HttpHandler, HttpServer } from "@lichtblick/xmlrpc";

export class MockHttpServer implements HttpServer {
  handler: HttpHandler = async (_req) => ({ statusCode: 404 });

  #port?: number;
  #hostname?: string;
  #defaultHost: string;
  #defaultPort: number;

  constructor(defaultHost: string, defaultPort: number) {
    this.#defaultHost = defaultHost;
    this.#defaultPort = defaultPort;
  }

  url(): string | undefined {
    if (this.#hostname == undefined || this.#port == undefined) {
      return undefined;
    }
    return `http://${this.#hostname}:${this.#port}/`;
  }

  port(): number | undefined {
    return this.#port;
  }

  async listen(port?: number, hostname?: string, _backlog?: number): Promise<void> {
    this.#port = port ?? this.#defaultPort;
    this.#hostname = hostname ?? this.#defaultHost;
  }

  close(): void {
    this.#port = undefined;
    this.#hostname = undefined;
  }
}
