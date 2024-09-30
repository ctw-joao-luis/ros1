import { XmlRpcClient, XmlRpcFault, XmlRpcValue } from "@lichtblick/xmlrpc";

import { RosXmlRpcResponse, RosXmlRpcResponseOrFault } from "./XmlRpcTypes";

export class RosXmlRpcClient {
  #client: XmlRpcClient;

  constructor(url: string) {
    this.#client = new XmlRpcClient(url, { encoding: "utf-8" });
  }

  url(): string {
    return this.#client.url;
  }

  protected _methodCall = async (
    methodName: string,
    args: XmlRpcValue[],
  ): Promise<RosXmlRpcResponse> => {
    const res = await this.#client.methodCall(methodName, args);
    if (!Array.isArray(res) || res.length !== 3) {
      throw new Error(`Malformed XML-RPC response`);
    }

    const [code, msg] = res;
    if (typeof code !== "number" || typeof msg !== "string") {
      throw new Error(`Invalid code/msg, code="${code}", msg="${msg}"`);
    }
    return res as RosXmlRpcResponse;
  };

  protected _multiMethodCall = async (
    requests: { methodName: string; params: XmlRpcValue[] }[],
  ): Promise<RosXmlRpcResponseOrFault[]> => {
    const res = await this.#client.multiMethodCall(requests);

    const output: RosXmlRpcResponseOrFault[] = [];
    for (const entry of res) {
      if (entry instanceof XmlRpcFault) {
        output.push(entry);
      } else if (!Array.isArray(entry) || entry.length !== 3) {
        throw new Error(`Malformed XML-RPC multicall response`);
      } else {
        const [code, msg] = entry;
        if (typeof code !== "number" || typeof msg !== "string") {
          throw new Error(`Invalid code/msg, code="${code}", msg="${msg}"`);
        }
        output.push(entry as RosXmlRpcResponse);
      }
    }
    return output;
  };
}
