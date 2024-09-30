import { XmlRpcFault, XmlRpcValue } from "@lichtblick/xmlrpc";

export type RosXmlRpcResponse = [code: number, msg: string, value: XmlRpcValue];
export type RosXmlRpcResponseOrFault = RosXmlRpcResponse | XmlRpcFault;
