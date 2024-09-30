import { EventEmitter } from "eventemitter3";

import { concatData } from "./concatData";

// This is the maximum message length allowed by ros_comm. See
// <https://github.com/strawlab/ros_comm/blob/master/clients/cpp/roscpp/src/libros/transport_publisher_link.cpp#L164-L167>
const MAX_MSG_LENGTH = 1000000000;

export interface RosTcpMessageStreamEvents {
  message: (message: Uint8Array) => void;
}

// A stateful transformer that takes a raw TCPROS data stream and parses the
// TCPROS format of 4 byte length prefixes followed by message payloads into one
// complete message per "message" event, discarding the length prefix
export class RosTcpMessageStream extends EventEmitter<RosTcpMessageStreamEvents> {
  #inMessage = false;
  #bytesNeeded = 4;
  #chunks: Uint8Array[] = [];

  addData(chunk: Uint8Array): void {
    let idx = 0;
    while (idx < chunk.length) {
      if (chunk.length - idx < this.#bytesNeeded) {
        // If we didn't receive enough bytes to complete the current message or
        // message length field, store this chunk and continue on
        this.#chunks.push(new Uint8Array(chunk.buffer, chunk.byteOffset + idx));
        this.#bytesNeeded -= chunk.length - idx;
        return;
      }

      // Store the final chunk needed to complete the current message or message
      // length field
      this.#chunks.push(new Uint8Array(chunk.buffer, chunk.byteOffset + idx, this.#bytesNeeded));
      idx += this.#bytesNeeded;

      const payload = concatData(this.#chunks);
      this.#chunks = [];

      if (this.#inMessage) {
        // Produce a Uint8Array representing a single message and transition to
        // reading a message length field
        this.#bytesNeeded = 4;
        this.emit("message", payload);
        this.#inMessage = false;
      } else {
        // Decode the message length field and transition to reading a message
        this.#bytesNeeded = new DataView(
          payload.buffer,
          payload.byteOffset,
          payload.byteLength,
        ).getUint32(0, true);

        if (this.#bytesNeeded > MAX_MSG_LENGTH) {
          throw new Error(`Invalid message length of ${this.#bytesNeeded} decoded in a tcp stream`);
        } else if (this.#bytesNeeded === 0) {
          this.#bytesNeeded = 4;
          this.emit("message", new Uint8Array());
        } else {
          this.#inMessage = true;
        }
      }
    }
  }
}
