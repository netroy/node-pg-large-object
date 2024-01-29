import { Readable } from "stream";
import type { LargeObject } from "./LargeObject";

export class ReadStream extends Readable {
  constructor(
    private largeObject: LargeObject,
    bufferSize: number = 16384,
  ) {
    super({
      highWaterMark: bufferSize,
      encoding: null,
      objectMode: false,
    });
  }

  _read(length: number): void {
    if (length <= 0) throw new Error("Illegal Argument");

    this.largeObject
      .read(length)
      .then((data) => {
        this.push(data);
        if (data.length < length) {
          this.push(null); // the large object has no more data left
        }
      })
      .catch((error) => {
        this.emit("error", error);
      });
  }
}
