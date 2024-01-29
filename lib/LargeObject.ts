import type { Client as PGClient, QueryResult } from "pg";
import { ReadStream } from "./ReadStream";
import { WriteStream } from "./WriteStream";

export const enum Seek {
  /** A seek from the beginning of a object */
  SEEK_SET = 0,
  /** A seek from the current position */
  SEEK_CUR = 1,
  /** A seek from the end of a object */
  SEEK_END = 2,
}

/**
 * Represents an opened large object.
 */
export class LargeObject {
  constructor(
    private query: PGClient["query"],
    private fd: number,
  ) {}

  /**
   * Closes this large object.
   * You should no longer call any methods on this object.
   */
  async close(): Promise<void> {
    await this.query("SELECT lo_close($1) as ok", [this.fd]);
  }

  /** Reads some data from the large object.
   * @param {Number} length How many bytes to read
   */
  async read(length: number): Promise<Buffer> {
    const result: QueryResult<{ data: Buffer }> = await this.query(
      "SELECT loread($1, $2) as data",
      [this.fd, length],
    );
    return result.rows[0].data;
  }

  /**
   * Writes some data to the large object.
   * @param {Buffer} buffer data to write
   */
  async write(buffer: Buffer): Promise<void> {
    await this.query("SELECT lowrite($1, $2)", [this.fd, buffer]);
  }

  async seek(position: number, ref: Seek): Promise<number> {
    const result: QueryResult<{ location: number }> = await this.query(
      "SELECT lo_lseek64($1, $2, $3) as location",
      [this.fd, position, ref],
    );
    return result.rows[0].location;
  }

  /**
   * Retrieves the current position within the large object.
   * Beware floating point rounding with values greater than 2^53 (8192 TiB)
   */
  async tell(): Promise<number> {
    const result: QueryResult<{ position: number }> = await this.query(
      "SELECT lo_tell64($1) as position",
      [this.fd],
    );
    return result.rows[0].position;
  }

  /**
   * Find the total size of the large object.
   */
  async size(): Promise<number> {
    const result: QueryResult<{ size: number }> = await this.query(
      "SELECT lo_lseek64($1, location, 0), seek.size FROM " +
        "(SELECT lo_lseek64($1, 0, 2) AS SIZE, tell.location FROM " +
        "(SELECT lo_tell64($1) AS location) tell) " +
        "seek;",
      [this.fd],
    );
    return result.rows[0].size;
  }

  /**
   * Truncates the large object to the given length in bytes.
   * If the number of bytes is larger than the current large
   * object length, the large object will be filled with zero
   * bytes.  This method does not modify the current file offset.
   */
  async truncate(length: number) {
    await this.query("SELECT lo_truncate64($1, $2)", [this.fd, length]);
  }

  /**
   * Return a stream to read this large object.
   * Call this within a transaction block.
   * @param {Number} [bufferSize=16384] A larger buffer size will
   * require more memory on both the server and client, however it will make
   * transfers faster because there is less overhead (less read calls to the server).
   * his overhead is most noticeable on high latency connections because each
   * transferred chunk will incur at least RTT of additional transfer time.
   */
  getReadableStream(bufferSize: number = 16384) {
    return new ReadStream(this, bufferSize);
  }

  /**
   * Return a stream to write to this large object.
   * Call this within a transaction block.
   * @param {Number} [bufferSize=16384] A larger buffer size will
   * require more memory on both the server and client, however it will make
   * transfers faster because there is less overhead (less read calls to the server).
   * his overhead is most noticeable on high latency connections because each
   * transferred chunk will incur at least RTT of additional transfer time.
   */
  getWritableStream(bufferSize: number = 16384) {
    return new WriteStream(this, bufferSize);
  }
}
