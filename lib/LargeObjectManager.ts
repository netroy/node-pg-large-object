import type { Client as PGClient, QueryResult } from "pg";
import { LargeObject } from "./LargeObject";

export enum Mode {
  WRITE = 0x00020000,
  READ = 0x00040000,
  READWRITE = WRITE | READ,
}

/** This class lets you use the Large Object functionality of PostgreSQL.
 * All usage of Large Object should take place within a transaction block!
 * (BEGIN ... COMMIT)
 */
export class LargeObjectManager {
  private query: PGClient["query"];

  constructor(pg: PGClient) {
    this.query = pg.query.bind(pg);
  }

  /** Open an existing large object, based on its OID.
   * In mode READ, the data read from it will reflect the
   * contents of the large object at the time of the transaction
   * snapshot that was active when open was executed,
   * regardless of later writes by this or other transactions.
   * If opened using WRITE (or READWRITE), data read will reflect
   * all writes of other committed transactions as well as
   * writes of the current transaction.
   */
  async open(objectId: number, mode: Mode): Promise<LargeObject> {
    const result: QueryResult<{ fd: number }> = await this.query(
      "SELECT lo_open($1, $2) AS fd",
      [objectId, mode],
    );
    return new LargeObject(this.query, result.rows[0].fd);
  }

  /** Creates a large object, returning its OID.
   * After which you can open() it.
   */
  async create(): Promise<number> {
    const result: QueryResult<{ objectId: number }> = await this.query(
      'SELECT lo_creat($1) AS "objectId"',
      [Mode.READWRITE],
    );
    return result.rows[0].objectId;
  }

  /** Unlinks (deletes) a large object */
  async unlink(objectId: number) {
    await this.query("SELECT lo_unlink($1) as ok", [objectId]);
  }

  /** Open a large object, return a stream and close the object when done streaming.
   * Only call this within a transaction block.
   */
  async openAndReadableStream(objectId: number) {
    const obj = await this.open(objectId, Mode.READ);
    const size = await obj.size();
    const stream = obj.getReadableStream();
    stream.once("end", async () => {
      // this should rarely happen, but if it does, explicitly handle this
      // (otherwise an error will be emitted by node-postgres)
      try {
        await obj.close();
      } catch (error) {
        console.error("Warning: closing a large object failed:", error);
      }
    });
    return [size, stream];
  }

  /** Create and open a large object, return a stream and close the object when done streaming.
   * Only call this within a transaction block.
   */
  async createAndWritableStream() {
    const objectId = await this.create();
    const obj = await this.open(objectId, Mode.WRITE);
    const stream = obj.getWritableStream();
    stream.once("finish", async () => {
      try {
        await obj.close();
      } catch (error) {
        console.error("Warning: closing a large object failed:", error);
      }
    });
    return [objectId, stream];
  }
}
