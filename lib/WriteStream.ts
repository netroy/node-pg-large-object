import { Writable } from 'stream'
import type { LargeObject } from './LargeObject'

export class WriteStream extends Writable {
	constructor(private largeObject: LargeObject, bufferSize: number = 16384) {
		super({
			'highWaterMark': bufferSize,
			'decodeStrings': true,
			'objectMode': false
		})
	}

	_write(chunk: any, encoding: BufferEncoding, callback: (error?: Error) => void): void {
		if (!Buffer.isBuffer(chunk)) throw new Error('Chunk was not a Buffer')

		this.largeObject.write(chunk).then(() => callback()).catch(callback);
	}
}
