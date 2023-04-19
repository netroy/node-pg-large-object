import { Client } from 'pg';
import { LargeObjectManager, Mode } from '../lib';

describe('PG', () => {
	let client: Client;

	beforeAll(async () => {
		client = new Client({
			host: 'localhost',
			port: 5432,
			database: 'nodetest',
			user: 'nodetest',
			password: 'nodetest',
		});
		await client.connect();
	})

	afterAll(async () => {
		await client?.end();
	})

	test('Test Create and Write', async () => {
		const testBuf = Buffer.from('0123456789ABCDEF', 'hex');
		const manager = new LargeObjectManager(client);
		const objectId = await manager.create();
		expect(objectId).not.toEqual(0);

		await client.query('BEGIN');
		let obj = await manager.open(objectId, Mode.WRITE);
		await obj.write(testBuf);
		await obj.close();
		await client.query('COMMIT');

		await client.query('BEGIN');
		obj = await manager.open(objectId, Mode.READ);
		const size = await obj.size();
		const data = await obj.read(size);
		await obj.close();
		expect(data).toEqual(testBuf);
		await client.query('COMMIT');

		await manager.unlink(objectId);

		await expect(manager.open(objectId, Mode.READ)).rejects.toThrowError(`large object ${objectId} does not exist`);
	});
})
