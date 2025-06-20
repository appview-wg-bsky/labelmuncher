import { DatabaseSync } from "node:sqlite";

export interface CursorRecord {
	did: string;
	cursor: number;
}

export interface DidCacheRecord {
	did: string;
	serviceEndpoint: string;
	publicKey: string;
	cachedAt: number;
}

export interface ServiceCacheRecord {
	did: string;
	labelValues: string[];
	cachedAt: number;
}

export type Serialized<T> = {
	[K in keyof T]: T[K] extends unknown[] ? string : T[K];
};

export class StateStore {
	protected db: DatabaseSync;

	constructor(path: string) {
		this.db = new DatabaseSync(path);
	}

	init() {
		this.db.exec(`
      CREATE TABLE IF NOT EXISTS cursors (
        did TEXT PRIMARY KEY,
        cursor INTEGER NOT NULL
      )
    `);
		this.db.exec(`
      CREATE TABLE IF NOT EXISTS did_cache (
        did TEXT PRIMARY KEY,
        serviceEndpoint TEXT NOT NULL,
        publicKey TEXT NOT NULL,
        cachedAt INTEGER NOT NULL
      )
    `);
		this.db.exec(`
      CREATE TABLE IF NOT EXISTS service_cache (
        did TEXT PRIMARY KEY,
        labelValues TEXT NOT NULL,
        cachedAt INTEGER NOT NULL
      )
    `);
	}

	getCursor(did: string): number | null {
		const result = this.db
			.prepare("SELECT cursor FROM cursors WHERE did = ?")
			.get(did) as { cursor: number };
		return result ? result.cursor : null;
	}

	setCursor(did: string, cursor: number) {
		return this.db
			.prepare("INSERT OR REPLACE INTO cursors (did, cursor) VALUES (?, ?)")
			.run(did, cursor);
	}

	getDidCache(did: string): DidCacheRecord | null {
		return this.db
			.prepare("SELECT * FROM did_cache WHERE did = ?")
			.get(did) as Serialized<DidCacheRecord> ?? null;
	}

	setDidCache(record: DidCacheRecord) {
		this.db
			.prepare(
				`
      INSERT OR REPLACE INTO did_cache (did, serviceEndpoint, publicKey, cachedAt)
      VALUES (?, ?, ?, ?)
    `,
			)
			.run(
				record.did,
				record.serviceEndpoint,
				record.publicKey,
				record.cachedAt,
			);
	}

	getServiceCache(did: string): ServiceCacheRecord | null {
		const result = this.db
			.prepare("SELECT * FROM service_cache WHERE did = ?")
			.get(did) as Serialized<ServiceCacheRecord>;
		if (!result) return null;

		return {
			did: result.did,
			labelValues: JSON.parse(result.labelValues),
			cachedAt: result.cachedAt,
		};
	}

	setServiceCache(record: ServiceCacheRecord) {
		this.db
			.prepare(
				`
      INSERT OR REPLACE INTO service_cache (did, labelValues, cachedAt)
      VALUES (?, ?, ?)
    `,
			)
			.run(record.did, JSON.stringify(record.labelValues), record.cachedAt);
	}

	close() {
		this.db.close();
	}
}
