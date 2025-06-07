import { DatabaseSync } from "node:sqlite";

export interface CursorRecord {
	did: string;
	cursor: number;
}

export interface DidCacheRecord {
	did: string;
	service_endpoint: string;
	public_key: string;
	cached_at: number;
}

export interface ServiceCacheRecord {
	did: string;
	label_values: string[];
	cached_at: number;
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
        service_endpoint TEXT NOT NULL,
        public_key TEXT NOT NULL,
        cached_at INTEGER NOT NULL
      )
    `);
		this.db.exec(`
      CREATE TABLE IF NOT EXISTS service_cache (
        did TEXT PRIMARY KEY,
        label_values TEXT NOT NULL,
        cached_at INTEGER NOT NULL
      )
    `);
	}

	getCursor(did: string): number | null {
		const result = this.db
			.prepare("SELECT cursor FROM cursors WHERE did = ?")
			.get(did);
		return result ? result.cursor as number : null;
	}

	setCursor(did: string, cursor: number) {
		return this.db
			.prepare("INSERT OR REPLACE INTO cursors (did, cursor) VALUES (?, ?)")
			.run(did, cursor);
	}

	getDidCache(did: string): DidCacheRecord | null {
		const result = this.db
			.prepare("SELECT * FROM did_cache WHERE did = ?")
			.get(did);
		return result as unknown as DidCacheRecord ?? null;
	}

	setDidCache(record: DidCacheRecord) {
		this.db
			.prepare(
				`
      INSERT OR REPLACE INTO did_cache (did, service_endpoint, public_key, cached_at)
      VALUES (?, ?, ?, ?)
    `,
			)
			.run(
				record.did,
				record.service_endpoint,
				record.public_key,
				record.cached_at,
			);
	}

	getServiceCache(did: string): ServiceCacheRecord | null {
		const result = this.db
			.prepare("SELECT * FROM service_cache WHERE did = ?")
			.get(did) as unknown as Serialized<ServiceCacheRecord>;
		if (!result) return null;

		const now = Date.now();
		const cacheAge = now - result.cached_at;
		const twentyFourHours = 24 * 60 * 60 * 1000;

		if (cacheAge > twentyFourHours) {
			this.db.prepare("DELETE FROM service_cache WHERE did = ?").run(did);
			return null;
		}

		return {
			did: result.did,
			label_values: JSON.parse(result.label_values),
			cached_at: result.cached_at,
		};
	}

	setServiceCache(record: ServiceCacheRecord) {
		this.db
			.prepare(
				`
      INSERT OR REPLACE INTO service_cache (did, label_values, cached_at)
      VALUES (?, ?, ?)
    `,
			)
			.run(record.did, JSON.stringify(record.label_values), record.cached_at);
	}

	close() {
		this.db.close();
	}
}
