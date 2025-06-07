import { StateStore } from "./state.ts";
import { Database } from "@atproto/bsky";
import { LabelerSubscriber } from "./labelerSubscriber.ts";
import { JetstreamWatcher } from "./jetstreamWatcher.ts";

export interface LabelMuncherOptions {
	dbOptions: Database["opts"];
	jetstreamUrl?: string;
	sqlitePath: string;
	labelerDids: string[];
	plcUrl?: string;
}

export class LabelMuncher {
	protected db: Database;
	protected state: StateStore;
	protected subscriber: LabelerSubscriber;
	protected jetstreamWatcher: JetstreamWatcher;
	protected options: LabelMuncherOptions;
	protected isRunning = false;

	constructor(options: LabelMuncherOptions) {
		this.options = options;

		this.db = new Database(options.dbOptions);
		this.state = new StateStore(options.sqlitePath);

		this.subscriber = new LabelerSubscriber({
			db: this.db,
			state: this.state,
			plcUrl: options.plcUrl,
		});
		this.jetstreamWatcher = new JetstreamWatcher({
			state: this.state,
			endpoint: options.jetstreamUrl,
			wantedDids: options.labelerDids,
		});
	}

	static fromEnvironment(): LabelMuncher {
		const dbUrl = Deno.env.get("BSKY_DB_POSTGRES_URL");
		if (!dbUrl) {
			throw new Error("BSKY_DB_POSTGRES_URL environment variable is required");
		}

		const schema = Deno.env.get("BSKY_DB_POSTGRES_SCHEMA") || "bsky";

		const labelerDidsEnv = Deno.env.get("BSKY_LABELS_FROM_ISSUER_DIDS");
		if (!labelerDidsEnv) {
			throw new Error("BSKY_LABELS_FROM_ISSUER_DIDS environment variable is required");
		}

		const labelerDids = labelerDidsEnv
			.split(",")
			.map((did) => did.trim())
			.filter((did) => did.length > 0);

		if (labelerDids.length === 0) {
			throw new Error("no labeler dids found in BSKY_LABELS_FROM_ISSUER_DIDS");
		}

		const sqlitePath = Deno.env.get("DB_PATH") || "./muncher-state.sqlite";

		return new LabelMuncher({
			dbOptions: {
				url: dbUrl,
				schema,
			},
			labelerDids,
			sqlitePath,
		});
	}

	async start(): Promise<void> {
		if (this.isRunning) {
			throw new Error("service is already running");
		}

		console.log("starting label muncher");

		try {
			console.log("starting jetstream watcher");
			this.jetstreamWatcher.start();

			console.log(`subscribing to ${this.options.labelerDids.length} labelers`);
			await this.subscriber.subscribeToLabelers(this.options.labelerDids);

			this.isRunning = true;
			console.log("label muncher started successfully");

			this.logStatus();

			setInterval(() => {
				this.logStatus();
			}, 60000);
		} catch (error) {
			console.error("Failed to start service:", error);
			await this.cleanup();
			throw error;
		}
	}

	async stop(): Promise<void> {
		if (!this.isRunning) {
			return;
		}

		console.log("stopping label muncher");
		await this.cleanup();
		this.isRunning = false;
		console.log("label muncher stopped");
	}

	protected async cleanup(): Promise<void> {
		try {
			await Promise.all([
				this.subscriber.close(),
				this.jetstreamWatcher.close(),
				this.db.close(),
			]);
		} catch (error) {
			console.error("error during cleanup:", error);
		}
	}

	protected logStatus(): void {
		const subscriberStatus = this.subscriber.getConnectionStatus();

		console.log(
			"labeler connections:",
			Object.entries(subscriberStatus).map(([did, connected]) =>
				`${did}: ${connected ? "connected" : "disconnected"}`
			).join(", "),
		);
	}
}
