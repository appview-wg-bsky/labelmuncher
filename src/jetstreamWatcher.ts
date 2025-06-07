import { Jetstream } from "@skyware/jetstream";
import { StateStore } from "./state.ts";

export interface JetstreamWatcherOptions {
	endpoint?: string;
	wantedDids: string[];
	state: StateStore;
}

export class JetstreamWatcher {
	protected jetstream: Jetstream;
	protected state: StateStore;
	protected wantedDids: Set<string>;

	constructor({ endpoint, wantedDids, state }: JetstreamWatcherOptions) {
		this.state = state;
		this.wantedDids = new Set(wantedDids);
		this.jetstream = new Jetstream({
			endpoint,
			wantedCollections: ["app.bsky.labeler.service"],
			wantedDids: [...this.wantedDids],
		});
	}

	start() {
		this.jetstream.on("open", () => {
			console.log("connected to jetstream");
		});

		this.jetstream.on("close", () => {
			console.log("disconnected from jetstream");
		});

		this.jetstream.on("error", (error) => {
			console.error("jetstream error:", error);
		});

		this.jetstream.on("commit", (commit) => {
			if (this.wantedDids.has(commit.did)) this.handleServiceRecordChange(commit.did);
		});

		this.jetstream.start();
	}

	protected handleServiceRecordChange(did: string) {
		try {
			console.log(`service record changed for ${did}, clearing cache`);

			const cached = this.state.getServiceCache(did);
			if (cached) {
				this.state.setServiceCache({
					did,
					label_values: [],
					cached_at: 0, // Force expiry
				});
			}
		} catch (error) {
			console.error(`error handling service record change for ${did}:`, error);
		}
	}

	close() {
		this.jetstream.close();
	}
}
