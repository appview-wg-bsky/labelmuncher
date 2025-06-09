import { JetstreamSubscription } from "@atcute/jetstream";
import type { StateStore } from "./state.ts";

export interface JetstreamWatcherOptions {
	endpoint?: string;
	wantedDids: string[];
	state: StateStore;
}

export class JetstreamWatcher {
	protected jetstream: JetstreamSubscription;
	protected state: StateStore;
	protected wantedDids: Set<string>;
	protected controller = new AbortController();

	constructor({ endpoint, wantedDids, state }: JetstreamWatcherOptions) {
		this.state = state;
		this.wantedDids = new Set(wantedDids);
		this.jetstream = new JetstreamSubscription({
			url: endpoint ?? "wss://jetstream1.us-east.bsky.network/subscribe",
			wantedCollections: ["app.bsky.labeler.service"],
			wantedDids: [...(this.wantedDids as Set<`did:plc:${string}`>)],
		});
	}

	async start() {
		for await (const evt of this.jetstream) {
			if (
				this.wantedDids.has(evt.did) &&
				evt.kind === "commit" &&
				(evt.commit.operation === "update" ||
					evt.commit.operation === "create") &&
				!this.controller.signal.aborted
			) {
				this.handleServiceRecordChange(evt.did);
			}
		}
	}

	protected handleServiceRecordChange(did: string) {
		try {
			console.log(`service record changed for ${did}, clearing cache`);

			const cached = this.state.getServiceCache(did);
			if (cached) {
				this.state.setServiceCache({
					did,
					labelValues: [],
					cachedAt: 0, // Force expiry
				});
			}
		} catch (error) {
			console.error(`error handling service record change for ${did}:`, error);
		}
	}

	close() {
		this.controller.abort();
	}
}
