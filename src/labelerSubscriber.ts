import type { Database } from "@atproto/bsky";
import { decodeFirst } from "@atcute/cbor";
import { LabelValidator, type LabelValidatorOptions } from "./labelValidator.ts";
import { WebSocket } from "partysocket";
import type { StateStore } from "./state.ts";
import { type ComAtprotoLabelDefs, ComAtprotoLabelSubscribeLabels } from "@atcute/atproto";
import { is } from "@atcute/lexicons";

export interface LabelerSubscription {
	did: string;
	ws: WebSocket | null;
	reconnectAttempts: number;
	isConnected: boolean;
}

export interface LabelerSubscriberOptions extends LabelValidatorOptions {}

export class LabelerSubscriber {
	protected subscriptions = new Map<string, LabelerSubscription>();
	protected state: StateStore;
	protected db: Database;
	protected validator: LabelValidator;
	protected maxReconnectAttempts = 10;
	protected reconnectDelay = 5000;

	constructor(options: LabelerSubscriberOptions) {
		this.state = options.state;
		this.db = options.db;
		this.validator = new LabelValidator(options);
	}

	async subscribeToLabelers(dids: string[]) {
		for (const did of dids) {
			await this.subscribeToLabeler(did);
		}
	}

	protected async subscribeToLabeler(did: string) {
		try {
			const didCache = this.state.getDidCache(did);
			let serviceEndpoint: string;

			if (didCache) {
				serviceEndpoint = didCache.service_endpoint;
			} else {
				// This will update the DID cache before failing
				await this.validator.validateLabel({
					src: did as `did:plc:${string}`,
					uri: "at://dummy",
					val: "dummy",
					cts: new Date().toISOString(),
				}, did);

				const cached = this.state.getDidCache(did);
				if (!cached) {
					console.error(`failed to resolve service endpoint for ${did}`);
					return;
				}
				serviceEndpoint = cached.service_endpoint;
			}

			const cursor = this.state.getCursor(did) || 0;

			const url = new URL("/xrpc/com.atproto.label.subscribeLabels", serviceEndpoint);
			url.searchParams.set("cursor", cursor.toString());

			const subscription: LabelerSubscription = {
				did,
				ws: null,
				reconnectAttempts: 0,
				isConnected: false,
			};

			this.subscriptions.set(did, subscription);

			this.connectLabeler(subscription, url.toString());
		} catch (error) {
			console.error(`error subscribing to labeler ${did}:`, error);
		}
	}

	protected connectLabeler(subscription: LabelerSubscription, url: string) {
		try {
			const ws = new WebSocket(url);
			ws.binaryType = "arraybuffer";

			ws.addEventListener("open", () => {
				console.log(`connected to labeler ${subscription.did}`);
				subscription.isConnected = true;
				subscription.reconnectAttempts = 0;
			});

			ws.addEventListener("close", () => {
				console.log(`disconnected from labeler ${subscription.did}`);
				subscription.isConnected = false;
				this.handleReconnect(subscription, url);
			});

			ws.addEventListener("error", (error) => {
				console.error(`websocket error for ${subscription.did}:`, error);
				subscription.isConnected = false;
			});

			ws.addEventListener("message", (event) => {
				this.handleMessage(subscription.did, event.data);
			});

			subscription.ws = ws;
		} catch (error) {
			console.error(`failed to connect to ${subscription.did}:`, error);
			this.handleReconnect(subscription, url);
		}
	}

	protected async handleMessage(did: string, data: ArrayBuffer) {
		try {
			const message = this.parseMessage(data);

			if (message.$type === "com.atproto.label.subscribeLabels#labels") {
				await this.processLabelsMessage(did, message);
			} else if (message.$type === "com.atproto.label.subscribeLabels#info") {
				console.log(`info message from ${did}:`, message.message);
			}
		} catch (error) {
			console.error(`error processing message from ${did}:`, error);
		}
	}

	protected async processLabelsMessage(
		did: string,
		message: ComAtprotoLabelSubscribeLabels.Labels,
	) {
		try {
			this.state.setCursor(did, message.seq);

			for (const label of message.labels) {
				await this.processLabel(label, did);
			}
		} catch (error) {
			console.error(`error processing labels from ${did}:`, error);
		}
	}

	protected async processLabel(label: ComAtprotoLabelDefs.Label, did: string) {
		try {
			const validation = await this.validator.validateLabel(label, did);

			if (!validation.valid) {
				console.error(`invalid label from ${label.src}: ${validation.reason}`);
				return;
			}

			const dbLabel = {
				src: label.src,
				uri: label.uri,
				cid: label.cid || "",
				val: label.val,
				neg: label.neg || false,
				cts: label.cts,
				exp: label.exp || null,
			};

			await this.db.db.insertInto("label").values(dbLabel).execute();
		} catch (error) {
			console.error(`error processing label:`, error);
		}
	}

	protected parseMessage(
		data: ArrayBuffer,
	): ComAtprotoLabelSubscribeLabels.Labels | ComAtprotoLabelSubscribeLabels.Info {
		try {
			const buffer = new Uint8Array(data);
			const [header, remainder] = decodeFirst(buffer);
			const [body, remainder2] = decodeFirst(remainder);
			if (remainder2.length > 0) {
				throw new Error("excess bytes in message");
			}

			const { t, op } = this.parseHeader(header);

			if (op === -1) {
				throw body.message;
			}

			const message = {
				$type: `com.atproto.sync.subscribeLabels${t}`,
				...body,
			};

			if (
				!is(ComAtprotoLabelSubscribeLabels.labelsSchema, message) &&
				!is(ComAtprotoLabelSubscribeLabels.infoSchema, message)
			) {
				throw new Error("invalid message format");
			}

			return message;
		} catch (error) {
			console.error("failed to parse message:", error);
			throw error;
		}
	}

	protected parseHeader(header: any): { t: string; op: 1 | -1 } {
		if (
			!header ||
			typeof header !== "object" ||
			!header.t ||
			typeof header.t !== "string" ||
			!header.op ||
			typeof header.op !== "number"
		) {
			throw new Error("invalid header received");
		}
		return { t: header.t, op: header.op };
	}

	protected handleReconnect(subscription: LabelerSubscription, url: string) {
		if (subscription.reconnectAttempts >= this.maxReconnectAttempts) {
			console.error(`max reconnect attempts reached for ${subscription.did}`);
			return;
		}

		subscription.reconnectAttempts++;

		setTimeout(() => {
			console.log(
				`attempting to reconnect to ${subscription.did} (attempt ${subscription.reconnectAttempts})`,
			);

			const cursor = this.state.getCursor(subscription.did) || 0;
			const reconnectUrl = new URL(url);
			reconnectUrl.searchParams.set("cursor", cursor.toString());

			this.connectLabeler(subscription, reconnectUrl.toString());
		}, this.reconnectDelay * subscription.reconnectAttempts);
	}

	close() {
		for (const subscription of this.subscriptions.values()) {
			if (subscription.ws) {
				subscription.ws.close();
			}
		}
		this.subscriptions.clear();
	}

	getConnectionStatus(): Record<string, boolean> {
		const status: Record<string, boolean> = {};
		for (const [did, subscription] of this.subscriptions) {
			status[did] = subscription.isConnected;
		}
		return status;
	}
}
