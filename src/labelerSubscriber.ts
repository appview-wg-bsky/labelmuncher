import {
	BasicHostList,
	createDataPlaneClient,
	type Database,
	type DataPlaneClient,
} from "@atproto/bsky";
import { decodeFirst } from "@atcute/cbor";
import { LabelValidator, type LabelValidatorOptions } from "./labelValidator.ts";
import { WebSocket } from "partysocket";
import type { StateStore } from "./state.ts";
import { type ComAtprotoLabelDefs, ComAtprotoLabelSubscribeLabels } from "@atcute/atproto";
import { is } from "@atcute/lexicons";
import { Timestamp } from "@bufbuild/protobuf";

export interface LabelerSubscription {
	did: string;
	ws: WebSocket | null;
	reconnectAttempts: number;
	isConnected: boolean;
}

export interface LabelerSubscriberOptions extends LabelValidatorOptions {
	modServiceDid?: string;
	dataplaneUrls?: string[];
	dataplaneHttpVersion?: "1.1" | "2";
}

export class LabelerSubscriber {
	validator: LabelValidator;
	protected subscriptions = new Map<string, LabelerSubscription>();
	protected state: StateStore;
	protected db: Database;
	protected dataplane?: DataPlaneClient;
	protected maxReconnectAttempts = 10;
	protected reconnectDelay = 5000;

	constructor(options: LabelerSubscriberOptions) {
		this.state = options.state;
		this.db = options.db;
		if (options.dataplaneUrls?.length) {
			this.dataplane = createDataPlaneClient(new BasicHostList(options.dataplaneUrls), {
				httpVersion: options.dataplaneHttpVersion || "1.1",
			});
		}
		this.validator = new LabelValidator(options);
	}

	async subscribeToLabelers(dids: string[]) {
		for (const did of dids) {
			await this.subscribeToLabeler(did);
		}
	}

	protected async subscribeToLabeler(did: string) {
		try {
			const didData = await this.validator.fetchDidDocument(did);
			if (!didData?.serviceEndpoint) {
				console.warn(`no service endpoint found for labeler ${did}`);
				return;
			}

			const cursor = this.state.getCursor(did) || 0;
			if (cursor === 0) console.log(`starting from the beginning for ${did}`);

			const url = new URL("/xrpc/com.atproto.label.subscribeLabels", didData.serviceEndpoint);
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
				if (this.dataplane && label.val === "!takedown") {
					await this.handleTakedown(label);
				}
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

	protected async handleTakedown(label: ComAtprotoLabelDefs.Label) {
		const { uri, neg, cts, src } = label;
		const ref = `BSKY-TAKEDOWN-${cts.replaceAll(/[^[0-9a-zA-Z]/g, "")}`;
		const now = new Date();
		try {
			if (uri.startsWith("did:")) {
				if (!neg) {
					await this.dataplane?.takedownActor({
						did: uri,
						ref,
						seen: Timestamp.fromDate(now),
					});
				} else {
					await this.dataplane?.untakedownActor({
						did: uri,
						seen: Timestamp.fromDate(now),
					});
				}
			} else if (uri.startsWith("at://")) {
				if (!neg) {
					await this.dataplane?.takedownRecord({
						recordUri: uri,
						ref,
						seen: Timestamp.fromDate(now),
					});
				} else {
					await this.dataplane?.untakedownRecord({
						recordUri: uri,
						seen: Timestamp.fromDate(now),
					});
				}
			} else {
				throw "invalid subject";
			}
		} catch (error) {
			console.error(`error processing takedown for ${uri} from ${src}:`, error);
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
				$type: `com.atproto.label.subscribeLabels${t}`,
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
