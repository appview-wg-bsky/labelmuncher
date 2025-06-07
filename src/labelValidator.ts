import { encode, fromBytes } from "@atcute/cbor";
import {
	CompositeDidDocumentResolver,
	PlcDidDocumentResolver,
	WebDidDocumentResolver,
} from "@atcute/identity-resolver";
import { Client, simpleFetchHandler } from "@atcute/client";
import type { Database } from "@atproto/bsky";
import { verifySigWithDidKey } from "@atcute/crypto";
import type { StateStore } from "./state.ts";
import { is } from "@atcute/lexicons";
import { AppBskyLabelerService } from "@atcute/bluesky";
import type { ComAtprotoLabelDefs } from "@atcute/atproto";

export interface ValidationResult {
	valid: boolean;
	reason?: string;
}

export interface LabelValidatorOptions {
	plcUrl?: string;
	state: StateStore;
	db: Database;
}

export class LabelValidator {
	protected idResolver: CompositeDidDocumentResolver<"plc" | "web">;
	protected state: StateStore;
	protected db: Database;

	constructor({ plcUrl, state, db }: LabelValidatorOptions) {
		this.idResolver = new CompositeDidDocumentResolver({
			methods: {
				plc: new PlcDidDocumentResolver({ apiUrl: plcUrl ?? "https://plc.directory" }),
				web: new WebDidDocumentResolver(),
			},
		});
		this.state = state;
		this.db = db;
	}

	async validateLabel(label: ComAtprotoLabelDefs.Label, did: string): Promise<ValidationResult> {
		for (const key of ["src", "uri", "val", "cts"] as const) {
			if (!label?.[key]) {
				return { valid: false, reason: "label is missing required field " + key };
			}
		}

		if (label.src !== did) {
			return { valid: false, reason: "label source DID does not match provided DID" };
		}

		if (label.sig) {
			const sigValid = await this.verifySignature(label);
			if (!sigValid.valid) {
				return sigValid;
			}
		}

		const valValid = await this.validateLabelValue(label.src, label.val);
		if (!valValid.valid) {
			return valValid;
		}

		if (label.exp) {
			const expTime = new Date(label.exp);
			const now = new Date();
			if (expTime <= now) {
				return { valid: false, reason: "label has expired" };
			}
		}

		return { valid: true };
	}

	private async verifySignature(label: ComAtprotoLabelDefs.Label): Promise<ValidationResult> {
		if (!label.sig) {
			return { valid: false, reason: "no signature present" };
		}

		try {
			const publicKey = await this.getLabelerDidKey(label.src);
			if (!publicKey) {
				return { valid: false, reason: `could not resolve labeler ${label.src} public key` };
			}

			const labelForSigning: Record<string, unknown> = {};

			if (label.ver !== undefined) labelForSigning.ver = label.ver;
			labelForSigning.src = label.src;
			labelForSigning.uri = label.uri;
			if (label.cid !== undefined) labelForSigning.cid = label.cid;
			labelForSigning.val = label.val;
			if (label.neg !== undefined) labelForSigning.neg = label.neg;
			labelForSigning.cts = label.cts;
			if (label.exp !== undefined) labelForSigning.exp = label.exp;

			const cborBytes = encode(labelForSigning);
			const hashBuffer = await crypto.subtle.digest("SHA-256", cborBytes);
			const hashBytes = new Uint8Array(hashBuffer);

			const sigBytes = fromBytes(label.sig);

			const isSigValid = await verifySigWithDidKey(publicKey, sigBytes, hashBytes);

			if (!isSigValid) {
				const refreshedKey = await this.getLabelerDidKey(label.src, true);
				if (refreshedKey && refreshedKey !== publicKey) {
					const retryValid = await verifySigWithDidKey(refreshedKey, sigBytes, hashBytes);
					if (!retryValid) {
						return { valid: false, reason: "invalid signature after key refresh" };
					}
				} else {
					return { valid: false, reason: "invalid signature" };
				}
			}

			return { valid: true };
		} catch (error) {
			return {
				valid: false,
				reason: `signature verification error: ${
					error instanceof Error ? error.message : String(error)
				}`,
			};
		}
	}

	private async getLabelerDidKey(did: string, forceRefresh = false): Promise<string | null> {
		try {
			if (!forceRefresh) {
				const cached = this.state.getDidCache(did);
				if (cached) {
					return cached.public_key;
				}
			}

			const didDoc = await this.idResolver.resolve(did as `did:plc:${string}`, {
				noCache: forceRefresh,
			});
			if (!didDoc) {
				return null;
			}

			const labelKey = didDoc.verificationMethod?.find(
				(vm) => vm.id.endsWith("#atproto_label"),
			);

			if (!labelKey?.publicKeyMultibase) {
				return null;
			}

			const labelerService = didDoc.service?.find(
				(service) => service.id.endsWith("#atproto_labeler"),
			)?.serviceEndpoint;

			if (!labelerService || typeof labelerService !== "string") {
				return null;
			}

			this.state.setDidCache({
				did,
				public_key: labelKey.publicKeyMultibase,
				service_endpoint: labelerService,
				cached_at: Date.now(),
			});

			return labelKey.publicKeyMultibase;
		} catch (error) {
			console.error(`error resolving DID ${did}:`, error);
			return null;
		}
	}

	private async validateLabelValue(did: string, val: string): Promise<ValidationResult> {
		try {
			const cached = this.state.getServiceCache(did);
			let validValues: string[];

			if (cached) {
				validValues = cached.label_values;
			} else {
				const serviceRecord = await this.fetchServiceRecord(did);
				if (!serviceRecord) {
					return { valid: false, reason: "could not fetch labeler service record" };
				}

				validValues = serviceRecord.policies.labelValues || [];

				this.state.setServiceCache({
					did,
					label_values: validValues,
					cached_at: Date.now(),
				});
			}

			if (!validValues.includes(val)) {
				return { valid: false, reason: `label value '${val}' not in labeler's declared values` };
			}

			return { valid: true };
		} catch (error) {
			return {
				valid: false,
				reason: `error validating label value: ${
					error instanceof Error ? error.message : String(error)
				}`,
			};
		}
	}

	private async fetchServiceRecord(did: string): Promise<AppBskyLabelerService.Main | null> {
		try {
			// Get PDS from DID document
			const didData = await this.idResolver.resolve(did as `did:plc:${string}`);
			const pds = didData.service?.find(
				(service) => service.id.endsWith("#atproto_pds"),
			)?.serviceEndpoint;
			if (!pds || typeof pds !== "string") {
				throw new Error("no PDS found in DID document");
			}

			const client = new Client({
				handler: simpleFetchHandler({ service: pds }),
			});

			const response = await client.get("com.atproto.repo.getRecord", {
				params: {
					repo: did as `did:plc:${string}`,
					collection: "app.bsky.labeler.service",
					rkey: "self",
				},
			});

			if (!response.ok) {
				console.error(`failed to fetch service record for ${did}:`, response.data.error);
				return null;
			}

			if (!is(AppBskyLabelerService.mainSchema, response.data.value)) {
				console.error(`service record for ${did} does not match lexicon`);
				return null;
			}

			return response.data.value;
		} catch (error) {
			console.error(`error fetching service record for ${did}:`, error);
			return null;
		}
	}
}
