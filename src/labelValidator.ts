import { encode, fromBytes } from "@atcute/cbor";
import {
	CompositeDidDocumentResolver,
	PlcDidDocumentResolver,
	WebDidDocumentResolver,
} from "@atcute/identity-resolver";
import { Client, simpleFetchHandler } from "@atcute/client";
import type { Database } from "@atproto/bsky";
import { type FoundPublicKey, parsePublicMultikey, verifySig } from "@atcute/crypto";
import type { DidCacheRecord, StateStore } from "./state.ts";
import { is } from "@atcute/lexicons";
import { AppBskyLabelerService } from "@atcute/bluesky";
import type { ComAtprotoLabelDefs } from "@atcute/atproto";
import type { ServiceCacheRecord } from "./state.ts";

export const GLOBAL_LABEL_VALUES = ["porn", "sexual", "nudity", "graphic-media", "gore"];

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
				plc: new PlcDidDocumentResolver({
					apiUrl: plcUrl ?? "https://plc.directory",
				}),
				web: new WebDidDocumentResolver(),
			},
		});
		this.state = state;
		this.db = db;
	}

	async validateLabel(label: ComAtprotoLabelDefs.Label, did: string): Promise<ValidationResult> {
		for (const key of ["src", "uri", "val", "cts", "sig"] as const) {
			if (!label?.[key]) {
				return { valid: false, reason: "label is missing required field " + key };
			}
		}

		if (label.src !== did) {
			return { valid: false, reason: "label source DID does not match provided DID" };
		}

		const sigValid = await this.verifySignature(label);
		if (!sigValid.valid) {
			return sigValid;
		}

		const valValid = this.validateLabelValue(label.src, label.val);
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

	async verifySignature(label: ComAtprotoLabelDefs.Label): Promise<ValidationResult> {
		if (!label.sig) {
			return { valid: false, reason: "no signature present" };
		}

		try {
			const publicKey = this.getLabelerPubKey(label.src);
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

			const labelBytes = encode(labelForSigning);
			const sigBytes = fromBytes(label.sig);

			const isSigValid = await verifySig(publicKey, sigBytes, labelBytes);

			if (!isSigValid) {
				const refreshedKey = this.getLabelerPubKey(label.src);
				if (
					refreshedKey &&
					!refreshedKey.publicKeyBytes.every((b, i) => b === publicKey.publicKeyBytes[i])
				) {
					const retryValid = await verifySig(refreshedKey, sigBytes, labelBytes);
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

	validateLabelValue(did: string, val: string): ValidationResult {
		const { labelValues } = this.state.getServiceCache(did) ?? {};
		if (!labelValues) {
			return { valid: false, reason: "no label values found in service record cache" };
		}

		if (!labelValues.includes(val) && !GLOBAL_LABEL_VALUES.includes(val)) {
			return { valid: false, reason: `label value '${val}' not in labeler's declared values` };
		}

		return { valid: true };
	}

	getLabelerPubKey(did: string): FoundPublicKey | null {
		const publicKey = (this.state.getDidCache(did))?.publicKey;
		if (!publicKey) return null;
		return parsePublicMultikey(publicKey);
	}

	async fetchDidDocument(did: string): Promise<DidCacheRecord | null> {
		try {
			const didDoc = await this.idResolver.resolve(did as `did:plc:${string}`, { noCache: true });
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

			const didData: DidCacheRecord = {
				did,
				publicKey: labelKey.publicKeyMultibase,
				serviceEndpoint: labelerService,
				cachedAt: Date.now(),
			};
			this.state.setDidCache(didData);
			return didData;
		} catch (error) {
			console.error(`error resolving DID ${did}:`, error);
			return null;
		}
	}

	async fetchServiceRecord(did: string): Promise<ServiceCacheRecord | null> {
		try {
			const didData = await this.idResolver.resolve(did as `did:plc:${string}`, { noCache: true });
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

			const data = {
				did,
				labelValues: response.data.value.policies.labelValues || [],
				cachedAt: Date.now(),
			};
			this.state.setServiceCache(data);
			return data;
		} catch (error) {
			console.error(`error fetching service record for ${did}:`, error);
			return null;
		}
	}
}
