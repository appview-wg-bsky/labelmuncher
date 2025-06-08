import { LabelMuncher } from "./labelMuncher.ts";

async function main() {
	let service: LabelMuncher;

	try {
		service = LabelMuncher.fromEnvironment();
	} catch (error) {
		console.error(
			"failed to initialize muncher:",
			error,
		);
		Deno.exit(1);
	}

	try {
		await service.start();
	} catch (error) {
		console.error("service failed:", error);
		await service.stop();
		Deno.exit(1);
	}
}

if (import.meta.main) {
	main();
}
