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

	const shutdown = async (signal: string) => {
		console.log(`received ${signal}, shutting down`);
		try {
			await service.stop();
			Deno.exit(0);
		} catch (error) {
			console.error(
				"error during shutdown:",
				error,
			);
			Deno.exit(1);
		}
	};

	Deno.addSignalListener("SIGINT", () => shutdown("SIGINT"));
	Deno.addSignalListener("SIGTERM", () => shutdown("SIGTERM"));

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
