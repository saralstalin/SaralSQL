const esbuild = require("esbuild");

const production = process.argv.includes('--production');
const watch = process.argv.includes('--watch');

/**
 * @type {import('esbuild').Plugin}
 */
const esbuildProblemMatcherPlugin = {
	name: 'esbuild-problem-matcher',

	setup(build) {
		build.onStart(() => {
			console.log('[watch] build started');
		});
		build.onEnd((result) => {
			result.errors.forEach(({ text, location }) => {
				console.error(`✘ [ERROR] ${text}`);
				if (location) {
					console.error(`    ${location.file}:${location.line}:${location.column}:`);
				}
			});
			console.log('[watch] build finished');
		});
	},
};

const common = {
	bundle: true,
	format: 'cjs',
	minify: production,
	sourcemap: true,          // always generate maps
	sourcesContent: true,     // embed original TS sources into the map
	platform: 'node',
	external: ['vscode'],
	logLevel: 'silent',
	plugins: [esbuildProblemMatcherPlugin],
};

async function main() {
	// Client build (extension entrypoint)
	const clientConfig = {
		...common,
		entryPoints: ['src/extension.ts'],
		outfile: 'dist/extension.js',
	};

	// Server build (language server entrypoint)
	const serverConfig = {
		...common,
		entryPoints: ['server/src/server.ts'],
		outfile: 'dist/server.js',
	};

	// Contexts for watch mode
	const clientCtx = await esbuild.context(clientConfig);
	const serverCtx = await esbuild.context(serverConfig);

	if (watch) {
		await clientCtx.watch();
		await serverCtx.watch();
	} else {
		await clientCtx.rebuild();
		await serverCtx.rebuild();
		clientCtx.dispose();
		serverCtx.dispose();
	}
}

main().catch(e => {
	console.error(e);
	process.exit(1);
});
