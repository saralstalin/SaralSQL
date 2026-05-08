const esbuild = require("esbuild");

const production = process.argv.includes("--production");
const watch = process.argv.includes("--watch");

/**
 * @type {import('esbuild').Plugin}
 */
const esbuildProblemMatcherPlugin = {
    name: "esbuild-problem-matcher",

    setup(build) {
        build.onStart(() => {
            console.log("[watch] build started");
        });

        build.onEnd((result) => {
            result.errors.forEach(({ text, location }) => {
                console.error(`✘ [ERROR] ${text}`);
                if (location) {
                    console.error(
                        `    ${location.file}:${location.line}:${location.column}:`
                    );
                }
            });

            console.log("[watch] build finished");
        });
    },
};

const common = {
    bundle: true,
    format: "cjs",
    minify: production,
    sourcemap: true,
    sourcesContent: true,
    platform: "node",
    external: ["vscode"],
    logLevel: "info",
    plugins: [esbuildProblemMatcherPlugin],
};

async function main() {
    const clientCtx = await esbuild.context({
        ...common,
        entryPoints: ["src/extension.ts"],
        outfile: "dist/extension.js",
    });

    const serverCtx = await esbuild.context({
        ...common,
        entryPoints: ["server/src/server.ts"],
        outfile: "dist/server.js",
    });

    if (watch) {
        await clientCtx.watch();
        await serverCtx.watch();
    } else {
        await clientCtx.rebuild();
        await serverCtx.rebuild();

        await clientCtx.dispose();
        await serverCtx.dispose();
    }
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});