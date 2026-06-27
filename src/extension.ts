import * as path from 'path';
import * as vscode from 'vscode';
import {
	LanguageClient,
	LanguageClientOptions,
	ServerOptions,
	TransportKind
} from 'vscode-languageclient/node';

let client: LanguageClient;

export function activate(context: vscode.ExtensionContext) {
	const serverModule = context.asAbsolutePath(
		path.join('dist', 'server.js') // ✅ matches esbuild output
	);

	const serverOptions: ServerOptions = {
		run: { module: serverModule, transport: TransportKind.ipc },
		debug: {
			module: serverModule,
			transport: TransportKind.ipc,
			options: { execArgv: ['--nolazy', '--inspect=6009'] }
		}
	};

	const clientOptions: LanguageClientOptions = {
		documentSelector: [{ scheme: 'file', language: 'sql' }],
		synchronize: {
			fileEvents: [
				vscode.workspace.createFileSystemWatcher('**/*.sql'),
				vscode.workspace.createFileSystemWatcher('**/*.sqlproj')
			],
			configurationSection: "saralsql" 
		},
		// ✅ Explicitly advertise support for workspace folders
		initializationOptions: {},
	
	};

	// Add workspace capability
	(clientOptions as any).capabilities = {
		workspace: {
			workspaceFolders: true
		}
	};

	client = new LanguageClient(
		'saralsql',
		'SaralSQL Language Server',
		serverOptions,
		clientOptions
	);

	context.subscriptions.push(
		vscode.commands.registerCommand(
			'saralsql.showReferences',
			async (uriString: string, position: { line: number; character: number }, locations: any[]) => {
				const uri = vscode.Uri.parse(uriString);
				const pos = new vscode.Position(position.line, position.character);
				const refs = (locations ?? []).map((loc) => new vscode.Location(
					vscode.Uri.parse(loc.uri),
					new vscode.Range(
						new vscode.Position(loc.range.start.line, loc.range.start.character),
						new vscode.Position(loc.range.end.line, loc.range.end.character)
					)
				));

				await vscode.commands.executeCommand('editor.action.showReferences', uri, pos, refs);
			}
		)
	);

	client.start();
}

export function deactivate(): Thenable<void> | undefined {
	return client ? client.stop() : undefined;
}
