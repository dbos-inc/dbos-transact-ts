export function getApplicationVersion(): string | undefined {
    return applicationVersion;
}

export function setApplicationVersion(version: string): void {
    applicationVersion = version;
}

let applicationVersion: string | undefined = undefined;

