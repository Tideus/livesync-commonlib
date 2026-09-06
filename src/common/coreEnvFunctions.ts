// This module provides core environment functions that can be set by the
// host application (like Obsidian) and used across the library without
// direct dependencies on the host's APIs.
// For `features`, please implement service, feature, or, serviceFeature for the sake of
// robust architecture and dependency management. Only put truly core functions here that.

export type LanguageGetter = () => string;

let _getLanguage: LanguageGetter = () => "en";

export function setGetLanguage(func: LanguageGetter) {
    _getLanguage = func;
}

export function getLanguage() {
    return _getLanguage();
}

// Compatibility for globalThis across different environments (browser, Node.js, etc.)
export const compatGlobal = (
    typeof window !== "undefined"
        ? window
        : // compatibility for CLIs, tests, and other non-browser environments.
          // eslint-disable-next-line obsidianmd/no-global-this
          globalThis
) as typeof window;

export type CompatTimeoutHandle = ReturnType<typeof setTimeout> | number;
export type CompatIntervalHandle = ReturnType<typeof setInterval> | number;

/**
 * The default transport is the host's global fetch implementation. Obsidian
 * mobile runs inside Android WebView, where otherwise valid CouchDB requests
 * can fail at the WebView fetch/CORS layer even though Obsidian's native
 * requestUrl transport succeeds. Hosts may therefore replace this transport
 * while keeping the common library independent of any host-specific API.
 */
export type FetchFunction = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

const defaultFetch: FetchFunction = compatGlobal.fetch.bind(compatGlobal);
let activeFetch: FetchFunction = defaultFetch;

export const _fetch: FetchFunction = (input, init) => activeFetch(input, init);

export function setFetch(func: FetchFunction) {
    activeFetch = func;
}

export function resetFetch() {
    activeFetch = defaultFetch;
}

type ActiveDocumentWindow = typeof window & { activeDocument?: Document };
const activeDocumentWindow = compatGlobal as ActiveDocumentWindow;
export const _activeDocument: Document = activeDocumentWindow.activeDocument ?? activeDocumentWindow.document;
