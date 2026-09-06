import { _fetch } from "@lib/common/coreEnvFunctions";
import { LOG_LEVEL_VERBOSE, Logger } from "octagonal-wheels/common/logger";
import type { EntryDoc } from "@lib/common/models/db.definition";
import type { AnyEntry, EntryLeaf } from "@lib/common/models/db.type";

interface CouchChangeLine {
    seq: number | string;
    id: string;
    changes: Array<{ rev: string }>;
    doc?: EntryDoc;
    deleted?: boolean;
}

interface AnyDecryptedDoc {
    _id: string;
}

type DBSequence = number | string;

const FAST_FETCH_CHANGES_PAGE_LIMIT = 500;

export type StreamingFetchFailureStage = "transport" | "authentication" | "protocol" | "decryption" | "storage";

export class StreamingFetchFailure extends Error {
    override readonly name = "StreamingFetchFailure";

    constructor(
        readonly stage: StreamingFetchFailureStage,
        message: string,
        readonly retryable: boolean,
        options?: { status?: number; cause?: unknown }
    ) {
        super(message, options?.cause === undefined ? undefined : { cause: options.cause });
        this.status = options?.status;
    }

    readonly status?: number;
}

export function isRetryableStreamingFetchFailure(error: unknown): error is StreamingFetchFailure {
    return error instanceof StreamingFetchFailure && error.retryable;
}

function errorMessage(error: unknown): string {
    if (error instanceof Error && error.message) return error.message;
    return String(error);
}

function asStreamingFetchFailure(error: unknown): StreamingFetchFailure {
    if (error instanceof StreamingFetchFailure) return error;
    return new StreamingFetchFailure(
        "protocol",
        `Fast Fetch encountered an unexpected processing failure: ${errorMessage(error)}`,
        false,
        { cause: error }
    );
}

function transportFailure(operation: string, error: unknown): StreamingFetchFailure {
    return new StreamingFetchFailure("transport", `Fast Fetch could not ${operation}: ${errorMessage(error)}`, true, {
        cause: error,
    });
}

function responseFailure(response: Response, operation: string): StreamingFetchFailure {
    const status = response.status;
    if (status === 401 || status === 403) {
        return new StreamingFetchFailure(
            "authentication",
            `Fast Fetch could not ${operation}: CouchDB returned HTTP ${status}.`,
            false,
            { status }
        );
    }
    const retryable = status === 408 || status === 429 || status >= 500;
    return new StreamingFetchFailure(
        retryable ? "transport" : "protocol",
        `Fast Fetch could not ${operation}: CouchDB returned HTTP ${status}.`,
        retryable,
        { status }
    );
}

function ensureResponseOK(response: Response, operation: string): void {
    if (!response.ok) throw responseFailure(response, operation);
}

async function fetchResponse(url: string, init: RequestInit, operation: string): Promise<Response> {
    let response: Response;
    try {
        response = await _fetch(url, init);
    } catch (error) {
        throw transportFailure(operation, error);
    }
    ensureResponseOK(response, operation);
    return response;
}

async function readResponseText(response: Response, operation: string): Promise<string> {
    try {
        return await response.text();
    } catch (error) {
        throw transportFailure(operation, error);
    }
}

async function saveCheckpoint(
    onCheckpoint: ((sequence: DBSequence) => void | Promise<void>) | undefined,
    sequence: DBSequence
): Promise<void> {
    try {
        await onCheckpoint?.(sequence);
    } catch (error) {
        throw new StreamingFetchFailure(
            "storage",
            `Fast Fetch could not save its checkpoint: ${errorMessage(error)}`,
            false,
            { cause: error }
        );
    }
}

function generatePouchDBBatchWriter(
    downloadToDB: PouchDB.Database,
    decryptFunction: (doc: EntryDoc) => Promise<AnyEntry | EntryLeaf>,
    onCheckpoint?: (sequence: DBSequence) => void | Promise<void>
) {
    let batchBuffer: AnyDecryptedDoc[] = [];
    let currentBatchSizeBytes = 0;
    let batchLastSequence: DBSequence | undefined;

    const BATCH_ITEM_LIMIT = 100;
    const BATCH_SIZE_LIMIT = 2 * 1024 * 1024;

    const flush = async () => {
        if (batchBuffer.length === 0) return;

        const documents = batchBuffer;
        const checkpoint = batchLastSequence;
        let results: Array<PouchDB.Core.Response | PouchDB.Core.Error>;
        try {
            results = await downloadToDB.bulkDocs(documents, { new_edits: false });
        } catch (error) {
            throw new StreamingFetchFailure(
                "storage",
                `Fast Fetch could not write a batch to the local database: ${errorMessage(error)}`,
                false,
                { cause: error }
            );
        }

        const failedResult = results.find(
            (result): result is PouchDB.Core.Error => "error" in result && Boolean(result.error)
        );
        if (failedResult) {
            const detail = failedResult.message || failedResult.name || "the local database rejected a document";
            throw new StreamingFetchFailure(
                "storage",
                `Fast Fetch could not write a batch to the local database: ${detail}`,
                false,
                { cause: failedResult }
            );
        }

        if (checkpoint !== undefined) await saveCheckpoint(onCheckpoint, checkpoint);
        batchBuffer = [];
        currentBatchSizeBytes = 0;
        batchLastSequence = undefined;
    };

    return {
        async write(doc: EntryDoc, sequence: DBSequence) {
            let decryptedDoc: AnyEntry | EntryLeaf;
            if (doc._deleted) {
                decryptedDoc = doc as unknown as AnyEntry;
            } else {
                try {
                    decryptedDoc = await decryptFunction(doc);
                } catch (error) {
                    throw new StreamingFetchFailure(
                        "decryption",
                        `Fast Fetch could not decrypt a document: ${errorMessage(error)}`,
                        false,
                        { cause: error }
                    );
                }
            }

            let serialisedDoc: string;
            try {
                const serialised = JSON.stringify(decryptedDoc);
                if (serialised === undefined) throw new Error("the decrypted value is not a document");
                serialisedDoc = serialised;
            } catch (error) {
                throw new StreamingFetchFailure(
                    "decryption",
                    `Fast Fetch produced an invalid decrypted document: ${errorMessage(error)}`,
                    false,
                    { cause: error }
                );
            }

            batchBuffer.push(decryptedDoc);
            currentBatchSizeBytes += serialisedDoc.length;
            batchLastSequence = sequence;

            if (batchBuffer.length >= BATCH_ITEM_LIMIT || currentBatchSizeBytes >= BATCH_SIZE_LIMIT) {
                await flush();
            }
        },
        async flushThrough(sequence: DBSequence) {
            await flush();
            await saveCheckpoint(onCheckpoint, sequence);
        },
        flush,
        abort() {
            batchBuffer = [];
            currentBatchSizeBytes = 0;
            batchLastSequence = undefined;
        },
    };
}

function setParamsToURL(url: URL, params: Record<string, string>) {
    for (const [key, value] of Object.entries(params)) {
        url.searchParams.set(key, value);
    }
    return url;
}

export type FetchChangesForInitialSyncProgress = {
    totalFetched: number;
    totalValidFetched: number;
    targetSeq: number | string;
    docsToFetch: number;
    totalBytes: number;
};

type DatabaseSyncStatus = {
    last_seq?: DBSequence;
    pending?: number;
    results?: unknown[];
};

type NormalChangesPage = {
    last_seq?: DBSequence;
    pending?: number;
    results?: CouchChangeLine[];
};

function parseJSONResponse<T extends object>(source: string, operation: string): T {
    const trimmed = source.trim();
    if (!trimmed) {
        throw new StreamingFetchFailure("protocol", `Fast Fetch received an empty response while trying to ${operation}.`, false);
    }
    try {
        const parsed = JSON.parse(trimmed) as unknown;
        if (!parsed || typeof parsed !== "object") throw new Error("response is not an object");
        return parsed as T;
    } catch (error) {
        throw new StreamingFetchFailure(
            "protocol",
            `Fast Fetch received invalid JSON while trying to ${operation}.`,
            false,
            { cause: error }
        );
    }
}

/**
 * Fetches initial data from CouchDB using finite `feed=normal` pages and writes
 * it into PouchDB.
 *
 * This intentionally avoids consuming CouchDB's `feed=continuous` response via
 * ReadableStream. Android System WebView can leave `reader.read()` pending even
 * after the status probe succeeds, which stalls initial sync at 0/N documents.
 * Finite JSON responses use the same opaque `last_seq` checkpoint semantics but
 * are handled reliably by both browser fetch and Obsidian's mobile WebView.
 */
export async function fetchChangesForInitialSync(
    downloadToDB: PouchDB.Database,
    remoteDbUrl: string,
    authHeader: string,
    decryptFunction: (doc: EntryDoc) => Promise<AnyEntry | EntryLeaf>,
    since: number | string = "0",
    onProgress?: (progress: FetchChangesForInitialSyncProgress) => void,
    onCheckpoint?: (sequence: DBSequence) => void | Promise<void>,
    customHeaders?: Record<string, string>
): Promise<void> {
    let totalFetched = 0;
    let totalValidFetched = 0;
    let totalBytes = 0;

    const fetchHeaders = new Headers(customHeaders);
    fetchHeaders.set("Accept", "application/json");
    fetchHeaders.set("Authorization", authHeader);

    const commonParams = {
        style: "all_docs",
        conflicts: "true",
        revs: "true",
    } as const;

    const fetchStatus = async (statusSince: DBSequence | "now"): Promise<DatabaseSyncStatus> => {
        const statusURL = setParamsToURL(new URL(`${remoteDbUrl}/_changes`), {
            ...commonParams,
            feed: "normal",
            since: statusSince.toString(),
            limit: "1",
            include_docs: "false",
        });
        const response = await fetchResponse(
            statusURL.toString(),
            { method: "GET", headers: fetchHeaders },
            "read changes status"
        );
        return parseJSONResponse<DatabaseSyncStatus>(
            await readResponseText(response, "read changes status"),
            "read changes status"
        );
    };

    const targetStatus = await fetchStatus("now");
    const progressTargetSeq = targetStatus.last_seq;
    if (progressTargetSeq === undefined) {
        throw new StreamingFetchFailure(
            "protocol",
            "Fast Fetch could not obtain a changes progress target from CouchDB.",
            false
        );
    }

    const batchWriter = generatePouchDBBatchWriter(downloadToDB, decryptFunction, onCheckpoint);
    let docsToFetch = 0;
    let lastProgress = 0;
    let lastReportTime = Date.now();

    const reportProgress = (force = false) => {
        if (!force && totalFetched - lastProgress < 25 && Date.now() - lastReportTime < 2000) return;
        lastProgress = totalFetched;
        lastReportTime = Date.now();
        onProgress?.({
            totalFetched,
            totalValidFetched,
            targetSeq: progressTargetSeq,
            docsToFetch,
            totalBytes,
        });
    };

    const readAvailableChanges = async (pageSince: DBSequence): Promise<number> => {
        const status = await fetchStatus(pageSince);
        if (!Array.isArray(status.results)) {
            throw new StreamingFetchFailure(
                "protocol",
                "Fast Fetch received changes status without a valid results list.",
                false
            );
        }
        const pending = status.pending;
        if (typeof pending !== "number" || !Number.isSafeInteger(pending) || pending < 0) {
            throw new StreamingFetchFailure(
                "protocol",
                "Fast Fetch received changes status without a valid pending count.",
                false
            );
        }
        const available = status.results.length + pending;
        if (!Number.isSafeInteger(available)) {
            throw new StreamingFetchFailure(
                "protocol",
                "Fast Fetch received a changes count outside the supported range.",
                false
            );
        }
        return available;
    };

    const fetchPageNormal = async (pageSince: DBSequence, pageLimit: number): Promise<DBSequence> => {
        const changesURL = setParamsToURL(new URL(`${remoteDbUrl}/_changes`), {
            ...commonParams,
            feed: "normal",
            include_docs: "true",
            since: pageSince.toString(),
            limit: pageLimit.toString(),
        });

        const response = await fetchResponse(
            changesURL.toString(),
            { method: "GET", headers: fetchHeaders },
            "fetch a bounded normal changes page"
        );
        const raw = await readResponseText(response, "read a bounded normal changes page");
        totalBytes += new TextEncoder().encode(raw).byteLength;
        const page = parseJSONResponse<NormalChangesPage>(raw, "parse a bounded normal changes page");

        if (!Array.isArray(page.results)) {
            throw new StreamingFetchFailure(
                "protocol",
                "Fast Fetch normal changes page did not contain a valid results array.",
                false
            );
        }
        if (page.last_seq === undefined) {
            throw new StreamingFetchFailure(
                "protocol",
                "Fast Fetch normal changes page did not contain last_seq.",
                false
            );
        }
        if (page.results.length === 0) {
            throw new StreamingFetchFailure(
                "transport",
                "Fast Fetch received no rows after its status probe reported available changes.",
                true
            );
        }

        for (const change of page.results) {
            if (!change || typeof change !== "object" || change.seq === undefined) {
                throw new StreamingFetchFailure("protocol", "Fast Fetch received an invalid changes row.", false);
            }
            if (change.doc !== undefined && (!change.doc || typeof change.doc !== "object")) {
                throw new StreamingFetchFailure("protocol", "Fast Fetch received an invalid included document.", false);
            }

            totalFetched++;
            if (change.doc) {
                await batchWriter.write(change.doc, change.seq);
                totalValidFetched++;
            } else {
                await batchWriter.flushThrough(change.seq);
            }
            reportProgress();
        }

        await batchWriter.flush();
        await saveCheckpoint(onCheckpoint, page.last_seq);
        reportProgress(true);
        return page.last_seq;
    };

    try {
        let pageSince: DBSequence = since;
        let started = false;

        while (true) {
            const available = await readAvailableChanges(pageSince);
            docsToFetch = Math.max(docsToFetch, totalFetched + available);
            if (available === 0) break;

            if (!started) {
                started = true;
                Logger(
                    `Starting initial synchronisation. Current sequence: ${since}, Target sequence: ${progressTargetSeq}, Documents to fetch: ${docsToFetch}.`
                );
            }

            const pageLimit = Math.min(FAST_FETCH_CHANGES_PAGE_LIMIT, available);
            pageSince = await fetchPageNormal(pageSince, pageLimit);
        }

        if (started) {
            Logger("Fast Fetch is caught up and durable in the local database (normal-feed fallback).");
            reportProgress(true);
        } else {
            Logger("No changes remain for Fast Fetch.");
        }
    } catch (error) {
        const failure = asStreamingFetchFailure(error);
        if (failure.stage !== "storage") {
            try {
                await batchWriter.flush();
            } catch (flushError) {
                batchWriter.abort();
                throw asStreamingFetchFailure(flushError);
            }
        }
        batchWriter.abort();
        Logger(`Fast Fetch failed during ${failure.stage}.`, LOG_LEVEL_VERBOSE);
        Logger(failure, LOG_LEVEL_VERBOSE);
        throw failure;
    }
}
