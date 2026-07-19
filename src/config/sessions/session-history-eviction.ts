import { executeSqliteQuerySync } from "../../infra/kysely-sync.js";
import {
  collectActiveSessionWorkAdmissionIdentities,
  runExclusiveSessionLifecycleMutation,
} from "../../sessions/session-lifecycle-admission.js";
import {
  openOpenClawAgentDatabase,
  runOpenClawAgentWriteTransaction,
  type OpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import {
  hasRetainedSessionTranscriptArchives,
  measureSessionPhysicalDiskUsage,
  pruneSessionTranscriptArchivesToHighWater,
  type SessionDiskBudgetSweepResult,
} from "./disk-budget.js";
import { materializeSqliteSessionStateDeletePlans } from "./session-accessor.sqlite-archive.js";
import { emitArchivedSqliteTranscriptUpdates } from "./session-accessor.sqlite-events.js";
import {
  collectSqliteSessionStateIdsForEntry,
  deleteMaterializedSqliteSessionStatePlans,
  planSqliteSessionStateDeleteIfUnreferenced,
  readReferencedSqliteSessionIds,
} from "./session-accessor.sqlite-lifecycle-state.js";
import {
  getSessionKysely,
  resolveSqliteScope,
  resolveSqliteTranscriptArchiveDirectory,
  runExclusiveSqliteSessionWrite,
  toDatabaseOptions,
} from "./session-accessor.sqlite-scope.js";
import { parseSqliteSessionEntryJson } from "./session-accessor.sqlite-status.js";
import { normalizeStoreSessionKey } from "./store-entry.js";
import type { ResolvedSessionMaintenanceConfig } from "./store-maintenance.js";

type SessionHistoryDiskBudgetParams = {
  agentId?: string;
  mode: ResolvedSessionMaintenanceConfig["mode"];
  storePath: string;
  maintenance: Pick<ResolvedSessionMaintenanceConfig, "highWaterBytes" | "maxDiskBytes">;
};

function createPhysicalBudgetResult(params: {
  totalBytesBefore: number;
  totalBytesAfter?: number;
  removedEntries?: number;
  removedFiles?: number;
  maxBytes: number;
  highWaterBytes: number;
}): SessionDiskBudgetSweepResult {
  const totalBytesAfter = params.totalBytesAfter ?? params.totalBytesBefore;
  return {
    totalBytesBefore: params.totalBytesBefore,
    totalBytesAfter,
    removedFiles: params.removedFiles ?? 0,
    removedEntries: params.removedEntries ?? 0,
    freedBytes: Math.max(0, params.totalBytesBefore - totalBytesAfter),
    maxBytes: params.maxBytes,
    highWaterBytes: params.highWaterBytes,
    overBudget: params.totalBytesBefore > params.maxBytes,
  };
}

export type SessionHistoryDiskBudgetInspection = {
  diskBudget: SessionDiskBudgetSweepResult | null;
  wouldMutate: boolean;
};

/** Reports the same physical total enforce mode compares, without projecting logical row bytes. */
export async function inspectSqliteSessionHistoryDiskBudget(
  params: SessionHistoryDiskBudgetParams,
): Promise<SessionHistoryDiskBudgetInspection> {
  const { highWaterBytes, maxDiskBytes } = params.maintenance;
  if (maxDiskBytes == null || highWaterBytes == null) {
    return { diskBudget: null, wouldMutate: false };
  }
  const usage = await measureSessionPhysicalDiskUsage(params.storePath);
  const diskBudget = createPhysicalBudgetResult({
    totalBytesBefore: usage.totalBytes,
    maxBytes: maxDiskBytes,
    highWaterBytes,
  });
  if (!diskBudget.overBudget || params.mode !== "enforce") {
    return { diskBudget, wouldMutate: false };
  }
  const resolved = resolveSqliteScope({
    ...(params.agentId ? { agentId: params.agentId } : {}),
    sessionKey: "",
    storePath: params.storePath,
  });
  const database = openOpenClawAgentDatabase(toDatabaseOptions(resolved));
  const hasHistoricalSession =
    readHistoricalSessionIds({
      database,
      protectedSessionIds: collectProtectedHistoricalSessionIds({
        database,
        storePath: params.storePath,
      }),
    }).length > 0;
  return {
    diskBudget,
    wouldMutate:
      hasHistoricalSession || (await hasRetainedSessionTranscriptArchives(params.storePath)),
  };
}

function collectProtectedHistoricalSessionIds(params: {
  database: OpenClawAgentDatabase;
  storePath: string;
}): Set<string> {
  const protectedSessionIds = readReferencedSqliteSessionIds(params.database);
  const admissionIdentities = collectActiveSessionWorkAdmissionIdentities(params.storePath);
  if (admissionIdentities.size === 0) {
    return protectedSessionIds;
  }

  // Admissions may carry either the backing session id or its live session key. Protect both,
  // then resolve admitted keys through their entries so cleanup cannot reclaim active work.
  for (const identity of admissionIdentities) {
    protectedSessionIds.add(identity);
  }
  const normalizedAdmissionKeys = new Set(
    [...admissionIdentities].map((identity) => normalizeStoreSessionKey(identity)),
  );
  const db = getSessionKysely(params.database.db);
  const rows = executeSqliteQuerySync(
    params.database.db,
    db.selectFrom("session_entries").select(["entry_json", "session_id", "session_key"]),
  ).rows;
  for (const row of rows) {
    if (!normalizedAdmissionKeys.has(normalizeStoreSessionKey(row.session_key))) {
      continue;
    }
    protectedSessionIds.add(row.session_id);
    const entry = parseSqliteSessionEntryJson(row);
    if (entry) {
      for (const sessionId of collectSqliteSessionStateIdsForEntry(entry)) {
        protectedSessionIds.add(sessionId);
      }
    }
  }
  return protectedSessionIds;
}

function readHistoricalSessionIds(params: {
  database: OpenClawAgentDatabase;
  protectedSessionIds: ReadonlySet<string>;
}): string[] {
  const db = getSessionKysely(params.database.db);
  return executeSqliteQuerySync(
    params.database.db,
    db
      .selectFrom("sessions")
      .select("session_id")
      .orderBy("updated_at", "asc")
      .orderBy("session_id", "asc"),
  ).rows.flatMap((row) => (params.protectedSessionIds.has(row.session_id) ? [] : [row.session_id]));
}

function reclaimSqliteFreePages(database: OpenClawAgentDatabase): void {
  // Committed row deletion first lands in the WAL. TRUNCATE makes that shrink immediately;
  // incremental vacuum can then return free tail pages from the main file without a rewrite.
  database.walMaintenance.checkpoint();
  const row = database.db.prepare("PRAGMA freelist_count").get() as
    | { freelist_count?: unknown }
    | undefined;
  const freePages = Number(row?.freelist_count ?? 0);
  if (Number.isSafeInteger(freePages) && freePages > 0) {
    database.db.exec(`PRAGMA incremental_vacuum(${freePages});`);
  }
  database.walMaintenance.checkpoint();
}

/** Extracts historical sessions durably before reclaiming their SQLite rows. */
export async function enforceSqliteSessionHistoryDiskBudget(
  params: SessionHistoryDiskBudgetParams,
): Promise<SessionDiskBudgetSweepResult | null> {
  const { highWaterBytes, maxDiskBytes } = params.maintenance;
  if (maxDiskBytes == null || highWaterBytes == null) {
    return null;
  }
  const initialUsage = await measureSessionPhysicalDiskUsage(params.storePath);
  if (initialUsage.totalBytes <= maxDiskBytes || params.mode === "warn") {
    return createPhysicalBudgetResult({
      totalBytesBefore: initialUsage.totalBytes,
      maxBytes: maxDiskBytes,
      highWaterBytes,
    });
  }

  const resolved = resolveSqliteScope({
    ...(params.agentId ? { agentId: params.agentId } : {}),
    sessionKey: "",
    storePath: params.storePath,
  });
  const database = openOpenClawAgentDatabase(toDatabaseOptions(resolved));
  const archiveDirectory = resolveSqliteTranscriptArchiveDirectory(resolved);
  let usage = await runExclusiveSqliteSessionWrite(resolved, async () => {
    reclaimSqliteFreePages(database);
    return await measureSessionPhysicalDiskUsage(params.storePath);
  });
  let removedEntries = 0;
  let removedFiles = 0;
  if (usage.totalBytes > highWaterBytes) {
    const archiveSweep = await pruneSessionTranscriptArchivesToHighWater({
      highWaterBytes,
      storePath: params.storePath,
    });
    removedFiles = archiveSweep.removedFiles;
    usage = archiveSweep.usage;
  }
  const candidates = readHistoricalSessionIds({
    database,
    protectedSessionIds: collectProtectedHistoricalSessionIds({
      database,
      storePath: params.storePath,
    }),
  });

  for (const sessionId of candidates) {
    if (usage.totalBytes <= highWaterBytes) {
      break;
    }
    const eviction = await runExclusiveSessionLifecycleMutation({
      scope: params.storePath,
      identities: [sessionId],
      run: async () =>
        await runExclusiveSqliteSessionWrite(resolved, async () => {
          const protectedBeforeArchive = collectProtectedHistoricalSessionIds({
            database,
            storePath: params.storePath,
          });
          const plan = planSqliteSessionStateDeleteIfUnreferenced({
            archiveDirectory,
            archiveTranscript: true,
            database,
            reason: "deleted",
            referencedSessionIds: protectedBeforeArchive,
            sessionId,
          });
          if (!plan) {
            return null;
          }

          // Extract-before-delete is the retention invariant. Admission is fenced across archive
          // creation, then rechecked inside the write transaction before any rows are reclaimed.
          const materialized = materializeSqliteSessionStateDeletePlans([plan]);
          let deleted = false;
          let archivedTranscripts: ReturnType<typeof deleteMaterializedSqliteSessionStatePlans> =
            [];
          runOpenClawAgentWriteTransaction((transactionDb) => {
            const protectedAtDelete = collectProtectedHistoricalSessionIds({
              database: transactionDb,
              storePath: params.storePath,
            });
            archivedTranscripts = deleteMaterializedSqliteSessionStatePlans(
              transactionDb,
              materialized,
              protectedAtDelete,
            );
            const db = getSessionKysely(transactionDb.db);
            deleted =
              executeSqliteQuerySync(
                transactionDb.db,
                db.selectFrom("sessions").select("session_id").where("session_id", "=", sessionId),
              ).rows.length === 0;
          }, toDatabaseOptions(resolved));
          if (!deleted) {
            return null;
          }
          reclaimSqliteFreePages(database);
          return {
            archivedTranscripts,
            usage: await measureSessionPhysicalDiskUsage(params.storePath),
          };
        }),
    });
    if (!eviction) {
      continue;
    }
    removedEntries += 1;
    emitArchivedSqliteTranscriptUpdates(eviction.archivedTranscripts);
    usage = eviction.usage;
  }

  return createPhysicalBudgetResult({
    totalBytesBefore: initialUsage.totalBytes,
    totalBytesAfter: usage.totalBytes,
    removedEntries,
    removedFiles,
    maxBytes: maxDiskBytes,
    highWaterBytes,
  });
}
