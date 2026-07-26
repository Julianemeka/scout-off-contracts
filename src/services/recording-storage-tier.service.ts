/**
 * RecordingStorageTierService — Issue #748
 *
 * Manages S3 lifecycle rules for session recordings and keeps the
 * database storage_tier field in sync with actual S3 storage classes.
 *
 * Tier mapping:
 *   0–30 days   → STANDARD             (hot storage)
 *   30–90 days  → STANDARD_IA          (infrequent access)
 *   90–365 days → GLACIER_IR           (Glacier Instant Retrieval)
 *   365+ days   → DEEP_ARCHIVE         (Glacier Deep Archive)
 *   > RECORDING_RETENTION_DAYS → delete
 *
 * Glacier restore flow:
 *   1. User requests playback of an archived recording.
 *   2. Service detects storage_tier is 'glacier' or 'deep_archive'.
 *   3. Calls RestoreObjectCommand, marks status = 'restoring'.
 *   4. Notifies user via email that restore is in progress.
 *   5. S3 event (or polling job) calls completeGlacierRestore() on completion.
 */

import {
  S3Client,
  PutBucketLifecycleConfigurationCommand,
  HeadObjectCommand,
  RestoreObjectCommand,
  GetObjectCommand,
  HeadObjectCommandOutput,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import pool from "../config/database";
import { env } from "../config/env";
import { logger } from "../utils/logger";
import { EmailService } from "./email.service";

// ─── Types ────────────────────────────────────────────────────────────────────

export type StorageTier =
  | "standard"
  | "infrequent_access"
  | "glacier"
  | "deep_archive";

export interface StorageCostProjection {
  tier: StorageTier;
  recordingCount: number;
  totalSizeGb: number;
  estimatedMonthlyCostUsd: number;
}

export interface TieredAccessResult {
  available: boolean;
  tier: StorageTier;
  /** Present when available === true */
  playbackUrl?: string;
  /** Present when available === false (restore triggered) */
  restorationStatus?: "restoring" | "restore_requested";
  estimatedRestoreHours?: number;
}

// ─── Constants ────────────────────────────────────────────────────────────────

/** S3 storage class strings returned by HeadObject */
const STORAGE_CLASS_TO_TIER: Record<string, StorageTier> = {
  STANDARD: "standard",
  STANDARD_IA: "infrequent_access",
  GLACIER: "glacier",
  GLACIER_IR: "glacier",
  DEEP_ARCHIVE: "deep_archive",
};

/** Approximate USD/GB/month for each tier (AWS us-east-1 pricing) */
const TIER_COST_PER_GB: Record<StorageTier, number> = {
  standard: 0.023,
  infrequent_access: 0.0125,
  glacier: 0.004,
  deep_archive: 0.00099,
};

/** Estimated restore hours per tier */
const RESTORE_HOURS: Record<"glacier" | "deep_archive", number> = {
  glacier: 1,       // Glacier Instant Retrieval — minutes, shown as <1h
  deep_archive: 12, // Glacier Deep Archive standard retrieval
};

const BYTES_PER_GB = 1024 * 1024 * 1024;

// ─── S3 client ────────────────────────────────────────────────────────────────

const s3 = new S3Client({
  region: env.AWS_REGION,
  credentials: {
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
  },
});

const BUCKET = env.AWS_S3_BUCKET;
const RETENTION_DAYS = parseInt(env.RECORDING_RETENTION_DAYS, 10) || 90;

// ─── Service ──────────────────────────────────────────────────────────────────

export const RecordingStorageTierService = {
  /**
   * Apply (or update) the S3 bucket lifecycle configuration.
   * Should be called once on deployment (or on admin request).
   *
   * Creates rules:
   *   recordings/* → IA at 30 days
   *   recordings/* → Glacier IR at 90 days
   *   recordings/* → Deep Archive at 365 days
   *   recordings/* → Delete at RECORDING_RETENTION_DAYS
   */
  async applyLifecyclePolicy(): Promise<void> {
    logger.info("[StorageTier] Applying S3 lifecycle policy", {
      bucket: BUCKET,
      retentionDays: RETENTION_DAYS,
    });

    const command = new PutBucketLifecycleConfigurationCommand({
      Bucket: BUCKET,
      LifecycleConfiguration: {
        Rules: [
          {
            ID: "recordings-tiering",
            Status: "Enabled",
            Filter: { Prefix: "recordings/" },
            Transitions: [
              {
                Days: 30,
                StorageClass: "STANDARD_IA",
              },
              {
                Days: 90,
                StorageClass: "GLACIER_IR",
              },
              {
                Days: 365,
                StorageClass: "DEEP_ARCHIVE",
              },
            ],
            Expiration: {
              Days: RETENTION_DAYS,
            },
          },
        ],
      },
    });

    await s3.send(command);
    logger.info("[StorageTier] S3 lifecycle policy applied successfully");
  },

  /**
   * Sync the storage_tier column for all non-deleted recordings.
   * Queries S3 HeadObject for each recording and updates the DB.
   * Called weekly by the recording cleanup job.
   *
   * Processes in batches of 50 to avoid overwhelming S3.
   */
  async syncStorageTiers(): Promise<{ updated: number; failed: number }> {
    logger.info("[StorageTier] Starting storage tier sync");

    const { rows: recordings } = await pool.query<{
      id: string;
      s3_key: string;
      s3_bucket: string;
    }>(
      `SELECT id, s3_key, s3_bucket
       FROM session_recordings
       WHERE status NOT IN ('deleted', 'failed')
       ORDER BY created_at DESC`,
    );

    let updated = 0;
    let failed = 0;
    const BATCH = 50;

    for (let i = 0; i < recordings.length; i += BATCH) {
      const batch = recordings.slice(i, i + BATCH);

      await Promise.all(
        batch.map(async (rec) => {
          try {
            const tier = await this._getS3StorageTier(rec.s3_key);
            if (tier) {
              await pool.query(
                `UPDATE session_recordings SET storage_tier = $1, updated_at = NOW() WHERE id = $2`,
                [tier, rec.id],
              );
              updated++;
            }
          } catch (err) {
            failed++;
            logger.warn("[StorageTier] Failed to sync tier for recording", {
              recordingId: rec.id,
              s3Key: rec.s3_key,
              error: err instanceof Error ? err.message : String(err),
            });
          }
        }),
      );
    }

    logger.info("[StorageTier] Storage tier sync complete", { updated, failed });
    return { updated, failed };
  },

  /**
   * Check the tier of a recording before serving playback.
   * - STANDARD / STANDARD_IA → generate presigned URL immediately.
   * - GLACIER / DEEP_ARCHIVE → trigger restore, return 202-style result.
   *
   * @param recordingId  DB id
   * @param userId       Requesting user (for notification)
   * @param expiresIn    URL expiry seconds (default 3600)
   */
  async checkAndHandleTieredAccess(
    recordingId: string,
    userId: string,
    expiresIn = 3600,
  ): Promise<TieredAccessResult> {
    const { rows } = await pool.query<{
      s3_key: string;
      storage_tier: StorageTier | null;
      status: string;
    }>(
      `SELECT s3_key, storage_tier, status FROM session_recordings WHERE id = $1`,
      [recordingId],
    );

    if (!rows.length) throw new Error("Recording not found");
    const rec = rows[0];

    // If already restoring, report status
    if (rec.status === "restoring") {
      return {
        available: false,
        tier: rec.storage_tier ?? "glacier",
        restorationStatus: "restoring",
        estimatedRestoreHours:
          RESTORE_HOURS[(rec.storage_tier as "glacier" | "deep_archive") ?? "glacier"],
      };
    }

    // Refresh tier from S3 for accuracy
    const liveTier = await this._getS3StorageTier(rec.s3_key);
    const tier = liveTier ?? rec.storage_tier ?? "standard";

    // Update DB if tier has changed
    if (liveTier && liveTier !== rec.storage_tier) {
      await pool.query(
        `UPDATE session_recordings SET storage_tier = $1, updated_at = NOW() WHERE id = $2`,
        [liveTier, recordingId],
      );
    }

    if (tier === "standard" || tier === "infrequent_access") {
      const url = await getSignedUrl(
        s3,
        new GetObjectCommand({ Bucket: BUCKET, Key: rec.s3_key }),
        { expiresIn },
      );
      return { available: true, tier, playbackUrl: url };
    }

    // Glacier or Deep Archive — initiate restore
    await this.requestGlacierRestore(recordingId, rec.s3_key, tier as "glacier" | "deep_archive", userId);

    return {
      available: false,
      tier,
      restorationStatus: "restore_requested",
      estimatedRestoreHours: RESTORE_HOURS[tier as "glacier" | "deep_archive"],
    };
  },

  /**
   * Initiate Glacier restoration and update recording status.
   */
  async requestGlacierRestore(
    recordingId: string,
    s3Key: string,
    tier: "glacier" | "deep_archive",
    requestedByUserId: string,
  ): Promise<void> {
    logger.info("[StorageTier] Requesting Glacier restore", {
      recordingId,
      s3Key,
      tier,
    });

    const tierDays = tier === "deep_archive" ? 1 : 1; // restore for 1 day

    try {
      await s3.send(
        new RestoreObjectCommand({
          Bucket: BUCKET,
          Key: s3Key,
          RestoreRequest: {
            Days: tierDays,
            GlacierJobParameters: {
              Tier: tier === "deep_archive" ? "Standard" : "Expedited",
            },
          },
        }),
      );
    } catch (err: any) {
      // RestoreAlreadyInProgress is acceptable
      if (err?.Code !== "RestoreAlreadyInProgress") {
        throw err;
      }
    }

    // Mark status as restoring
    await pool.query(
      `UPDATE session_recordings
       SET status = 'restoring', updated_at = NOW()
       WHERE id = $1`,
      [recordingId],
    );

    // Notify user
    await this._notifyRestoreInitiated(requestedByUserId, recordingId, tier);
  },

  /**
   * Called when S3 event confirms restore is complete.
   * Marks recording as ready again and notifies user.
   */
  async completeGlacierRestore(
    recordingId: string,
    userId: string,
  ): Promise<void> {
    await pool.query(
      `UPDATE session_recordings SET status = 'ready', updated_at = NOW() WHERE id = $1`,
      [recordingId],
    );

    await this._notifyRestoreComplete(userId, recordingId);
    logger.info("[StorageTier] Glacier restore completed", { recordingId });
  },

  /**
   * GET /api/v1/admin/recordings/storage-costs
   *
   * Returns estimated monthly cost by tier based on actual DB records.
   */
  async getStorageCostProjection(): Promise<StorageCostProjection[]> {
    const { rows } = await pool.query<{
      storage_tier: StorageTier | null;
      count: string;
      total_bytes: string;
    }>(
      `SELECT
         COALESCE(storage_tier, 'standard') AS storage_tier,
         COUNT(*)                           AS count,
         COALESCE(SUM(file_size), 0)        AS total_bytes
       FROM session_recordings
       WHERE status NOT IN ('deleted', 'failed')
       GROUP BY COALESCE(storage_tier, 'standard')`,
    );

    return rows.map((row) => {
      const tier = (row.storage_tier ?? "standard") as StorageTier;
      const totalSizeGb = Number(row.total_bytes) / BYTES_PER_GB;
      const costPerGb = TIER_COST_PER_GB[tier] ?? TIER_COST_PER_GB.standard;

      return {
        tier,
        recordingCount: parseInt(row.count, 10),
        totalSizeGb: parseFloat(totalSizeGb.toFixed(4)),
        estimatedMonthlyCostUsd: parseFloat(
          (totalSizeGb * costPerGb).toFixed(4),
        ),
      };
    });
  },

  // ─── Private helpers ──────────────────────────────────────────────────────

  /** Query S3 HeadObject and map storage class to tier. */
  async _getS3StorageTier(s3Key: string): Promise<StorageTier | null> {
    try {
      const head: HeadObjectCommandOutput = await s3.send(
        new HeadObjectCommand({ Bucket: BUCKET, Key: s3Key }),
      );
      const storageClass = head.StorageClass ?? "STANDARD";
      return STORAGE_CLASS_TO_TIER[storageClass] ?? "standard";
    } catch (err: any) {
      if (err?.$metadata?.httpStatusCode === 404) return null;
      throw err;
    }
  },

  async _notifyRestoreInitiated(
    userId: string,
    recordingId: string,
    tier: "glacier" | "deep_archive",
  ): Promise<void> {
    try {
      const hours = RESTORE_HOURS[tier];
      await EmailService.sendEmail({
        to: await this._getUserEmail(userId),
        subject: "Your session recording is being restored",
        template: "recording-restore-initiated",
        context: {
          recordingId,
          estimatedHours: hours,
          tier,
        },
      });
    } catch (err) {
      logger.warn("[StorageTier] Failed to send restore initiated email", {
        userId,
        recordingId,
        error: err,
      });
    }
  },

  async _notifyRestoreComplete(
    userId: string,
    recordingId: string,
  ): Promise<void> {
    try {
      await EmailService.sendEmail({
        to: await this._getUserEmail(userId),
        subject: "Your session recording is ready",
        template: "recording-restore-complete",
        context: { recordingId },
      });
    } catch (err) {
      logger.warn("[StorageTier] Failed to send restore complete email", {
        userId,
        recordingId,
        error: err,
      });
    }
  },

  async _getUserEmail(userId: string): Promise<string> {
    const { rows } = await pool.query<{ email: string }>(
      `SELECT email FROM users WHERE id = $1`,
      [userId],
    );
    return rows[0]?.email ?? "";
  },
};
