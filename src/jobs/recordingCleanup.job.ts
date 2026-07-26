/**
 * Recording Cleanup Job — Issues #449 & #748
 *
 * Runs daily via BullMQ repeatable job.
 *
 * Responsibilities:
 *  1. Delete expired recordings (older than RECORDING_RETENTION_DAYS) from S3 and DB.
 *  2. (Weekly) Sync storage_tier DB field with actual S3 storage class.
 *  3. (On first run) Apply S3 lifecycle policy to the recordings bucket.
 */

import { SessionRecordingService } from '../services/session-recording.service';
import { RecordingStorageTierService } from '../services/recording-storage-tier.service';
import { logger } from '../utils/logger.utils';

let lifecyclePolicyApplied = false;

/**
 * Main entry point — called by the BullMQ worker daily.
 */
export async function runRecordingCleanupJob(): Promise<void> {
  try {
    logger.info('[RecordingCleanup] Starting recording cleanup job');

    // ── 1. Apply S3 lifecycle policy once per process lifetime ────────────────
    if (!lifecyclePolicyApplied) {
      try {
        await RecordingStorageTierService.applyLifecyclePolicy();
        lifecyclePolicyApplied = true;
        logger.info('[RecordingCleanup] S3 lifecycle policy applied');
      } catch (err) {
        logger.warn('[RecordingCleanup] Failed to apply lifecycle policy (non-fatal)', {
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    // ── 2. Delete expired recordings ──────────────────────────────────────────
    const expiredRecordings = await SessionRecordingService.findExpiredRecordings();

    if (expiredRecordings.length === 0) {
      logger.info('[RecordingCleanup] No expired recordings found');
    } else {
      logger.info(`[RecordingCleanup] Found ${expiredRecordings.length} expired recordings`);

      let successCount = 0;
      let failureCount = 0;

      for (const recording of expiredRecordings) {
        try {
          await SessionRecordingService.cleanupExpiredRecording(recording.id);
          successCount++;
        } catch (error) {
          failureCount++;
          logger.error(`[RecordingCleanup] Failed to clean up recording ${recording.id}`, {
            sessionId: recording.session_id,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }

      logger.info('[RecordingCleanup] Expired recording cleanup complete', {
        total: expiredRecordings.length,
        success: successCount,
        failure: failureCount,
      });
    }

    // ── 3. Weekly storage tier sync ───────────────────────────────────────────
    const today = new Date();
    if (today.getDay() === 0) { // Sunday
      logger.info('[RecordingCleanup] Running weekly storage tier sync');
      try {
        const syncResult = await RecordingStorageTierService.syncStorageTiers();
        logger.info('[RecordingCleanup] Storage tier sync complete', syncResult);
      } catch (err) {
        logger.error('[RecordingCleanup] Storage tier sync failed', {
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    logger.info('[RecordingCleanup] Recording cleanup job finished');
  } catch (error) {
    logger.error('[RecordingCleanup] Recording cleanup job failed', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}
