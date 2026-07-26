/**
 * StorageCostsController — Issue #748
 *
 * GET  /api/v1/admin/recordings/storage-costs   — estimated monthly cost by tier
 * POST /api/v1/admin/recordings/lifecycle-policy — apply/update S3 lifecycle rules
 * POST /api/v1/admin/recordings/sync-tiers       — trigger manual tier sync job
 */

import { Response } from "express";
import { AuthenticatedRequest } from "../types/api.types";
import { ResponseUtil } from "../utils/response.utils";
import { asyncHandler } from "../utils/asyncHandler.utils";
import { RecordingStorageTierService } from "../services/recording-storage-tier.service";
import { logger } from "../utils/logger";

export const StorageCostsController = {
  /**
   * GET /api/v1/admin/recordings/storage-costs
   *
   * Returns per-tier recording count, total size (GB), and estimated
   * monthly USD cost based on current AWS S3 pricing.
   */
  getStorageCosts: asyncHandler(
    async (_req: AuthenticatedRequest, res: Response): Promise<void> => {
      const projections =
        await RecordingStorageTierService.getStorageCostProjection();

      const totalMonthlyCost = projections.reduce(
        (sum, p) => sum + p.estimatedMonthlyCostUsd,
        0,
      );

      ResponseUtil.success(
        res,
        {
          projections,
          totalEstimatedMonthlyCostUsd: parseFloat(totalMonthlyCost.toFixed(4)),
          pricingNote:
            "Estimates based on AWS S3 us-east-1 pricing. Actual bill may vary by region and data transfer costs.",
          tiers: {
            standard: "$0.023/GB/month (0–30 days)",
            infrequent_access: "$0.0125/GB/month (30–90 days)",
            glacier: "$0.004/GB/month (90–365 days)",
            deep_archive: "$0.00099/GB/month (365+ days)",
          },
        },
        "Storage cost projection",
      );
    },
  ),

  /**
   * POST /api/v1/admin/recordings/lifecycle-policy
   *
   * Apply or update the S3 bucket lifecycle configuration.
   * Idempotent — safe to call multiple times.
   */
  applyLifecyclePolicy: asyncHandler(
    async (_req: AuthenticatedRequest, res: Response): Promise<void> => {
      logger.info("[StorageCostsController] Applying S3 lifecycle policy");
      await RecordingStorageTierService.applyLifecyclePolicy();
      ResponseUtil.success(
        res,
        null,
        "S3 lifecycle policy applied successfully",
      );
    },
  ),

  /**
   * POST /api/v1/admin/recordings/sync-tiers
   *
   * Manually trigger a storage tier sync (normally runs weekly).
   */
  syncStorageTiers: asyncHandler(
    async (_req: AuthenticatedRequest, res: Response): Promise<void> => {
      logger.info("[StorageCostsController] Manual storage tier sync triggered");
      const result = await RecordingStorageTierService.syncStorageTiers();
      ResponseUtil.success(res, result, "Storage tier sync completed");
    },
  ),

  /**
   * POST /api/v1/admin/recordings/:recordingId/restore
   *
   * Admin-initiated Glacier restore for a specific recording.
   */
  restoreRecording: asyncHandler(
    async (req: AuthenticatedRequest, res: Response): Promise<void> => {
      const { recordingId } = req.params as Record<string, string>;
      const userId = (req.user as any)?.id ?? (req.user as any)?.userId;

      const result =
        await RecordingStorageTierService.checkAndHandleTieredAccess(
          recordingId,
          userId,
        );

      if (result.available) {
        ResponseUtil.success(res, result, "Recording is available for playback");
      } else {
        res.status(202).json({
          success: true,
          data: result,
          message: `Recording restoration initiated. Estimated time: ${result.estimatedRestoreHours}h`,
        });
      }
    },
  ),
};
