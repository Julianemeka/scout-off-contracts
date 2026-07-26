/**
 * BulkMentorImportController — Issue #747
 *
 * POST /api/v1/admin/bulk/mentors/import   — upload CSV, enqueue import job
 * GET  /api/v1/admin/bulk/jobs/:jobId/report — download S3 report URL
 */

import { Response } from "express";
import { AuthenticatedRequest } from "../types/api.types";
import { ResponseUtil } from "../utils/response.utils";
import { BulkService } from "../services/bulk.service";
import { asyncHandler } from "../utils/asyncHandler.utils";

function getUserId(req: AuthenticatedRequest): string {
  return (req.user as any)?.id ?? (req.user as any)?.userId;
}

export const BulkMentorImportController = {
  /**
   * POST /api/v1/admin/bulk/mentors/import
   *
   * Accepts multipart/form-data with a single CSV file field named "file".
   * CSV columns: email, firstName, lastName, hourlyRate, timezone, expertise, bio, stellarPublicKey
   *
   * Returns 202 with a jobId. Poll GET /admin/bulk/jobs/:jobId for status.
   */
  importMentors: asyncHandler(
    async (req: AuthenticatedRequest, res: Response): Promise<void> => {
      const file = (req as any).file as Express.Multer.File | undefined;
      if (!file?.buffer) {
        ResponseUtil.error(res, "CSV file is required (field name: file)", 400);
        return;
      }

      const csvContent = file.buffer.toString("utf-8");
      const jobId = await BulkService.requestMentorImport(getUserId(req), csvContent);

      ResponseUtil.success(
        res,
        {
          jobId,
          message:
            "Mentor import queued. Poll GET /api/v1/admin/bulk/jobs/:jobId for status.",
        },
        "Bulk mentor import queued",
        202,
      );
    },
  ),

  /**
   * GET /api/v1/admin/bulk/jobs/:jobId/report
   *
   * Returns a short-lived S3 presigned URL to download the import report CSV.
   * Only available after the job has completed or failed.
   */
  getImportReport: asyncHandler(
    async (req: AuthenticatedRequest, res: Response): Promise<void> => {
      const { jobId } = req.params as Record<string, string>;
      const url = await BulkService.getImportReportUrl(jobId, getUserId(req));
      ResponseUtil.success(
        res,
        { downloadUrl: url, expiresInSeconds: 3600 },
        "Import report URL generated",
      );
    },
  ),
};
