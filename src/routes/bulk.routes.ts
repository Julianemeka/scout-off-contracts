/**
 * Bulk Routes — includes Issue #747 mentor bulk import endpoints
 *
 * POST /api/v1/admin/bulk/users/import        — bulk user import (existing)
 * POST /api/v1/admin/bulk/payments/process    — bulk payments (existing)
 * GET  /api/v1/admin/bulk/jobs/:jobId         — job status (existing)
 * POST /api/v1/admin/bulk/mentors/import      — bulk mentor import (NEW #747)
 * GET  /api/v1/admin/bulk/jobs/:jobId/report  — download import report (NEW #747)
 */

import { Router } from "express";
import multer from "multer";
import { BulkController } from "../controllers/bulk.controller";
import { BulkMentorImportController } from "../controllers/bulk-mentor-import.controller";
import { authenticate } from "../middleware/auth.middleware";
import { requireAdmin } from "../middleware/admin-auth.middleware";
import { validate } from "../middleware/validation.middleware";
import { asyncHandler } from "../utils/asyncHandler.utils";
import {
  bulkPaymentsSchema,
  bulkJobIdParamSchema,
  bulkMentorImportJobParamSchema,
} from "../validators/schemas/bulk.schemas";

const router = Router();

// 10 MB limit — large CSV files for 500-row imports can be a few hundred KB,
// but we allow up to 10 MB for safety.
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (file.mimetype === "text/csv" || file.originalname.endsWith(".csv")) {
      cb(null, true);
    } else {
      cb(new Error("Only CSV files are accepted"));
    }
  },
});

router.use(authenticate, requireAdmin);

// ── Existing endpoints ────────────────────────────────────────────────────────

router.post(
  "/users/import",
  upload.single("file"),
  asyncHandler(BulkController.importUsers),
);

router.post(
  "/payments/process",
  validate(bulkPaymentsSchema),
  asyncHandler(BulkController.processPayments),
);

router.get(
  "/jobs/:jobId",
  validate(bulkJobIdParamSchema),
  asyncHandler(BulkController.getJobStatus),
);

// ── Issue #747: Bulk mentor import ────────────────────────────────────────────

/**
 * POST /api/v1/admin/bulk/mentors/import
 *
 * Accepts CSV with columns:
 *   email, firstName, lastName, hourlyRate, timezone, expertise, bio, stellarPublicKey
 *
 * Constraints:
 *   - Max 500 rows per import
 *   - Max 1 import per admin per hour (rate limited)
 *   - Duplicate emails are flagged in the report, not rejected upfront
 */
router.post(
  "/mentors/import",
  upload.single("file"),
  asyncHandler(BulkMentorImportController.importMentors),
);

/**
 * GET /api/v1/admin/bulk/jobs/:jobId/report
 *
 * Returns a presigned S3 URL to download the completed import report CSV.
 * Report columns: email, status (success/duplicate/failed), userId, errorMessage
 */
router.get(
  "/jobs/:jobId/report",
  validate(bulkMentorImportJobParamSchema),
  asyncHandler(BulkMentorImportController.getImportReport),
);

export default router;
