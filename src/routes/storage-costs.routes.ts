/**
 * Storage Costs Routes — Issue #748
 *
 * GET  /api/v1/admin/recordings/storage-costs
 * POST /api/v1/admin/recordings/lifecycle-policy
 * POST /api/v1/admin/recordings/sync-tiers
 * POST /api/v1/admin/recordings/:recordingId/restore
 */

import { Router } from "express";
import { authenticate } from "../middleware/auth.middleware";
import { requireAdmin } from "../middleware/admin-auth.middleware";
import { StorageCostsController } from "../controllers/storage-costs.controller";

const router = Router();

router.use(authenticate, requireAdmin);

router.get("/storage-costs", StorageCostsController.getStorageCosts);
router.post("/lifecycle-policy", StorageCostsController.applyLifecyclePolicy);
router.post("/sync-tiers", StorageCostsController.syncStorageTiers);
router.post("/:recordingId/restore", StorageCostsController.restoreRecording);

export default router;
