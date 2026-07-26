/**
 * Analytics Realtime Routes — Issue #749
 *
 * All routes are admin-only.
 *
 * GET  /api/v1/admin/analytics/realtime        Redis streaming counters (<5ms)
 * GET  /api/v1/admin/analytics/dashboard       Materialized view dashboard + last_refreshed_at
 * GET  /api/v1/admin/analytics/views/health    Per-view refresh health
 * GET  /api/v1/admin/analytics/summary         Quick aggregated summary
 * POST /api/v1/admin/analytics/refresh         Manual view refresh trigger
 */

import { Router } from "express";
import { authenticate } from "../middleware/auth.middleware";
import { requireAdmin } from "../middleware/admin-auth.middleware";
import { AnalyticsRealtimeController } from "../controllers/analytics-realtime.controller";

const router = Router();

router.use(authenticate, requireAdmin);

router.get("/realtime", AnalyticsRealtimeController.getRealtimeCounters);
router.get("/dashboard", AnalyticsRealtimeController.getDashboard);
router.get("/views/health", AnalyticsRealtimeController.getViewsHealth);
router.get("/summary", AnalyticsRealtimeController.getSummary);
router.post("/refresh", AnalyticsRealtimeController.triggerRefresh);

export default router;
