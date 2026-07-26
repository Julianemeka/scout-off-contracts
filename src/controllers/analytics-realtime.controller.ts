/**
 * AnalyticsRealtimeController — Issue #749
 *
 * Routes:
 *   GET /api/v1/admin/analytics/realtime   — sub-5ms Redis counters
 *   GET /api/v1/admin/analytics/dashboard  — materialized view data + refresh metadata
 *   POST /api/v1/admin/analytics/refresh   — manual trigger for view refresh
 */

import { Response } from "express";
import { AuthenticatedRequest } from "../types/api.types";
import { ResponseUtil } from "../utils/response.utils";
import { asyncHandler } from "../utils/asyncHandler.utils";
import { AnalyticsPipelineService } from "../services/analytics-pipeline.service";
import { AnalyticsService } from "../services/analytics.service";
import { AdvancedAnalyticsService } from "../services/advanced-analytics.service";
import { logger } from "../utils/logger";

export const AnalyticsRealtimeController = {
  /**
   * GET /api/v1/admin/analytics/realtime
   *
   * Returns Redis streaming counters for today.
   * Target latency: < 5 ms (pure Redis reads, no DB).
   */
  getRealtimeCounters: asyncHandler(
    async (_req: AuthenticatedRequest, res: Response): Promise<void> => {
      const counters = await AnalyticsPipelineService.getRealtimeCounters();
      ResponseUtil.success(res, counters, "Real-time analytics counters");
    },
  ),

  /**
   * GET /api/v1/admin/analytics/dashboard
   *
   * Returns materialized-view aggregates enriched with last_refreshed_at
   * timestamp and current refresh lag so the frontend can show data freshness.
   */
  getDashboard: asyncHandler(
    async (req: AuthenticatedRequest, res: Response): Promise<void> => {
      const period = (req.query.period as string) || "30d";
      const userId = (req.user as any)?.id ?? (req.user as any)?.userId;

      const [dashboard, refreshMeta] = await Promise.all([
        AdvancedAnalyticsService.getDashboard(userId, period),
        AnalyticsPipelineService.getViewRefreshLag(),
      ]);

      ResponseUtil.success(
        res,
        {
          ...dashboard,
          last_refreshed_at: refreshMeta.lastRefreshedAt,
          refresh_lag_seconds: refreshMeta.lagSeconds,
        },
        "Analytics dashboard",
      );
    },
  ),

  /**
   * GET /api/v1/admin/analytics/views/health
   *
   * Returns per-view refresh health and lag for operational monitoring.
   */
  getViewsHealth: asyncHandler(
    async (_req: AuthenticatedRequest, res: Response): Promise<void> => {
      const health = await AnalyticsPipelineService.getViewsHealth();
      ResponseUtil.success(res, { views: health }, "Analytics views health");
    },
  ),

  /**
   * POST /api/v1/admin/analytics/refresh
   *
   * Manually trigger a full concurrent refresh of all analytics views.
   * Useful after bulk data migrations.
   */
  triggerRefresh: asyncHandler(
    async (_req: AuthenticatedRequest, res: Response): Promise<void> => {
      logger.info("[AnalyticsRealtimeController] Manual refresh triggered");
      const results = await AnalyticsPipelineService.refreshMaterializedViews();

      const failed = results.filter((r) => !r.success);
      if (failed.length > 0) {
        ResponseUtil.error(
          res,
          `Refresh partially failed for: ${failed.map((r) => r.view).join(", ")}`,
          500,
        );
        return;
      }

      ResponseUtil.success(res, { results }, "All analytics views refreshed");
    },
  ),

  /**
   * GET /api/v1/admin/analytics/summary
   *
   * Quick summary from materialized views (revenue, users, sessions, top mentors).
   */
  getSummary: asyncHandler(
    async (req: AuthenticatedRequest, res: Response): Promise<void> => {
      const period = (req.query.period as string) || "30d";

      const [revenue, users, sessions, topMentors, assetDist, refreshMeta] =
        await Promise.all([
          AnalyticsService.getRevenue(period),
          AnalyticsService.getUserGrowth(period),
          AnalyticsService.getSessions(period),
          AnalyticsService.getTopMentors(5),
          AnalyticsService.getAssetDistribution(),
          AnalyticsPipelineService.getViewRefreshLag(),
        ]);

      ResponseUtil.success(
        res,
        {
          period,
          revenue,
          users,
          sessions,
          topMentors,
          assetDistribution: assetDist,
          last_refreshed_at: refreshMeta.lastRefreshedAt,
          refresh_lag_seconds: refreshMeta.lagSeconds,
        },
        "Analytics summary",
      );
    },
  ),
};
