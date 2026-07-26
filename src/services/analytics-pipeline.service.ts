/**
 * AnalyticsPipelineService — Issue #749
 *
 * Provides:
 *  - Concurrent (non-blocking) materialized view refresh for all 5 analytics views
 *  - Event-driven incremental refresh (targeted per event type)
 *  - Real-time Redis streaming counters (revenue, sessions, new users today)
 *  - Prometheus gauge for mv_refresh_lag_seconds
 *  - Helpers consumed by the realtime controller
 */

import pool from "../config/database";
import { redis } from "../config/redis";
import { logger } from "../utils/logger";
import { mvRefreshLagSeconds } from "../config/metrics";

// ─── Constants ────────────────────────────────────────────────────────────────

/** All materialized views managed by this service */
export const ANALYTICS_VIEWS = [
  "mv_daily_revenue",
  "mv_daily_users",
  "mv_session_stats",
  "mv_top_mentors",
  "mv_asset_distribution",
] as const;

export type AnalyticsView = (typeof ANALYTICS_VIEWS)[number];

/** Views that are refreshed incrementally on booking completion */
const BOOKING_VIEWS: AnalyticsView[] = ["mv_daily_revenue", "mv_session_stats"];

/** Views that are refreshed incrementally on user registration */
const USER_VIEWS: AnalyticsView[] = ["mv_daily_users"];

/** Redis key prefix for real-time streaming counters */
const RT_PREFIX = "analytics:rt";

/** Redis key storing when each view was last refreshed (ISO string) */
const VIEW_REFRESHED_AT_KEY = "analytics:views:last_refreshed_at";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface ViewRefreshResult {
  view: string;
  success: boolean;
  durationMs: number;
  error?: string;
}

export interface RealtimeCounters {
  revenueToday: number;
  sessionsToday: number;
  newUsersToday: number;
  lastUpdated: string;
}

export interface ViewHealthStatus {
  view: string;
  lastRefreshedAt: string | null;
  lagSeconds: number | null;
}

// ─── Service ──────────────────────────────────────────────────────────────────

export const AnalyticsPipelineService = {
  /**
   * Refresh all 5 materialized views concurrently (non-blocking).
   * Uses REFRESH MATERIALIZED VIEW CONCURRENTLY which does not lock readers.
   *
   * Returns per-view results so callers can log partial failures.
   */
  async refreshMaterializedViews(): Promise<ViewRefreshResult[]> {
    const startAll = Date.now();
    logger.info("[AnalyticsPipeline] Starting full concurrent view refresh");

    const results = await Promise.all(
      ANALYTICS_VIEWS.map((view) => this._refreshView(view)),
    );

    const succeeded = results.filter((r) => r.success).length;
    const failed = results.filter((r) => !r.success).length;
    const totalMs = Date.now() - startAll;

    logger.info("[AnalyticsPipeline] Full refresh complete", {
      succeeded,
      failed,
      totalMs,
    });

    // Update last-refreshed timestamp
    const now = new Date().toISOString();
    await redis.set(VIEW_REFRESHED_AT_KEY, now);

    // Update Prometheus lag gauge (lag = 0 right after refresh)
    mvRefreshLagSeconds.set(0);

    return results;
  },

  /**
   * Refresh only the views relevant to a booking completion event.
   * Called by BookingsService.completeBooking() for sub-second staleness.
   */
  async refreshOnBookingCompleted(payload: {
    bookingId: string;
    amount?: number;
  }): Promise<void> {
    logger.debug("[AnalyticsPipeline] Incremental refresh on booking.completed", {
      bookingId: payload.bookingId,
    });

    await Promise.all(BOOKING_VIEWS.map((view) => this._refreshView(view)));

    // Increment real-time counters
    if (payload.amount != null && payload.amount > 0) {
      await this.incrementRealtimeCounter("revenue", payload.amount);
    }
    await this.incrementRealtimeCounter("sessions", 1);
  },

  /**
   * Refresh only the views relevant to a new user registration.
   */
  async refreshOnUserRegistered(): Promise<void> {
    logger.debug("[AnalyticsPipeline] Incremental refresh on user.registered");
    await Promise.all(USER_VIEWS.map((view) => this._refreshView(view)));
    await this.incrementRealtimeCounter("newUsers", 1);
  },

  /**
   * Increment a real-time counter stored in Redis.
   * Keys auto-expire at midnight UTC so they always represent "today".
   *
   * @param metric  'revenue' | 'sessions' | 'newUsers'
   * @param by      Amount to add (default 1)
   */
  async incrementRealtimeCounter(
    metric: "revenue" | "sessions" | "newUsers",
    by = 1,
  ): Promise<void> {
    const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    const key = `${RT_PREFIX}:${metric}:${today}`;

    // INCRBYFLOAT supports decimal amounts (revenue can be fractional)
    await redis.incrbyfloat(key, by);

    // Expire key at end of day (86400s max, calculate seconds until midnight)
    const now = new Date();
    const midnight = new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1),
    );
    const ttl = Math.floor((midnight.getTime() - now.getTime()) / 1000);
    await redis.expire(key, ttl);
  },

  /**
   * Get current real-time counters for today.
   * This is the <5 ms endpoint — pure Redis reads.
   */
  async getRealtimeCounters(): Promise<RealtimeCounters> {
    const today = new Date().toISOString().slice(0, 10);

    const [revenue, sessions, newUsers] = await Promise.all([
      redis.get(`${RT_PREFIX}:revenue:${today}`),
      redis.get(`${RT_PREFIX}:sessions:${today}`),
      redis.get(`${RT_PREFIX}:newUsers:${today}`),
    ]);

    return {
      revenueToday: parseFloat(revenue ?? "0"),
      sessionsToday: parseInt(sessions ?? "0", 10),
      newUsersToday: parseInt(newUsers ?? "0", 10),
      lastUpdated: new Date().toISOString(),
    };
  },

  /**
   * Returns the ISO timestamp of the last successful full refresh,
   * and computes lag in seconds for the Prometheus gauge.
   */
  async getViewRefreshLag(): Promise<{
    lastRefreshedAt: string | null;
    lagSeconds: number | null;
  }> {
    const lastRefreshedAt = await redis.get(VIEW_REFRESHED_AT_KEY);
    if (!lastRefreshedAt) {
      return { lastRefreshedAt: null, lagSeconds: null };
    }

    const lagMs = Date.now() - new Date(lastRefreshedAt).getTime();
    const lagSeconds = Math.floor(lagMs / 1000);

    // Keep Prometheus gauge in sync
    mvRefreshLagSeconds.set(lagSeconds);

    return { lastRefreshedAt, lagSeconds };
  },

  /**
   * Health status for each view: last refresh time and lag.
   */
  async getViewsHealth(): Promise<ViewHealthStatus[]> {
    const { lastRefreshedAt, lagSeconds } = await this.getViewRefreshLag();

    return ANALYTICS_VIEWS.map((view) => ({
      view,
      lastRefreshedAt,
      lagSeconds,
    }));
  },

  // ─── Private helpers ────────────────────────────────────────────────────────

  /** Refresh a single view and return a result record. */
  async _refreshView(view: AnalyticsView): Promise<ViewRefreshResult> {
    const start = Date.now();
    try {
      // CONCURRENTLY means no exclusive lock — existing readers are unaffected.
      // Requires a UNIQUE index on the view (set up in migration 015).
      await pool.query(
        `REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`,
      );
      const durationMs = Date.now() - start;
      logger.debug(`[AnalyticsPipeline] Refreshed ${view}`, { durationMs });
      return { view, success: true, durationMs };
    } catch (error) {
      const durationMs = Date.now() - start;
      const message = error instanceof Error ? error.message : String(error);
      logger.error(`[AnalyticsPipeline] Failed to refresh ${view}`, {
        error: message,
      });
      return { view, success: false, durationMs, error: message };
    }
  },
};
