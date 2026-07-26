import crypto from "crypto";
import bcrypt from "bcryptjs";
import pool from "../config/database";
import { BulkJobModel, BulkJobRecord } from "../models/bulk-job.model";
import { bulkQueue } from "../queues/bulk.queue";
import { PaymentsService } from "./payments.service";
import { createError } from "../middleware/errorHandler";
import { emailSchema } from "../validators/schemas/common.schemas";
import { z } from "zod";
import { QUEUE_PRIORITIES } from "../config/queue";
import { redis } from "../config/redis";
import { SocketService } from "./socket.service";
import { StorageService } from "./storage.service";
import { mentorImportRowSchema, MentorImportRow } from "../validators/schemas/bulk.schemas";
import { logger } from "../utils/logger";

export interface BulkRowResult {
  index: number;
  success: boolean;
  identifier?: string;
  error?: string;
}

export interface BulkValidationResult<T> {
  valid: boolean;
  records: T[];
  errors: BulkRowResult[];
}

const importUserRowSchema = z.object({
  email: emailSchema,
  firstName: z.string().min(2).max(50),
  lastName: z.string().min(2).max(50),
  role: z.enum(["mentee", "mentor"]).default("mentee"),
});

const paymentRowSchema = z.object({
  userId: z.string().uuid(),
  bookingId: z.string().uuid(),
  amount: z.string().regex(/^\d+(\.\d+)?$/),
  currency: z.string().min(1).max(12).optional(),
  description: z.string().max(500).optional(),
});

export type BulkImportUserRow = z.infer<typeof importUserRowSchema>;
export type BulkPaymentRow = z.infer<typeof paymentRowSchema>;

export function parseUsersCsv(csvContent: string): {
  header: string[];
  rows: string[][];
} {
  const lines = csvContent
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length < 2) {
    throw createError(
      "CSV must include a header row and at least one data row",
      400,
    );
  }

  const header = lines[0].split(",").map((h) => h.trim().toLowerCase());
  const required = ["email", "firstname", "lastname"];
  for (const col of required) {
    if (!header.includes(col)) {
      throw createError(`CSV missing required column: ${col}`, 400);
    }
  }

  const rows = lines
    .slice(1)
    .map((line) => line.split(",").map((cell) => cell.trim()));
  return { header, rows };
}

export function mapCsvRowsToUsers(
  header: string[],
  rows: string[][],
): BulkImportUserRow[] {
  const indexOf = (name: string) => header.indexOf(name);
  const emailIdx = indexOf("email");
  const firstIdx = indexOf("firstname");
  const lastIdx = indexOf("lastname");
  const roleIdx = indexOf("role");

  return rows.map((cells) => ({
    email: cells[emailIdx] ?? "",
    firstName: cells[firstIdx] ?? "",
    lastName: cells[lastIdx] ?? "",
    role: (cells[roleIdx]?.toLowerCase() === "mentor" ? "mentor" : "mentee") as
      | "mentee"
      | "mentor",
  }));
}

export function validateUserImportRows(
  records: BulkImportUserRow[],
): BulkValidationResult<BulkImportUserRow> {
  const errors: BulkRowResult[] = [];
  const validRecords: BulkImportUserRow[] = [];

  records.forEach((record, index) => {
    const parsed = importUserRowSchema.safeParse(record);
    if (!parsed.success) {
      errors.push({
        index,
        success: false,
        identifier: record.email,
        error: parsed.error.issues.map((i) => i.message).join("; "),
      });
      return;
    }
    validRecords.push(parsed.data);
  });

  return { valid: errors.length === 0, records: validRecords, errors };
}

export function validatePaymentRows(
  records: BulkPaymentRow[],
): BulkValidationResult<BulkPaymentRow> {
  const errors: BulkRowResult[] = [];
  const validRecords: BulkPaymentRow[] = [];

  records.forEach((record, index) => {
    const parsed = paymentRowSchema.safeParse(record);
    if (!parsed.success) {
      errors.push({
        index,
        success: false,
        identifier: record.bookingId,
        error: parsed.error.issues.map((i) => i.message).join("; "),
      });
      return;
    }
    validRecords.push(parsed.data);
  });

  return { valid: errors.length === 0, records: validRecords, errors };
}

export const BulkService = {
  async requestUserImport(
    requestedBy: string,
    csvContent: string,
  ): Promise<string> {
    const { header, rows } = parseUsersCsv(csvContent);
    const mapped = mapCsvRowsToUsers(header, rows);
    const validation = validateUserImportRows(mapped);

    if (!validation.valid) {
      const err = createError("CSV validation failed", 400);
      (err as any).details = { errors: validation.errors };
      throw err;
    }

    const job = await BulkJobModel.create(
      "users_import",
      requestedBy,
      mapped.length,
    );
    await bulkQueue.add("process-bulk", {
      jobId: job.id,
      jobType: "users_import",
      requestedBy,
      payload: { users: mapped },
    }, { priority: QUEUE_PRIORITIES.BULK });

    return job.id;
  },

  async requestPaymentsProcess(
    requestedBy: string,
    payments: BulkPaymentRow[],
  ): Promise<string> {
    const validation = validatePaymentRows(payments);
    if (!validation.valid) {
      const err = createError("Payment batch validation failed", 400);
      (err as any).details = { errors: validation.errors };
      throw err;
    }

    const job = await BulkJobModel.create(
      "payments_process",
      requestedBy,
      validation.records.length,
    );
    await bulkQueue.add("process-bulk", {
      jobId: job.id,
      jobType: "payments_process",
      requestedBy,
      payload: { payments: validation.records },
    }, { priority: QUEUE_PRIORITIES.BULK });

    return job.id;
  },

  async getJob(
    jobId: string,
    requestedBy: string,
  ): Promise<BulkJobRecord | null> {
    const job = await BulkJobModel.findById(jobId);
    if (!job || job.requested_by !== requestedBy) return null;
    return job;
  },

  // ── Mentor Import ────────────────────────────────────────────────────────────

  /** Max rows allowed per import */
  MENTOR_IMPORT_MAX_ROWS: 500,
  /** Rate limit: 1 import per admin per hour */
  MENTOR_IMPORT_COOLDOWN_SECONDS: 3600,

  /**
   * Parse and validate a mentor import CSV.
   * Expected header: email, firstName, lastName, hourlyRate, timezone, expertise, bio, stellarPublicKey
   */
  parseMentorCsv(csvContent: string): MentorImportRow[] {
    const lines = csvContent
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter(Boolean);

    if (lines.length < 2) {
      throw createError("CSV must include a header row and at least one data row", 400);
    }

    if (lines.length - 1 > (BulkService as any).MENTOR_IMPORT_MAX_ROWS) {
      throw createError(
        `CSV exceeds the maximum of ${(BulkService as any).MENTOR_IMPORT_MAX_ROWS} rows`,
        400,
      );
    }

    const header = lines[0].split(",").map((h) => h.trim().toLowerCase());
    const required = ["email", "firstname", "lastname", "hourlyrate", "timezone", "expertise"];
    for (const col of required) {
      if (!header.includes(col)) {
        throw createError(`CSV missing required column: ${col}`, 400);
      }
    }

    const idx = (name: string) => header.indexOf(name);
    const rows = lines.slice(1);

    const records: MentorImportRow[] = [];
    const errors: string[] = [];

    rows.forEach((line, i) => {
      const cells = line.split(",").map((c) => c.trim());
      const raw = {
        email: cells[idx("email")] ?? "",
        firstName: cells[idx("firstname")] ?? "",
        lastName: cells[idx("lastname")] ?? "",
        hourlyRate: parseFloat(cells[idx("hourlyrate")] ?? "0"),
        timezone: cells[idx("timezone")] ?? "",
        expertise: (cells[idx("expertise")] ?? "").split(";").map((e) => e.trim()).filter(Boolean),
        bio: cells[idx("bio")] ?? undefined,
        stellarPublicKey: cells[idx("stellarpublickey")] || undefined,
      };

      const parsed = mentorImportRowSchema.safeParse(raw);
      if (!parsed.success) {
        errors.push(`Row ${i + 2}: ${parsed.error.issues.map((e) => e.message).join("; ")}`);
      } else {
        records.push(parsed.data);
      }
    });

    if (errors.length > 0) {
      const err = createError("CSV validation failed", 400);
      (err as any).details = { errors };
      throw err;
    }

    return records;
  },

  /**
   * Enqueue a mentor bulk import job.
   * Enforces rate limit: max 1 active import per admin per hour.
   */
  async requestMentorImport(
    requestedBy: string,
    csvContent: string,
  ): Promise<string> {
    // Rate limit check
    const rateLimitKey = `bulk:mentor_import:ratelimit:${requestedBy}`;
    const lastImport = await redis.get(rateLimitKey);
    if (lastImport) {
      throw createError(
        "Rate limit: only one mentor import per hour is allowed",
        429,
      );
    }

    const mentors = (this as any).parseMentorCsv(csvContent);

    const job = await BulkJobModel.create("mentor_import", requestedBy, mentors.length);

    await bulkQueue.add(
      "process-bulk",
      { jobId: job.id, jobType: "mentor_import", requestedBy, payload: { mentors } },
      { priority: QUEUE_PRIORITIES.BULK },
    );

    // Set rate limit key (expires after cooldown)
    await redis.set(
      rateLimitKey,
      new Date().toISOString(),
      "EX",
      (BulkService as any).MENTOR_IMPORT_COOLDOWN_SECONDS,
    );

    return job.id;
  },

  /**
   * Generate a CSV import report and upload it to S3.
   * Returns the S3 download key.
   */
  async generateAndUploadImportReport(
    jobId: string,
    results: BulkRowResult[],
  ): Promise<string> {
    const csvRows = [
      ["email", "status", "userId", "errorMessage"].join(","),
    ];

    for (const r of results) {
      const status = r.success ? "success" : (r.error?.startsWith("duplicate") ? "duplicate" : "failed");
      csvRows.push(
        [
          r.identifier ?? "",
          status,
          r.success ? (r as any).userId ?? "" : "",
          r.error ?? "",
        ]
          .map((v) => `"${String(v).replace(/"/g, '""')}"`)
          .join(","),
      );
    }

    const csvBuffer = Buffer.from(csvRows.join("\n"), "utf-8");
    const s3Key = `bulk-reports/mentor-import/${jobId}/report_${Date.now()}.csv`;

    await StorageService.uploadFile(s3Key, csvBuffer, "text/csv", {
      jobId,
      type: "mentor_import_report",
    });

    return s3Key;
  },

  /**
   * Get the S3 presigned download URL for a completed import report.
   */
  async getImportReportUrl(jobId: string, requestedBy: string): Promise<string> {
    const job = await BulkJobModel.findById(jobId);
    if (!job || job.requested_by !== requestedBy) {
      throw createError("Job not found", 404);
    }
    if (job.status !== "completed" && job.status !== "failed") {
      throw createError("Report not ready yet", 409);
    }

    const report = job.result_report as any;
    const s3Key = report?.reportS3Key as string | undefined;
    if (!s3Key) {
      throw createError("Report file not available", 404);
    }

    return StorageService.generatePresignedUrl(s3Key, 3600);
  },

  async processJob(
    jobId: string,
    jobType: "users_import" | "payments_process" | "mentor_import",
    payload: { users?: BulkImportUserRow[]; payments?: BulkPaymentRow[]; mentors?: MentorImportRow[] },
    requestedBy?: string
  ): Promise<void> {
    await BulkJobModel.updateStatus(jobId, "processing");
    if (!requestedBy) {
      const job = await BulkJobModel.findById(jobId);
      requestedBy = job?.requested_by;
    }
    const results: BulkRowResult[] = [];
    let successCount = 0;
    let failureCount = 0;

    const totalItems = (jobType === "users_import" ? payload.users?.length : jobType === "mentor_import" ? payload.mentors?.length : payload.payments?.length) || 0;

    const trackProgress = async (currentIdx: number) => {
      if (totalItems === 0) return;
      const processed = currentIdx + 1;
      const progress = Math.floor((processed / totalItems) * 100);
      const prevProgress = Math.floor((currentIdx / totalItems) * 100);
      
      if (progress > prevProgress || processed === 1) {
        const state = { total: totalItems, processed, failed: failureCount, status: "processing" };
        await redis.set(`bulk:${jobId}:progress`, JSON.stringify(state), "EX", 3600);
      }
      if (progress % 5 === 0 && (progress > prevProgress || processed === 1)) {
        const eventData = { jobId, progress, processed, total: totalItems, failed: failureCount, status: "processing" };
        if (requestedBy) {
          SocketService.emitToUser(requestedBy, "bulk:progress", eventData);
        } else {
          SocketService.emitToAll("bulk:progress", eventData);
        }
      }
    };

    try {
      if (jobType === "users_import" && payload.users) {
        for (const [index, user] of payload.users.entries()) {
          try {
            const existing = await pool.query(
              "SELECT id FROM users WHERE email = $1",
              [user.email],
            );
            if (existing.rows.length > 0) {
              throw new Error("Email is already registered");
            }

            const tempPassword = crypto.randomBytes(16).toString("hex");
            const passwordHash = await bcrypt.hash(tempPassword, 10);
            await pool.query(
              `INSERT INTO users (email, password_hash, first_name, last_name, role)
               VALUES ($1, $2, $3, $4, $5)`,
              [
                user.email,
                passwordHash,
                user.firstName,
                user.lastName,
                user.role,
              ],
            );
            successCount++;
            results.push({ index, success: true, identifier: user.email });
          } catch (error) {
            failureCount++;
            results.push({
              index,
              success: false,
              identifier: user.email,
              error: error instanceof Error ? error.message : String(error),
            });
          }
          await trackProgress(index);
        }
      }

      if (jobType === "mentor_import" && payload.mentors) {
        for (const [index, mentor] of payload.mentors.entries()) {
          try {
            // Duplicate detection
            const existing = await pool.query(
              "SELECT id FROM users WHERE email = $1",
              [mentor.email],
            );
            if (existing.rows.length > 0) {
              failureCount++;
              results.push({
                index,
                success: false,
                identifier: mentor.email,
                error: "duplicate: email already registered",
              });
              await trackProgress(index);
              continue;
            }

            // Create user account with mentor role
            const tempPassword = crypto.randomBytes(16).toString("hex");
            const passwordHash = await bcrypt.hash(tempPassword, 10);
            const { rows: newUser } = await pool.query<{ id: string }>(
              `INSERT INTO users (email, password_hash, first_name, last_name, role)
               VALUES ($1, $2, $3, $4, 'mentor') RETURNING id`,
              [mentor.email, passwordHash, mentor.firstName, mentor.lastName],
            );
            const userId = newUser[0].id;

            // Create mentor profile
            await pool.query(
              `INSERT INTO mentors (user_id, hourly_rate, timezone, bio)
               VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO NOTHING`,
              [userId, mentor.hourlyRate, mentor.timezone, mentor.bio ?? null],
            );

            // Store expertise tags
            for (const tag of mentor.expertise ?? []) {
              await pool.query(
                `INSERT INTO mentor_expertise (mentor_user_id, expertise)
                 VALUES ($1, $2) ON CONFLICT DO NOTHING`,
                [userId, tag],
              );
            }

            // Store Stellar public key if provided
            if (mentor.stellarPublicKey) {
              await pool.query(
                `INSERT INTO wallets (user_id, public_key, type)
                 VALUES ($1, $2, 'stellar')
                 ON CONFLICT (user_id, type) DO UPDATE SET public_key = EXCLUDED.public_key`,
                [userId, mentor.stellarPublicKey],
              );
            }

            // Fire-and-forget invitation email
            import("./email.service").then(({ EmailService }) => {
              EmailService.sendEmail({
                to: mentor.email,
                subject: "You've been invited to MentorsMind as a mentor",
                template: "bulk-mentor-invitation",
                context: {
                  firstName: mentor.firstName,
                  lastName: mentor.lastName,
                  tempPassword,
                  loginUrl: `${process.env.FRONTEND_URL ?? "https://app.mentorsmind.com"}/login`,
                },
              }).catch(() => {});
            });

            successCount++;
            results.push({ index, success: true, identifier: mentor.email });
          } catch (error) {
            failureCount++;
            results.push({
              index,
              success: false,
              identifier: mentor.email,
              error: error instanceof Error ? error.message : String(error),
            });
          }
          await trackProgress(index);
        }
      }

      if (jobType === "payments_process" && payload.payments) {
        for (const [index, payment] of payload.payments.entries()) {
          try {
            await PaymentsService.initiatePayment({
              userId: payment.userId,
              bookingId: payment.bookingId,
              amount: payment.amount,
              currency: payment.currency,
              description: payment.description,
            });
            successCount++;
            results.push({
              index,
              success: true,
              identifier: payment.bookingId,
            });
          } catch (error) {
            failureCount++;
            results.push({
              index,
              success: false,
              identifier: payment.bookingId,
              error: error instanceof Error ? error.message : String(error),
            });
          }
          await trackProgress(index);
        }
      }

      const state = { total: totalItems, processed: successCount + failureCount, failed: failureCount, status: "completed" };
      await redis.set(`bulk:${jobId}:progress`, JSON.stringify(state), "EX", 3600);
      const eventData = { jobId, progress: 100, status: "completed" };
      if (requestedBy) {
        SocketService.emitToUser(requestedBy, "bulk:progress", eventData);
      } else {
        SocketService.emitToAll("bulk:progress", eventData);
      }

      // For mentor imports, generate and upload the report CSV
      let reportS3Key: string | undefined;
      if (jobType === "mentor_import") {
        try {
          reportS3Key = await BulkService.generateAndUploadImportReport(jobId, results);
        } catch (reportErr) {
          logger.warn("Failed to upload import report", { jobId, error: reportErr });
        }
      }

      await BulkJobModel.updateStatus(jobId, "completed", {
        successCount,
        failureCount,
        resultReport: { results, ...(reportS3Key ? { reportS3Key } : {}) },
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      const state = { total: totalItems, processed: successCount + failureCount, failed: failureCount, status: "failed", error: errorMessage };
      await redis.set(`bulk:${jobId}:progress`, JSON.stringify(state), "EX", 3600);
      const eventData = { jobId, status: "failed", error: errorMessage };
      if (requestedBy) {
        SocketService.emitToUser(requestedBy, "bulk:progress", eventData);
      } else {
        SocketService.emitToAll("bulk:progress", eventData);
      }

      await BulkJobModel.updateStatus(jobId, "failed", {
        errorMessage,
        resultReport: { results },
        successCount,
        failureCount,
      });
      throw error;
    }
  },
};
