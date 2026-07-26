import { z } from "zod";
import { uuidSchema } from "./common.schemas";

export const bulkJobIdParamSchema = z.object({
  params: z.object({ jobId: uuidSchema }),
});

export const bulkPaymentsSchema = z.object({
  body: z.object({
    payments: z
      .array(
        z.object({
          userId: z.string().uuid(),
          bookingId: z.string().uuid(),
          amount: z.string().regex(/^\d+(\.\d+)?$/),
          currency: z.string().min(1).max(12).optional(),
          description: z.string().max(500).optional(),
        }),
      )
      .min(1)
      .max(500),
  }),
});

/**
 * Schema for a single mentor row parsed from the bulk-import CSV.
 * Used both inside parseMentorCsv and exposed for unit tests.
 */
export const mentorImportRowSchema = z.object({
  email: z.string().email("Invalid email address"),
  firstName: z.string().min(2, "firstName must be at least 2 characters").max(50),
  lastName: z.string().min(2, "lastName must be at least 2 characters").max(50),
  hourlyRate: z
    .number()
    .min(1, "hourlyRate must be at least $1")
    .max(500, "hourlyRate must not exceed $500"),
  timezone: z.string().refine(
    (tz) => {
      try {
        // Use Intl.DateTimeFormat to validate IANA timezone strings without importing luxon here
        Intl.DateTimeFormat(undefined, { timeZone: tz });
        return true;
      } catch {
        return false;
      }
    },
    { message: "timezone must be a valid IANA timezone identifier" },
  ),
  expertise: z
    .array(z.string().min(1).max(100))
    .min(1, "expertise must have at least one item"),
  bio: z.string().max(2000).optional(),
  stellarPublicKey: z
    .string()
    .regex(
      /^G[A-Z2-7]{55}$/,
      "stellarPublicKey must start with G and be 56 characters",
    )
    .optional()
    .or(z.literal("")),
});

export type MentorImportRow = z.infer<typeof mentorImportRowSchema>;

/**
 * Route-level param schema for the report download endpoint.
 */
export const bulkMentorImportJobParamSchema = z.object({
  params: z.object({ jobId: uuidSchema }),
});
