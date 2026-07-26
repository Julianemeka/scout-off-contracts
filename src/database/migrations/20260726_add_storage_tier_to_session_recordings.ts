/**
 * Migration: Add storage_tier to session_recordings — Issue #748
 *
 * Adds:
 *   storage_tier  ENUM('standard','infrequent_access','glacier','deep_archive')
 *   Extends status ENUM with 'restoring'
 */

import pool from "../../config/database";
import { logger } from "../../utils/logger";

export async function up(): Promise<void> {
  logger.info("[Migration] Adding storage_tier to session_recordings");

  await pool.query(`
    DO $$
    BEGIN
      -- Add storage_tier column if not present
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'session_recordings' AND column_name = 'storage_tier'
      ) THEN
        ALTER TABLE session_recordings
          ADD COLUMN storage_tier TEXT
            CHECK (storage_tier IN ('standard','infrequent_access','glacier','deep_archive'))
            DEFAULT 'standard';

        COMMENT ON COLUMN session_recordings.storage_tier IS
          'Current S3 storage class: standard (0-30d), infrequent_access (30-90d), glacier (90-365d), deep_archive (365d+)';
      END IF;

      -- Extend status check constraint to include 'restoring'
      -- Drop and recreate the constraint if it does not include 'restoring'
      IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'session_recordings_status_check'
          AND conrelid = 'session_recordings'::regclass
      ) THEN
        -- Only alter if 'restoring' is not already present in the constraint
        IF NOT EXISTS (
          SELECT 1
          FROM pg_constraint c
          JOIN pg_class t ON c.conrelid = t.oid
          WHERE t.relname = 'session_recordings'
            AND c.conname = 'session_recordings_status_check'
            AND pg_get_constraintdef(c.oid) LIKE '%restoring%'
        ) THEN
          ALTER TABLE session_recordings
            DROP CONSTRAINT session_recordings_status_check;

          ALTER TABLE session_recordings
            ADD CONSTRAINT session_recordings_status_check
              CHECK (status IN ('recording','processing','ready','deleted','failed','restoring'));
        END IF;
      ELSE
        -- No existing check constraint — add a fresh one
        ALTER TABLE session_recordings
          ADD CONSTRAINT session_recordings_status_check
            CHECK (status IN ('recording','processing','ready','deleted','failed','restoring'));
      END IF;

      -- Index for tier-based queries (cost projection, sync job)
      CREATE INDEX IF NOT EXISTS idx_session_recordings_storage_tier
        ON session_recordings (storage_tier)
        WHERE status NOT IN ('deleted','failed');
    END $$;
  `);

  logger.info("[Migration] storage_tier migration complete");
}

export async function down(): Promise<void> {
  await pool.query(`
    ALTER TABLE session_recordings DROP COLUMN IF EXISTS storage_tier;
    DROP INDEX IF EXISTS idx_session_recordings_storage_tier;
  `);
}
