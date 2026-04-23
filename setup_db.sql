-- Run this once on EC2 to set up PostgreSQL schema
-- psql -U postgres -f setup_db.sql

CREATE DATABASE dochistory;

\c dochistory;

CREATE TABLE IF NOT EXISTS tracked_docs (
    id                  SERIAL PRIMARY KEY,
    meeting_id          VARCHAR(64) UNIQUE NOT NULL,
    doc_id              VARCHAR(256) NOT NULL,
    doc_url             TEXT,
    salesforce_record_id VARCHAR(64),
    candidate           VARCHAR(256),
    company             VARCHAR(256),
    host_name           VARCHAR(256),
    temp_s3_prefix      TEXT,
    status              VARCHAR(32) DEFAULT 'active',   -- active | idle | finalized | error
    is_active           BOOLEAN DEFAULT TRUE,
    last_change_at      TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS doc_watch_state (
    id                  SERIAL PRIMARY KEY,
    doc_id              VARCHAR(256) UNIQUE NOT NULL,
    meeting_id          VARCHAR(64),
    watch_channel_id    VARCHAR(256),
    watch_resource_id   VARCHAR(256),
    watch_expiry        TIMESTAMPTZ,
    page_token          TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS doc_snapshots (
    id                  SERIAL PRIMARY KEY,
    meeting_id          VARCHAR(64) NOT NULL,
    doc_id              VARCHAR(256) NOT NULL,
    version_number      INTEGER NOT NULL,
    content_text        TEXT,
    edited_at           TIMESTAMPTZ DEFAULT NOW(),
    edited_by           VARCHAR(256),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for fast lookup
CREATE INDEX IF NOT EXISTS idx_tracked_docs_meeting_id ON tracked_docs(meeting_id);
CREATE INDEX IF NOT EXISTS idx_tracked_docs_doc_id ON tracked_docs(doc_id);
CREATE INDEX IF NOT EXISTS idx_tracked_docs_status ON tracked_docs(status);
CREATE INDEX IF NOT EXISTS idx_tracked_docs_last_change ON tracked_docs(last_change_at);
CREATE INDEX IF NOT EXISTS idx_doc_watch_doc_id ON doc_watch_state(doc_id);
CREATE INDEX IF NOT EXISTS idx_doc_snapshots_meeting ON doc_snapshots(meeting_id, doc_id);

-- Function to auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_tracked_docs_updated_at
    BEFORE UPDATE ON tracked_docs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_doc_watch_state_updated_at
    BEFORE UPDATE ON doc_watch_state
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

\echo 'Schema created successfully.'
