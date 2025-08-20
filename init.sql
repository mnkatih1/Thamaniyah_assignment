-- Schema Initialization for the Streaming Project
-- This file is automatically executed by PostgreSQL when the container starts for the first time.

-- Ensure the 'public' schema exists. This is the default schema in PostgreSQL.
CREATE SCHEMA IF NOT EXISTS public;

-- Table for Content Catalog
-- Stores metadata about various content items (videos, articles, audio).
CREATE TABLE IF NOT EXISTS public.content (
  id BIGSERIAL PRIMARY KEY, -- Unique identifier for each content item, auto-incrementing.
  title TEXT NOT NULL,       -- Title or name of the content.
  content_type TEXT NOT NULL, -- Type of content (e.g., 'video', 'article', 'audio').
  length_seconds INTEGER NOT NULL CHECK (length_seconds > 0), -- Duration of the content in seconds.
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW() -- Timestamp when the content record was created.
);

-- Index on content_type for faster lookups when filtering by content type.
CREATE INDEX IF NOT EXISTS idx_content_type ON public.content(content_type);

-- Table for Engagement Events
-- Records user interactions with content items (e.g., video views, article reads).
CREATE TABLE IF NOT EXISTS public.engagement_events (
  id BIGSERIAL PRIMARY KEY, -- Unique identifier for each engagement event, auto-incrementing.
  user_id BIGINT NOT NULL,   -- Identifier for the user who performed the engagement.
  content_id BIGINT NOT NULL REFERENCES public.content(id) ON DELETE CASCADE, -- Foreign key linking to the 'content' table.
                                                                            -- ON DELETE CASCADE means if a content item is deleted, its related engagement events are also deleted.
  duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0), -- Duration of the engagement in milliseconds.
  event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW() -- Timestamp when the engagement event occurred.
);

-- Index on content_id for faster lookups when joining with the 'content' table or filtering by content.
CREATE INDEX IF NOT EXISTS idx_events_content_id ON public.engagement_events(content_id);
-- Index on event_ts for efficient time-based queries (e.g., finding recent events).
CREATE INDEX IF NOT EXISTS idx_events_event_ts ON public.engagement_events(event_ts);

-- Minimal Initial Content Data
-- Inserts a few sample content items into the 'content' table.
-- This data is used by the data generator and for initial testing of the pipeline.
INSERT INTO public.content (title, content_type, length_seconds)
VALUES
  ("Intro to Streaming", "video", 300),
  ("How Redis Works", "video", 420),
  ("Flink Windowing 101", "video", 600),
  ("Article: CDC Basics", "article", 240)
ON CONFLICT DO NOTHING; -- Prevents insertion if these records already exist (e.g., on container restart).
