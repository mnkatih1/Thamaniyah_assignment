-- Schema Initialization for the Streaming Project
-- This file is automatically executed by PostgreSQL when the container starts for the first time.

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Ensure the 'public' schema exists. This is the default schema in PostgreSQL.
CREATE SCHEMA IF NOT EXISTS public;

-- Table for Content Catalog (CONFORME à l'assignment avec UUID)
CREATE TABLE IF NOT EXISTS public.content (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), -- UUID comme dans l'assignment
    slug TEXT UNIQUE NOT NULL,                       -- Slug requis dans l'assignment
    title TEXT NOT NULL,
    content_type TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')), -- Types exacts de l'assignment
    length_seconds INTEGER,  -- Nullable comme dans l'assignment
    publish_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()   -- publish_ts requis dans l'assignment
);

-- Table for Engagement Events (CONFORME à l'assignment)
CREATE TABLE IF NOT EXISTS public.engagement_events (
    id BIGSERIAL PRIMARY KEY,
    content_id UUID REFERENCES public.content(id) ON DELETE CASCADE, -- UUID foreign key
    user_id UUID DEFAULT uuid_generate_v4(),        -- UUID pour user_id
    event_type TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')), -- Types exacts
    event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms INTEGER,      -- Nullable comme dans l'assignment
    device TEXT,             -- Device field
    raw_payload JSONB        -- JSONB field pour extra data
);

-- Indexes pour performance
CREATE INDEX IF NOT EXISTS idx_content_type ON public.content(content_type);
CREATE INDEX IF NOT EXISTS idx_content_slug ON public.content(slug);
CREATE INDEX IF NOT EXISTS idx_events_content_id ON public.engagement_events(content_id);
CREATE INDEX IF NOT EXISTS idx_events_event_ts ON public.engagement_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON public.engagement_events(user_id);

-- Données initiales conformes
INSERT INTO public.content (slug, title, content_type, length_seconds) VALUES
    ('intro-streaming', 'Introduction au Streaming', 'video', 300),
    ('redis-guide', 'Guide Redis Complet', 'newsletter', 420),
    ('flink-windowing', 'Flink Windowing Avancé', 'podcast', 600),
    ('cdc-basics', 'Les Bases du CDC', 'video', 240),
    ('kafka-streams', 'Kafka Streams en Pratique', 'podcast', 480)
ON CONFLICT (slug) DO NOTHING;