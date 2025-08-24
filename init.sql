-- Schema Initialization for the Streaming Project
-- This file is automatically executed by PostgreSQL when the container starts for the first time.

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Ensure the 'public' schema exists. This is the default schema in PostgreSQL.
CREATE SCHEMA IF NOT EXISTS public;

-- Table for Content Catalog (CONFORME à l'assignment avec UUID)
CREATE TABLE IF NOT EXISTS public.content (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), 
    slug TEXT UNIQUE NOT NULL,                      
    title TEXT NOT NULL,
    content_type TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')), 
    length_seconds INTEGER,  
    publish_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()  
);

-- Table for Engagement Events 
CREATE TABLE IF NOT EXISTS public.engagement_events (
    id BIGSERIAL PRIMARY KEY,
    content_id UUID REFERENCES public.content(id) ON DELETE CASCADE, 
    user_id UUID DEFAULT uuid_generate_v4(),      
    event_type TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')), 
    event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms INTEGER,      
    device TEXT,             
    raw_payload JSONB       
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_content_type ON public.content(content_type);
CREATE INDEX IF NOT EXISTS idx_content_slug ON public.content(slug);
CREATE INDEX IF NOT EXISTS idx_events_content_id ON public.engagement_events(content_id);
CREATE INDEX IF NOT EXISTS idx_events_event_ts ON public.engagement_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON public.engagement_events(user_id);

-- Compliant initial data
INSERT INTO public.content (slug, title, content_type, length_seconds) VALUES
    ('intro-streaming', 'Introduction au Streaming', 'video', 300),
    ('redis-guide', 'Guide Redis Complet', 'newsletter', 420),
    ('flink-windowing', 'Flink Windowing Avancé', 'podcast', 600),
    ('cdc-basics', 'Les Bases du CDC', 'video', 240),
    ('kafka-streams', 'Kafka Streams en Pratique', 'podcast', 480)
ON CONFLICT (slug) DO NOTHING;