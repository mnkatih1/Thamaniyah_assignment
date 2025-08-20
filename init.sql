-- Initialisation du schéma pour le projet de streaming
-- Ce fichier est exécuté automatiquement par Postgres au premier démarrage du conteneur

CREATE SCHEMA IF NOT EXISTS public;

-- Table catalogue de contenu
CREATE TABLE IF NOT EXISTS public.content (
  id BIGSERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  content_type TEXT NOT NULL,
  length_seconds INTEGER NOT NULL CHECK (length_seconds > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_content_type ON public.content(content_type);

-- Table des événements d'engagement
CREATE TABLE IF NOT EXISTS public.engagement_events (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  content_id BIGINT NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
  duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
  event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_content_id ON public.engagement_events(content_id);
CREATE INDEX IF NOT EXISTS idx_events_event_ts ON public.engagement_events(event_ts);

-- Données de contenu minimales pour démarrer
INSERT INTO public.content (title, content_type, length_seconds)
VALUES
  ('Intro to Streaming', 'video', 300),
  ('How Redis Works', 'video', 420),
  ('Flink Windowing 101', 'video', 600),
  ('Article: CDC Basics', 'article', 240)
ON CONFLICT DO NOTHING;

