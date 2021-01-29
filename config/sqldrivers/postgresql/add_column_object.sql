ALTER TABLE public.{table}
    ADD COLUMN {name} jsonb DEFAULT '{value}'::jsonb;