ALTER TABLE public.{table}
    ADD COLUMN {name} text COLLATE pg_catalog."default" DEFAULT '{value}'::text;