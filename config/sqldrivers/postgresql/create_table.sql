CREATE TABLE public.{name}
(
    txt_keyname text COLLATE pg_catalog."default" NOT NULL,
    jsa_belong jsonb DEFAULT '[]'::jsonb,
    dbl_stamp double precision DEFAULT 0.0,
    dbl_modify double precision DEFAULT 0.0,
    CONSTRAINT {name}_pkey PRIMARY KEY (txt_keyname)
)

TABLESPACE pg_default;

ALTER TABLE public.{name}
    OWNER to postgres;