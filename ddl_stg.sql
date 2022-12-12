CREATE TABLE public.posts (
    id bigint NOT NULL PRIMARY KEY,
    user_id bigint NOT NULL,
    title character varying,
    body text,
    load_date date NOT NULL,
    rec_source character varying NOT NULL
);
