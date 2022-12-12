CREATE TABLE public.hub_post (
    post_key character(32) NOT NULL PRIMARY KEY,
    post_bk bigint NOT NULL UNIQUE,
    load_date date NOT NULL,
    rec_source character varying NOT NULL
);

CREATE TABLE public.hub_user (
    user_key character(32) NOT NULL PRIMARY KEY,
    user_bk bigint NOT NULL UNIQUE,
    load_date date NOT NULL,
    rec_source character varying NOT NULL
);

CREATE TABLE public.link_written_by (
    written_by_key character(32) NOT NULL PRIMARY KEY,
    user_key character(32) NOT NULL,
    post_key character(32) NOT NULL,
    load_date date NOT NULL,
    rec_source character varying NOT NULL,
    FOREIGN KEY (post_key) REFERENCES public.hub_post(post_key),
    FOREIGN KEY (user_key) REFERENCES public.hub_user(user_key)
);

CREATE TABLE public.sat_post_content (
    post_key character(32) NOT NULL,
    load_date date NOT NULL,
    rec_source character varying NOT NULL,
    hash_diff character(32) NOT NULL,
    post_title character varying,
    post_body text,
    PRIMARY KEY (post_key, load_date),
    FOREIGN KEY (post_key) REFERENCES public.hub_post(post_key)
);
