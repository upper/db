--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: children; Type: TABLE; Schema: public; Owner: gouser; Tablespace: 
--

CREATE TABLE children (
    id integer NOT NULL,
    parent_id integer,
    name character varying(60)
);


ALTER TABLE public.children OWNER TO gouser;

--
-- Name: children_id_seq; Type: SEQUENCE; Schema: public; Owner: gouser
--

CREATE SEQUENCE children_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.children_id_seq OWNER TO gouser;

--
-- Name: children_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gouser
--

ALTER SEQUENCE children_id_seq OWNED BY children.id;


--
-- Name: data_types; Type: TABLE; Schema: public; Owner: gouser; Tablespace: 
--

CREATE TABLE data_types (
    id integer NOT NULL,
    _uint integer,
    _uint8 integer,
    _uint16 integer,
    _uint32 integer,
    _uint64 integer,
    _int integer,
    _int8 integer,
    _int16 integer,
    _int32 integer,
    _int64 integer,
    _float32 numeric(10,6),
    _float64 numeric(10,6),
    _bool boolean,
    _string text,
    _date timestamp without time zone,
    _time time without time zone
);


ALTER TABLE public.data_types OWNER TO gouser;

--
-- Name: data_types_id_seq; Type: SEQUENCE; Schema: public; Owner: gouser
--

CREATE SEQUENCE data_types_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.data_types_id_seq OWNER TO gouser;

--
-- Name: data_types_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gouser
--

ALTER SEQUENCE data_types_id_seq OWNED BY data_types.id;


--
-- Name: people; Type: TABLE; Schema: public; Owner: gouser; Tablespace: 
--

CREATE TABLE people (
    id integer NOT NULL,
    place_code_id integer,
    name character varying(60)
);


ALTER TABLE public.people OWNER TO gouser;

--
-- Name: people_id_seq; Type: SEQUENCE; Schema: public; Owner: gouser
--

CREATE SEQUENCE people_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.people_id_seq OWNER TO gouser;

--
-- Name: people_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gouser
--

ALTER SEQUENCE people_id_seq OWNED BY people.id;


--
-- Name: places; Type: TABLE; Schema: public; Owner: gouser; Tablespace: 
--

CREATE TABLE places (
    id integer NOT NULL,
    code_id integer,
    name character varying(60)
);


ALTER TABLE public.places OWNER TO gouser;

--
-- Name: places_id_seq; Type: SEQUENCE; Schema: public; Owner: gouser
--

CREATE SEQUENCE places_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.places_id_seq OWNER TO gouser;

--
-- Name: places_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gouser
--

ALTER SEQUENCE places_id_seq OWNED BY places.id;


--
-- Name: visits; Type: TABLE; Schema: public; Owner: gouser; Tablespace: 
--

CREATE TABLE visits (
    id integer NOT NULL,
    place_id integer,
    person_id integer
);


ALTER TABLE public.visits OWNER TO gouser;

--
-- Name: visits_id_seq; Type: SEQUENCE; Schema: public; Owner: gouser
--

CREATE SEQUENCE visits_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.visits_id_seq OWNER TO gouser;

--
-- Name: visits_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: gouser
--

ALTER SEQUENCE visits_id_seq OWNED BY visits.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: gouser
--

ALTER TABLE ONLY children ALTER COLUMN id SET DEFAULT nextval('children_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: gouser
--

ALTER TABLE ONLY data_types ALTER COLUMN id SET DEFAULT nextval('data_types_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: gouser
--

ALTER TABLE ONLY people ALTER COLUMN id SET DEFAULT nextval('people_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: gouser
--

ALTER TABLE ONLY places ALTER COLUMN id SET DEFAULT nextval('places_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: gouser
--

ALTER TABLE ONLY visits ALTER COLUMN id SET DEFAULT nextval('visits_id_seq'::regclass);


--
-- Name: children_pkey; Type: CONSTRAINT; Schema: public; Owner: gouser; Tablespace: 
--

ALTER TABLE ONLY children
    ADD CONSTRAINT children_pkey PRIMARY KEY (id);


--
-- Name: data_types_pkey; Type: CONSTRAINT; Schema: public; Owner: gouser; Tablespace: 
--

ALTER TABLE ONLY data_types
    ADD CONSTRAINT data_types_pkey PRIMARY KEY (id);


--
-- Name: people_pkey; Type: CONSTRAINT; Schema: public; Owner: gouser; Tablespace: 
--

ALTER TABLE ONLY people
    ADD CONSTRAINT people_pkey PRIMARY KEY (id);


--
-- Name: places_pkey; Type: CONSTRAINT; Schema: public; Owner: gouser; Tablespace: 
--

ALTER TABLE ONLY places
    ADD CONSTRAINT places_pkey PRIMARY KEY (id);


--
-- Name: visits_pkey; Type: CONSTRAINT; Schema: public; Owner: gouser; Tablespace: 
--

ALTER TABLE ONLY visits
    ADD CONSTRAINT visits_pkey PRIMARY KEY (id);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

