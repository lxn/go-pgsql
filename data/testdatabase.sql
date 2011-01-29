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
-- Name: table1; Type: TABLE; Schema: public; Owner: testuser; Tablespace: 
--

CREATE TABLE table1 (
    id integer NOT NULL,
    strreq character varying(20) NOT NULL,
    stropt character varying(20),
    blnreq boolean NOT NULL,
    i32req integer NOT NULL
);


ALTER TABLE public.table1 OWNER TO testuser;

--
-- Name: table1_id_seq; Type: SEQUENCE; Schema: public; Owner: testuser
--

CREATE SEQUENCE table1_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE public.table1_id_seq OWNER TO testuser;

--
-- Name: table1_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: testuser
--

ALTER SEQUENCE table1_id_seq OWNED BY table1.id;


--
-- Name: table1_id_seq; Type: SEQUENCE SET; Schema: public; Owner: testuser
--

SELECT pg_catalog.setval('table1_id_seq', 3, true);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: testuser
--

ALTER TABLE table1 ALTER COLUMN id SET DEFAULT nextval('table1_id_seq'::regclass);


--
-- Data for Name: table1; Type: TABLE DATA; Schema: public; Owner: testuser
--

COPY table1 (id, strreq, stropt, blnreq, i32req) FROM stdin;
1	foo	bar	t	1234567890
2	baz		t	5432
3	※‣⁈	\N	f	-987654321
\.


--
-- Name: table1_pkey; Type: CONSTRAINT; Schema: public; Owner: testuser; Tablespace: 
--

ALTER TABLE ONLY table1
    ADD CONSTRAINT table1_pkey PRIMARY KEY (id);


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

