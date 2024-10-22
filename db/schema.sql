--
-- PostgreSQL database dump
--

-- Dumped from database version 17.0 (Debian 17.0-1.pgdg120+1)
-- Dumped by pg_dump version 17.0 (Debian 17.0-1.pgdg110+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: destinations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.destinations (
    name character varying(100) NOT NULL,
    port_id integer
);


ALTER TABLE public.destinations OWNER TO postgres;

--
-- Name: flags; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.flags (
    id integer NOT NULL,
    flag character varying(12) NOT NULL,
    country character varying(40)
);


ALTER TABLE public.flags OWNER TO postgres;

--
-- Name: TABLE flags; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.flags IS 'Флаги и страны судов';


--
-- Name: COLUMN flags.flag; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.flags.flag IS 'Код флага';


--
-- Name: COLUMN flags.country; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.flags.country IS 'Название страны';


--
-- Name: flags_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.flags_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.flags_id_seq OWNER TO postgres;

--
-- Name: flags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.flags_id_seq OWNED BY public.flags.id;


--
-- Name: parses; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.parses (
    id integer NOT NULL,
    start timestamp with time zone NOT NULL,
    "end" timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    description character varying(200)
);


ALTER TABLE public.parses OWNER TO postgres;

--
-- Name: TABLE parses; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.parses IS 'Информация о парсингах';


--
-- Name: COLUMN parses.start; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.parses.start IS 'Время начала парсинга';


--
-- Name: COLUMN parses."end"; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.parses."end" IS 'Время конца парсинга';


--
-- Name: COLUMN parses.description; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.parses.description IS 'Описание парсера (название устройства / кластера)';


--
-- Name: parses_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.parses_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.parses_id_seq OWNER TO postgres;

--
-- Name: parses_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.parses_id_seq OWNED BY public.parses.id;


--
-- Name: ports; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ports (
    id integer NOT NULL,
    name character varying(200),
    location point
);


ALTER TABLE public.ports OWNER TO postgres;

--
-- Name: ports_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ports_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ports_id_seq OWNER TO postgres;

--
-- Name: ports_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ports_id_seq OWNED BY public.ports.id;


--
-- Name: positions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.positions (
    id bigint NOT NULL,
    ship_id integer NOT NULL,
    "timestamp" timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    location point NOT NULL,
    speed smallint,
    course smallint,
    heading smallint,
    rot smallint,
    dwt integer,
    type smallint,
    gt_type smallint,
    parse_id integer,
    destination character varying
);


ALTER TABLE public.positions OWNER TO postgres;

--
-- Name: TABLE positions; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.positions IS 'Записи о расположении суден';


--
-- Name: COLUMN positions.ship_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions.ship_id IS 'Судно';


--
-- Name: COLUMN positions."timestamp"; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions."timestamp" IS 'Время, когда судно было обнаружено';


--
-- Name: COLUMN positions.location; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions.location IS 'Географическое расположение судна';


--
-- Name: COLUMN positions.speed; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions.speed IS 'Скорость судна в узлах';


--
-- Name: COLUMN positions.course; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions.course IS 'Курс судна относительно севера (в градусах), указывающий направление движения';


--
-- Name: COLUMN positions.heading; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions.heading IS 'Курс судна относительно севера (в градусах), указывающий направление, куда обращён нос судна (может отличаться от фактического курса)';


--
-- Name: COLUMN positions.rot; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions.rot IS 'Скорость поворота судна (Rate of Turn), измеряемая в градусах в минуту. 0 означает, что судно не поворачивает';


--
-- Name: COLUMN positions.dwt; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions.dwt IS 'Дедвейт (Deadweight Tonnage), максимальная грузоподъёмность судна в тоннах';


--
-- Name: COLUMN positions.type; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions.type IS 'Общий тип судна';


--
-- Name: COLUMN positions.gt_type; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.positions.gt_type IS 'Расширенный код';


--
-- Name: positions_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.positions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.positions_id_seq OWNER TO postgres;

--
-- Name: positions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.positions_id_seq OWNED BY public.positions.id;


--
-- Name: ships; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ships (
    id integer NOT NULL,
    ship_id character varying(256) NOT NULL,
    name character varying(256),
    flag_id integer,
    width smallint,
    l_fore smallint,
    w_left smallint,
    length smallint
);


ALTER TABLE public.ships OWNER TO postgres;

--
-- Name: TABLE ships; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.ships IS 'Зарегистрированные суда';


--
-- Name: COLUMN ships.id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.ships.id IS 'Внутренний идентификатор судна';


--
-- Name: COLUMN ships.ship_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.ships.ship_id IS 'ID судна с сайта marine traffic';


--
-- Name: COLUMN ships.name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.ships.name IS 'Название судна';


--
-- Name: COLUMN ships.flag_id; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.ships.flag_id IS 'Флаг судна';


--
-- Name: COLUMN ships.width; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.ships.width IS 'Ширина судна (в метрах)';


--
-- Name: COLUMN ships.l_fore; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.ships.l_fore IS 'Расстояние от носа судна до мачты или другой точки (в метрах)';


--
-- Name: COLUMN ships.w_left; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.ships.w_left IS 'Расстояние от осевой линии судна до левого борта (в метрах)';


--
-- Name: COLUMN ships.length; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.ships.length IS 'Длина судна (в метрах)';


--
-- Name: ships_flag_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ships_flag_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ships_flag_id_seq OWNER TO postgres;

--
-- Name: ships_flag_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ships_flag_id_seq OWNED BY public.ships.flag_id;


--
-- Name: ships_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ships_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ships_id_seq OWNER TO postgres;

--
-- Name: ships_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ships_id_seq OWNED BY public.ships.id;


--
-- Name: flags id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.flags ALTER COLUMN id SET DEFAULT nextval('public.flags_id_seq'::regclass);


--
-- Name: parses id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.parses ALTER COLUMN id SET DEFAULT nextval('public.parses_id_seq'::regclass);


--
-- Name: ports id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ports ALTER COLUMN id SET DEFAULT nextval('public.ports_id_seq'::regclass);


--
-- Name: positions id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.positions ALTER COLUMN id SET DEFAULT nextval('public.positions_id_seq'::regclass);


--
-- Name: ships id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ships ALTER COLUMN id SET DEFAULT nextval('public.ships_id_seq'::regclass);


--
-- Name: destinations destinations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.destinations
    ADD CONSTRAINT destinations_pkey PRIMARY KEY (name);


--
-- Name: flags flags_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.flags
    ADD CONSTRAINT flags_pkey PRIMARY KEY (id);


--
-- Name: parses parses_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.parses
    ADD CONSTRAINT parses_pkey PRIMARY KEY (id);


--
-- Name: ports ports_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ports
    ADD CONSTRAINT ports_pkey PRIMARY KEY (id);


--
-- Name: positions positions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_pkey PRIMARY KEY (id);


--
-- Name: ships ships_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ships
    ADD CONSTRAINT ships_pkey PRIMARY KEY (id);


--
-- Name: destinations destinations_port_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.destinations
    ADD CONSTRAINT destinations_port_id_fkey FOREIGN KEY (port_id) REFERENCES public.ports(id) ON UPDATE CASCADE ON DELETE SET NULL NOT VALID;


--
-- Name: positions positions_destination_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_destination_fkey FOREIGN KEY (destination) REFERENCES public.destinations(name) ON UPDATE CASCADE ON DELETE SET NULL NOT VALID;


--
-- Name: positions positions_parse_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_parse_id_fkey FOREIGN KEY (parse_id) REFERENCES public.parses(id) ON UPDATE CASCADE ON DELETE SET NULL NOT VALID;


--
-- Name: positions positions_ship_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_ship_id_fkey FOREIGN KEY (ship_id) REFERENCES public.ships(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: ships ships_flag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ships
    ADD CONSTRAINT ships_flag_id_fkey FOREIGN KEY (flag_id) REFERENCES public.flags(id) ON DELETE SET NULL;


--
-- PostgreSQL database dump complete
--

