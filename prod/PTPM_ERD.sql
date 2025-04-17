CREATE TABLE staging.stg_tmdb_json_movie_metadata (
  id bigint,
  request_json text
);

CREATE TABLE staging.stg_tmdb_json_movie_credits (
  id bigint,
  request_json text
);

CREATE TABLE staging.stg_links (
  movieid bigint PRIMARY KEY NOT NULL,
  imdbid bigint,
  tmdbid bigint
);

CREATE TABLE staging.stg_keyword (
  userid bigint,
  id bigint,
  keywords varchar(5000)
);

CREATE TABLE staging.stg_ratings (
  userid bigint,
  movieid bigint,
  rating int,
  timestamp_ TIMESTAMP
);

CREATE TABLE staging.stg_creadits_cast (
  id bigint,
  actor_id bigint,
  cast_id int,
  character varchar(500)
);

CREATE TABLE staging.stg_creadits_crew (
  id bigint,
  crew_id bigint,
  department varchar(500),
  job varchar(500)
);

CREATE TABLE staging.stg_person (
  id bigint,
  name_ varchar(256),
  original_name varchar(500),
  know_for varchar(500),
  popularity float,
  profile_path varchar(500)
);

CREATE TABLE staging.stg_movie_metadata (
  id bigint,
  title varchar(500),
  original_title varchar(500),
  original_language varchar(500),
  release_date date,
  status varchar(500),
  overview varchar(5000),
  tagline varchar(5000),
  adult varchar(500),
  popularity float,
  homepage varchar(500),
  poster_path varchar(500),
  runtime int,
  budget bigint,
  revenue bigint,
  vote_average float,
  vote_count int
);

CREATE TABLE staging.stg_genres (
  id bigint,
  genres varchar(500)
);

CREATE TABLE staging.stg_collection (
  id bigint,
  collect_id bigint,
  name_ varchar(500)
);
