CREATE TABLE "stg_links" (
  "movieid" bigint PRIMARY KEY NOT NULL,
  "imdbid" bigint,
  "tmdbid" bigint
);

CREATE TABLE "stg_keyword" (
  "userid" bigint,
  "id" bigint,
  "keywords" varchar(5000)
);

CREATE TABLE "stg_ratings" (
  "userid" bigint,
  "movieid" bigint,
  "rating" int,
  "timestamp_" TIMESTAMP
);

CREATE TABLE "stg_creadits_cast" (
  "id" bigint,
  "actor_id" bigint,
  "cast_id" int,
  "character" varchar(500)
);

CREATE TABLE "stg_creadits_crew" (
  "id" bigint,
  "crew_id" bigint,
  "department" varchar(500),
  "job" varchar(500)
);

CREATE TABLE "stg_person" (
  "id" bigint,
  "name_" varchar(256),
  "original_name" varchar(500),
  "know_for" varchar(500),
  "popularity" float,
  "profile_path" varchar(500)
);

CREATE TABLE "stg_movie_metadata" (
  "id" bigint,
  "title" varchar(500),
  "original_title" varchar(500),
  "original_language" varchar(500),
  "release_date" date,
  "status" varchar(500),
  "overview" varchar(5000),
  "tagline" varchar(5000),
  "adult" varchar(500),
  "popularity" float,
  "homepage" varchar(500),
  "poster_path" varchar(500),
  "runtime" int,
  "budget" bigint,
  "revenue" bigint,
  "vote_average" float,
  "vote_count" int
);

CREATE TABLE "stg_genres" (
  "id" bigint,
  "genres" varchar(500)
);

CREATE TABLE "stg_collection" (
  "id" bigint,
  "collect_id" bigint,
  "name_" varchar(500)
);

COMMENT ON COLUMN "stg_links"."movieid" IS 'ID phim do grouplen đặt';

COMMENT ON COLUMN "stg_links"."imdbid" IS 'Mã id của phim trong imdb database';

COMMENT ON COLUMN "stg_links"."tmdbid" IS 'Mã id của phim trong tmdb database';

COMMENT ON COLUMN "stg_keyword"."userid" IS 'Mã ID của user';

COMMENT ON COLUMN "stg_keyword"."id" IS 'mã tmdbid';

COMMENT ON COLUMN "stg_keyword"."keywords" IS 'Tag do user đặt';

COMMENT ON COLUMN "stg_ratings"."userid" IS 'Mã ID của user';

COMMENT ON COLUMN "stg_ratings"."movieid" IS 'ID phim do grouplen đặt';

COMMENT ON COLUMN "stg_ratings"."rating" IS 'điểm rating do user vote';

COMMENT ON COLUMN "stg_ratings"."timestamp_" IS 'thời gian vote';

COMMENT ON COLUMN "stg_genres"."id" IS 'ID phim do grouplen đặt';

COMMENT ON COLUMN "stg_genres"."genres" IS 'Thể loại phim';

ALTER TABLE "stg_ratings" ADD FOREIGN KEY ("movieid") REFERENCES "stg_links" ("movieid");

ALTER TABLE "stg_genres" ADD FOREIGN KEY ("id") REFERENCES "stg_links" ("tmdbid");

ALTER TABLE "stg_movie_metadata" ADD FOREIGN KEY ("id") REFERENCES "stg_links" ("tmdbid");

ALTER TABLE "stg_collection" ADD FOREIGN KEY ("id") REFERENCES "stg_links" ("tmdbid");

ALTER TABLE "stg_keyword" ADD FOREIGN KEY ("id") REFERENCES "stg_links" ("tmdbid");

ALTER TABLE "stg_creadits_cast" ADD FOREIGN KEY ("id") REFERENCES "stg_links" ("tmdbid");

ALTER TABLE "stg_creadits_crew" ADD FOREIGN KEY ("id") REFERENCES "stg_links" ("tmdbid");

ALTER TABLE "stg_creadits_crew" ADD FOREIGN KEY ("crew_id") REFERENCES "stg_person" ("id");

ALTER TABLE "stg_creadits_cast" ADD FOREIGN KEY ("cast_id") REFERENCES "stg_person" ("id");
