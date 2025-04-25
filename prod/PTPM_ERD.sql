CREATE TABLE "stg_links" (
  "movieid" bigint PRIMARY KEY,
  "imdbid" bigint,
  "tmdbid" bigint
);

CREATE TABLE "stg_keyword" (
  "keyid" varchar(5000) PRIMARY KEY,
  "userid" bigint,
  "movieid" bigint,
  "keywords" varchar(5000),
  "date_tags" TIMESTAMP
);

CREATE TABLE "stg_ratings" (
  "keyid" varchar(5000) PRIMARY KEY,
  "userid" bigint,
  "movieid" bigint,
  "rating" int,
  "date_rate" TIMESTAMP
);

CREATE TABLE "stg_user" (
  "userid" bigint PRIMARY KEY,
  "fullname" varchar(5000),
  "age" int,
  "address" varchar(5000)
);

CREATE TABLE "stg_movie_metadata" (
  "id" bigint PRIMARY KEY,
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

CREATE TABLE "stg_cast" (
  "credit_id" varchar(500) PRIMARY KEY,
  "id" bigint,
  "actor_id" bigint,
  "cast_id" int,
  "character" varchar(500)
);

CREATE TABLE "stg_crew" (
  "credit_id" varchar(500) PRIMARY KEY,
  "id" bigint,
  "crew_id" bigint,
  "department" varchar(500),
  "job" varchar(500)
);

CREATE TABLE "stg_person" (
  "id" bigint PRIMARY KEY,
  "imdb_id" varchar(500),
  "fullname" varchar(256),
  "also_name" varchar(5000),
  "gender" int,
  "birthday" date,
  "deathday" date,
  "place_of_birth" varchar(5000),
  "job" varchar(5000),
  "popularity" float,
  "profile_path" varchar(5000),
  "biography" text
);

CREATE TABLE "stg_genres" (
  "keyid" bigint PRIMARY KEY,
  "id" bigint,
  "genres" varchar(500)
);

CREATE TABLE "stg_collection" (
  "keyid" bigint PRIMARY KEY,
  "collect_id" bigint,
  "id" bigint,
  "name_" varchar(5000),
  "poster_path" varchar(5000),
  "backdrop_path" varchar(5000)
);

COMMENT ON COLUMN "stg_links"."movieid" IS 'ID phim do grouplen đặt';

COMMENT ON COLUMN "stg_links"."imdbid" IS 'unique -- Mã id của phim trong imdb database';

COMMENT ON COLUMN "stg_links"."tmdbid" IS 'unique --Mã id của phim trong tmdb database';

COMMENT ON COLUMN "stg_keyword"."keyid" IS 'userid + movieid + timestamp_';

COMMENT ON COLUMN "stg_keyword"."userid" IS 'Mã ID của user';

COMMENT ON COLUMN "stg_keyword"."movieid" IS 'ID phim do grouplen đặt';

COMMENT ON COLUMN "stg_keyword"."keywords" IS 'Tag do user đặt';

COMMENT ON COLUMN "stg_keyword"."date_tags" IS 'thời gian vote';

COMMENT ON COLUMN "stg_ratings"."keyid" IS 'userid + movieid + timestamp_';

COMMENT ON COLUMN "stg_ratings"."userid" IS 'Mã ID của user';

COMMENT ON COLUMN "stg_ratings"."movieid" IS 'ID phim do grouplen đặt';

COMMENT ON COLUMN "stg_ratings"."rating" IS 'điểm rating do user vote';

COMMENT ON COLUMN "stg_ratings"."date_rate" IS 'thời gian vote';

COMMENT ON COLUMN "stg_movie_metadata"."id" IS 'mã tmdbid của phim';

COMMENT ON COLUMN "stg_cast"."credit_id" IS 'mã tạo do hệ thống';

COMMENT ON COLUMN "stg_cast"."id" IS 'tmdbid_id';

COMMENT ON COLUMN "stg_crew"."credit_id" IS 'mã tạo do hệ thống';

COMMENT ON COLUMN "stg_crew"."id" IS 'tmdbid_id';

COMMENT ON COLUMN "stg_genres"."id" IS 'ID phim do grouplen đặt';

COMMENT ON COLUMN "stg_genres"."genres" IS 'Thể loại phim';

ALTER TABLE "stg_keyword" ADD FOREIGN KEY ("movieid") REFERENCES "stg_links" ("movieid");

ALTER TABLE "stg_ratings" ADD FOREIGN KEY ("movieid") REFERENCES "stg_links" ("movieid");

ALTER TABLE "stg_keyword" ADD FOREIGN KEY ("userid") REFERENCES "stg_user" ("userid");

ALTER TABLE "stg_ratings" ADD FOREIGN KEY ("userid") REFERENCES "stg_user" ("userid");

ALTER TABLE "stg_links" ADD FOREIGN KEY ("tmdbid") REFERENCES "stg_movie_metadata" ("id");

ALTER TABLE "stg_cast" ADD FOREIGN KEY ("id") REFERENCES "stg_movie_metadata" ("id");

ALTER TABLE "stg_crew" ADD FOREIGN KEY ("id") REFERENCES "stg_movie_metadata" ("id");

ALTER TABLE "stg_cast" ADD FOREIGN KEY ("actor_id") REFERENCES "stg_person" ("id");

ALTER TABLE "stg_crew" ADD FOREIGN KEY ("crew_id") REFERENCES "stg_person" ("id");

ALTER TABLE "stg_genres" ADD FOREIGN KEY ("id") REFERENCES "stg_movie_metadata" ("id");

ALTER TABLE "stg_collection" ADD FOREIGN KEY ("id") REFERENCES "stg_movie_metadata" ("id");
