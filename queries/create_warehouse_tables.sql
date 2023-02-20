DROP TABLE IF EXISTS "fact_tweets";
CREATE TABLE "fact_tweets" (
    "tweet_id" BIGINT PRIMARY KEY,
  	"possibly_sensitive" BOOLEAN,
    "dt" TIMESTAMP DISTKEY,
  	"geo_id" VARCHAR,
    "user_id" VARCHAR,
    "is_edited" BOOLEAN,
    "text" VARCHAR(10000),
  	"retweet_cnt" INT,
    "reply_cnt" INT,
  	"like_cnt" INT,
    "quote_cnt" INT,
  	"impression_cnt" INT
);

DROP TABLE IF EXISTS "dim_users";
CREATE TABLE "dim_users" (
    "user_id" BIGINT PRIMARY KEY DISTKEY,
    "user_location" VARCHAR (1000),
  	"username" VARCHAR,
  	"verified" BOOLEAN,
  	"verified_type" VARCHAR,
  	"user_description" VARCHAR (1000)
);

DROP TABLE IF EXISTS "dim_places";
CREATE TABLE "dim_places" (
    "place_id" VARCHAR PRIMARY KEY DISTKEY,
    "country" VARCHAR,
  	"full_name" VARCHAR,
  	"place_type" VARCHAR
);

DROP TABLE IF EXISTS "mapping_table";
CREATE TABLE "mapping_table" (
    "tweet_id" BIGINT,
    "keyword_date" VARCHAR DISTKEY
);
