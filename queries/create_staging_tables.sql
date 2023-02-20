DROP TABLE IF EXISTS "staging_tweets";
CREATE TABLE "staging_tweets" (
    "id" VARCHAR,
    "possibly_sensitive" BOOLEAN,
    "keyword" VARCHAR,
  	"geo" VARCHAR,
    "author_id" VARCHAR,
    "created_at" VARCHAR,
    "edit_history_tweet_ids" VARCHAR,
    "text" varchar(65535),
    "public_metrics" VARCHAR
);

DROP TABLE IF EXISTS "staging_places";
CREATE TABLE "staging_places" (
    "id" VARCHAR,
  	"geo" VARCHAR,
    "country" VARCHAR,
    "full_name" VARCHAR,
    "contained_within" VARCHAR,
  	"place_type" VARCHAR
);

DROP TABLE IF EXISTS "staging_users";
CREATE TABLE "staging_users" (
    "id" VARCHAR,
  	"location" VARCHAR,
    "name" VARCHAR,
    "verified" VARCHAR,
    "verified_type" VARCHAR,
  	"description" VARCHAR(1000)
);
