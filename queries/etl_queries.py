
etl_dim_users = '''
INSERT INTO dim_users
SELECT incoming_users.* FROM
(SELECT CAST(id AS BIGINT) AS user_id,
	location AS user_location,
    name AS username,
    CASE WHEN verified = 'f' then FALSE ELSE TRUE END AS verified,
    verified_type,
    description AS user_description
FROM staging_users) incoming_users
LEFT JOIN dim_users
ON incoming_users.user_id = dim_users.user_id
WHERE dim_users.user_id IS NULL
'''

etl_dim_places = '''
INSERT INTO dim_places
SELECT incoming_places.* FROM
(SELECT id AS place_id,
  country,
  full_name,
  place_type
FROM staging_places) incoming_places
LEFT JOIN dim_places
ON incoming_places.place_id = dim_places.place_id
WHERE dim_places.place_id IS NULL
'''

etl_fact_tweets = '''
INSERT INTO fact_tweets
SELECT incoming_tweets.* FROM
(SELECT CAST (id AS BIGINT) AS tweet_id,
	CAST (possibly_sensitive AS BOOLEAN) AS possibly_sensitive,
    TO_TIMESTAMP(created_at,'YYYY-MM-DD HH24:MI:SS') AS dt,
    json_extract_path_text(geo,'place_id') AS place_id,
    CAST(author_id AS BIGINT) AS user_id,
    CASE WHEN len(json_extract_array_element_text(edit_history_tweet_ids,1)) > 0 THEN TRUE ELSE FALSE END AS is_edited,
    text,
    CAST(json_extract_path_text(public_metrics,'retweet_count') AS INT) AS retweet_cnt,
    CAST(json_extract_path_text(public_metrics,'reply_count') AS INT) AS reply_cnt,
    CAST(json_extract_path_text(public_metrics,'like_count') AS INT) AS like_cnt,
    CAST(json_extract_path_text(public_metrics,'quote_count') AS INT) AS quote_cnt,  
    CAST(json_extract_path_text(public_metrics,'impression_count') AS INT) AS impression_cnt                                      
FROM staging_tweets) incoming_tweets
LEFT JOIN fact_tweets
ON incoming_tweets.tweet_id = fact_tweets.tweet_id
WHERE fact_tweets.tweet_id IS NULL
'''

etl_mapping_table = '''
INSERT INTO mapping_table
SELECT CAST (id AS BIGINT) AS tweet_id,
concat(keyword,TO_CHAR(TO_TIMESTAMP(created_at,'YYYY-MM-DD HH24:MI:SS'), '_YYYY-MM-DD')) as keyword_date
FROM staging_tweets
'''