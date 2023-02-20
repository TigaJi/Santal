from queries.etl_queries import etl_fact_tweets,etl_dim_places,etl_dim_users,etl_mapping_table
import psycopg2

def perform_etl(redshift):
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(redshift['DWH_DB_USER'], redshift['DWH_DB_PASSWORD'], redshift['DWH_ENDPOINT'], redshift['DWH_PORT'],redshift['DWH_DB'])
    con = psycopg2.connect(conn_string)
    cur = con.cursor()

    print("loading fact table...")
    cur.execute(etl_fact_tweets)
    cur.execute(etl_mapping_table)
    con.commit()

    print("updating dimention tables...")
    cur.execute(etl_dim_users)
    cur.execute(etl_dim_places)
    con.commit()
    
    con.close()
    print('finished populating data warehouse')

