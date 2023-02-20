import boto3
from botocore.errorfactory import ClientError
import psycopg2


def check_json_inplace(keyword,date,s3_client): 
    '''
    Check if json file has been succesfully uploaded
    '''
    date = str(date.date())
    key = keyword +'/'+date
    try:
        s3_client.head_object(Bucket='santal', Key=f'tweets/{key}.json')

    except KeyError:
        raise ValueError('JSON not Found for ',key)


def check_etl_result(keyword,date,redshift):
    '''
    Check if data has been loaded into redshift by looking at mapping table
    '''

    lut_key = keyword + '_' + str(date.date())
    query = f"SELECT * FROM mapping_table where keyword_date = '{lut_key}'"
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(redshift['DWH_DB_USER'], redshift['DWH_DB_PASSWORD'], redshift['DWH_ENDPOINT'], redshift['DWH_PORT'],redshift['DWH_DB'])
    con = psycopg2.connect(conn_string)
    cur = con.cursor()
    cur.execute(query)
    if cur.fetchone() == None:
        raise ValueError('ETL Failed for ',lut_key)
        con.close()
    con.close()

    

