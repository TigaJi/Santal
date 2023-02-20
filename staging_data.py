import psycopg2
import boto3

def copy_to_redshift(cursor,item,keyword,date,table,dwh_iam_role):

    '''
    Copy one json file to redshift
    '''
    qry = f'''
      TRUNCATE {table};
      copy {table} from 's3://santal/{item}/{keyword}/{date}.json'
      credentials 'aws_iam_role={dwh_iam_role}'
      format as json 'auto'
    '''
    
    cursor.execute(qry)

def stage_all_tables(keyword,date,redshift):
    '''
    stage the data to redshift
    '''
    
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(redshift['DWH_DB_USER'], redshift['DWH_DB_PASSWORD'], redshift['DWH_ENDPOINT'], redshift['DWH_PORT'],redshift['DWH_DB'])
    con = psycopg2.connect(conn_string)
    cur = con.cursor()

    date = str(date.date())
    
    copy_to_redshift(cur,'tweets',keyword,date,'staging_tweets',redshift['DWH_IAM_ROLE'])
    copy_to_redshift(cur,'users',keyword,date,'staging_users',redshift['DWH_IAM_ROLE'])
    copy_to_redshift(cur,'places',keyword,date,'staging_places',redshift['DWH_IAM_ROLE'])
    
    print("finished staging to redshift")

    con.close()
    