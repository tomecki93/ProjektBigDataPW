import snowflake.connector as sf
import pandas as pd

username = 
password = 
account = 
warehouse = 'COMPUTE_WH'
databese = 'SNOWFLAKE_SAMPLE_DATA'

ctx=sf.connect(user=username,password=password,account=account)

def test_connection(connection, query):
    cursor=connection.cursor()
    cursor.execute(query)
    cursor.close()


try:
    #sql='user {}'.format(databese)
    sql= 'SELECT C_NAME, C_PHONE, C_ACCTBAL FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER LIMIT 2'
    test_connection(ctx, sql)

    #sql = 'SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER LIMIT 2'
    cursor=ctx.cursor()
    cursor.execute(sql)
    df = pd.DataFrame.from_records(iter(cursor), columns= [x[0] for x in cursor.description])
    print(df)
#    for c in cursor:
#        print(c)

except Exception as e:
    print(e)