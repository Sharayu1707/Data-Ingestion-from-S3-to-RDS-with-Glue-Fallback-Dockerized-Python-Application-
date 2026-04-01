import pandas as pd
import boto3
from sqlalchemy import create_engine

# CONFIG
S3_BUCKET = "your-bucket"
S3_KEY = "data.csv"

RDS_ENDPOINT = "your-endpoint"
RDS_USER = "admin"
RDS_PASSWORD = "password123"
RDS_DB = "testdb"
TABLE_NAME = "mytable"

GLUE_DB = "my_glue_db"
GLUE_TABLE = "my_glue_table"

def read_from_s3():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    df = pd.read_csv(obj['Body'])
    return df

def upload_to_rds(df):
    try:
        engine = create_engine(
            f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_ENDPOINT}/{RDS_DB}"
        )
        df.to_sql(TABLE_NAME, con=engine, if_exists='replace', index=False)
        print("✅ Data uploaded to RDS")
        return True
    except Exception as e:
        print("❌ RDS failed:", e)
        return False

def fallback_to_glue():
    glue = boto3.client('glue')
    glue.create_table(
        DatabaseName=GLUE_DB,
        TableInput={
            'Name': GLUE_TABLE,
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'col1', 'Type': 'string'}
                ],
                'Location': f's3://{S3_BUCKET}/',
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe'
                }
            },
            'TableType': 'EXTERNAL_TABLE'
        }
    )
    print("⚠️ Fallback to Glue successful")

if __name__ == "__main__":
    df = read_from_s3()
    success = upload_to_rds(df)

    if not success:
        fallback_to_glue()
