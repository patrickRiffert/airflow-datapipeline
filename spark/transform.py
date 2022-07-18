from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime
import argparse
from os.path import join

def get_tweets_data(df):
    return df.select(f.explode('data').alias('tweets')).select('tweets.*')

def get_users_data(df):
    return df.select(f.explode('includes.users').alias('users')).select('users.*')

def export_json(df, dest):
    df.write.mode('overwrite').json(dest)
# def export_csv(df, dest):
#     df.write.mode('overwrite').option('delimiter','|').csv(dest)
    # df.write.mode('overwrite').option("delimiter", "\t").csv(dest)

def transform(spark, src, dest):
    df = spark.read.json(src)
    tweets_df = get_tweets_data(df)
    users_df = get_users_data(df)
    # Join + filtro (Lang = 'pt')
    merged_df = users_df.join(tweets_df, users_df.id == tweets_df.author_id)
    merged_df_filter = merged_df.where(merged_df.lang=='pt').select('username','text','lang')
    # merged_df_filter =  merged_df.select('username',f.trim(f.col('text')).alias('text'), 'lang').na.drop() ### TESTE TRIM ###
    merged_df_filter =  merged_df.select('username',f.trim(f.col('text')).alias('text')).na.drop() ### TESTE TRIM ###
    # Export para datalake (silver)
    # table_name = join(dest, f'tweets_{datetime.now().strftime("%Y-%m-%d")}.json')
    table_name = join(dest, 'tweets_last_7_days.json')
    export_json(merged_df_filter, dest)
    # export_csv(merged_df_filter, dest)

if __name__== "__main__":
    parser = argparse.ArgumentParser(
        description='Spark Twitter Transform'
    )
    parser.add_argument('--src', required=True)
    parser.add_argument('--dest', required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName('twitter_transform')\
        .getOrCreate()

    transform(spark, args.src, args.dest)