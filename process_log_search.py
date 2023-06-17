from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import pandas as pd

spark = SparkSession.builder\
        .master('local[*]')\
        .appName('Connection-Test')\
        .config('spark.driver.extraClassPath', '/content/drive/MyDrive/DE - test/mssql-jdbc-12.2.0.jre8.jar')\
        .config('spark.executor.extraClassPath', '/content/drive/MyDrive/DE - test/mssql-jdbc-12.2.0.jre8.jar')\
        .getOrCreate()


def main_task(folder_path):
    df = ETL_all_day(folder_path)
    df_t6 = most_search(df, month = '06')
    df_t7 = most_search(df, month = '07')

    df_t6 = map_key_dict(df_t6, 'Most_Search_T6', 'Category_T6')
    df_t7 = map_key_dict(df_t7, 'Most_Search_T7', 'Category_T7')

    df_all = df_t6.join(df_t7, 'user_id', 'inner')
    condition = col('Category_T6') == col('Category_T7')
    df_all = df_all.withColumn('Category_change', when(condition, 'NoChange').otherwise(concat(df_all['Category_T6'], lit(' - '), df_all['Category_T7'])))

    import_data_azure(Hostname= 'YourHostname', Database= 'YourDatabase', Username= 'YourUsername', Password= 'YourPassword', Port= 1433, data= df_t6, table= 'Most_Search_T6')
    import_data_azure(Hostname= 'YourHostname', Database= 'YourDatabase', Username= 'YourUsername', Password= 'Yourpassword', Port= 1433, data= df_t7, table= 'Most_Search_T7')
    import_data_azure(Hostname= 'YourHostname', Database= 'YourDatabase', Username= 'YourUsername', Password= 'Yourpassword', Port= 1433, data= df_all, table= 'Most_Search_T6_and_T7')

    return print('Task Successfully')


def import_data_azure(Hostname, Database, Username, Password, Port, data, table):
    jdbcUrl = f"jdbc:sqlserver://{Hostname}:{Port};database={Database};user={Username};password={Password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    data.write.jdbc(jdbcUrl,
                   mode ="append",
                   table= table
                   )
    return print(f'Import data to {table} on Azure Database successfully')

def map_key_dict(data, colname, colrename):
    key_dict_pd = pd.read_excel('/content/drive/MyDrive/DE - test/key_dict_1000.xlsx')
    key_dict = spark.createDataFrame(key_dict_pd)
    data = data.join(key_dict, 'Most_Search', 'inner').select('user_id', 'Most_Search', 'Category')
    data = data.withColumnRenamed('Most_Search', colname)\
              .withColumnRenamed('Category', colrename)
    print('Map {} with key dict successfully'.format(data))
    return data


def ETL_1_day(path, name):
    # Đọc file 1 để sau đó union các file khác vào
    file_list = os.listdir(f'{path}/{name}')
    parquet_file = [file_name for file_name in file_list if file_name.endswith(".parquet")]
    df = spark.read.parquet(f'{path}/{name}/{parquet_file[0]}')
    df = df.withColumn('Month', lit(name[4:6]))
    print('Finish processing {}'.format(name))
    return df


# Function xử lý dữ liệu log search
def ETL_all_day(folder_path):
    folder_list = os.listdir(folder_path)
    # Đọc data 1 ngày
    df = ETL_1_day(path = folder_path, name = folder_list[0])
    # Đọc qua tất cả data
    for file in folder_list[1:]:
        df1 = ETL_1_day(path = folder_path, name = file)
        df = df.union(df1)
        df = df.cache()
    # Lọc action là search, user_id và keyword not Null
    df = df.filter((col('action') == 'search') & (col('user_id').isNotNull()) & (col('keyword').isNotNull()))
    print('Done processing all file')
    return df


def most_search(df, month):
    # Tìm ra Most Search T6
    df = df.filter(col('Month') == month)
    df = df.select('user_id', 'keyword', 'Month')
    df = df.groupBy('user_id', 'keyword', 'Month').count()
    df = df.withColumnRenamed('count', 'Total_search').orderBy('Total_search', ascending= False)
    window = Window.partitionBy('user_id').orderBy(col('Total_search').desc())
    df = df.withColumn('Rank', row_number().over(window))
    df = df.filter(col('Rank') == 1)
    df = df.withColumnRenamed('keyword','Most_Search')
    df = df.select('user_id','Most_Search', 'Month')
    return df


main_task(folder_path = '/content/drive/MyDrive/DE - test/log_search')


