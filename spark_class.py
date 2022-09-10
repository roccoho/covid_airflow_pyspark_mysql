from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark import SparkContext, SparkConf, SparkFiles
import urllib.request

class SparkClass:
    def __init__(self, appName, config=None):
        self.appName = appName
        self.user = ''
        self.password = ''
        if config:
            configs = SparkConf().setAll(config)
            self.spark = SparkSession.builder.appName("covid").config(conf=configs).getOrCreate()

        else:
            self.spark = SparkSession.builder.appName("covid").getOrCreate()

    def init_sql(self, user, password, host):
        self.user = user
        self.password = password
        self.host = host 
        self.driver = 'com.mysql.cj.jdbc.Driver'

    def df_to_sql(self, db, table, df, mode='error', columntype=''):
        if columntype:
            df.write.format('jdbc') \
                .options(
                    url=f'jdbc:mysql://{self.host}/{db}',
                    driver=self.driver, #'com.mysql.cj.jdbc.Driver',
                    dbtable=table,
                    user=self.user,
                    password=self.password) \
                .option("createTableColumnTypes", columntype)\
                .mode(mode).save()

        else:
            df.write.format('jdbc').options(
                url=f'jdbc:mysql://{self.host}/{db}',
                driver=self.driver, #'com.mysql.cj.jdbc.Driver',
                dbtable=table,
                user=self.user,
                password=self.password).mode(mode).save()

    def sql_to_df(self, db, table):
        df = self.spark.read.format('jdbc').options(
                url=f'jdbc:mysql://{self.host}/{db}',
                driver=self.driver, #'com.mysql.cj.jdbc.Driver',
                dbtable=table,
                user=self.user,
                password=self.password).load()
        return df

    def csv_to_df(self, link, header=True, inferSchema=True):
        # self.spark.sparkContext.addFile(link)
        # csv_file = SparkFiles.get(link.split('/')[-1])
        csv_file = "csv_file.csv"
        urllib.request.urlretrieve(link, csv_file)
        print("\n\ncsv_file done\n\n")
        return self.spark.read.csv(csv_file, header=header, inferSchema=inferSchema)

    def print_df_full(self, df):
        print(df.show(df.count(), truncate=False))
