# import numpy as np
import os
import sys
import mysql_class
import spark_class
# import ml_model
# import matplotlib.pyplot as plt
# from statsmodels.tsa.arima.model import ARIMA
# from statsmodels.graphics.tsaplots import plot_predict
# from pyspark.sql import functions as sfunc
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import netifaces


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = "C:/winutils/"

COVID_CSV = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

USER = "apple"
PASSWORD = "apple2"
LOCALHOST = netifaces.gateways()['default'][2][0]
DRIVER = 'com.mysql.cj.jdbc.Driver'
MYSQL_PORT = "3306"

default_args = {
    'owner': 'apple'#,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5)
}


@task()
def get_latest_date():
    mysql = mysql_class.MySql(LOCALHOST, "covid", USER, PASSWORD)
    mysql.mycursor.execute('SELECT MAX(date) FROM country_covid;')
    last_date = (mysql.get_result()[0][0]).strftime('%Y-%m-%d')
    print(f"Latest date in country_covid table: {last_date}.")
    return last_date


@task()
def new_data_view(last_date):
    dirty_mysql = mysql_class.MySql(LOCALHOST, "covid_dirty", USER, PASSWORD)
    dirty_mysql.mycursor.execute(f'''CREATE OR REPLACE VIEW new_data AS
                                    SELECT * FROM covid_dirty
                                    WHERE DATE(date) > "{last_date}"''')


@task
def csv_to_dirty():
    spark = spark_class.SparkClass("covid")
    spark.init_sql(USER, PASSWORD, LOCALHOST)
    df = spark.csv_to_df(COVID_CSV)
    print(f"\nROW COUNTS: {df.count()}\n")
    spark.df_to_sql(db='covid_dirty', table='covid_dirty', df=df, mode='overwrite')


@task
def dirty_sql_to_clean(new_data=True):
    spark = spark_class.SparkClass("covid")
    spark.init_sql(USER, PASSWORD, LOCALHOST)
    if new_data:
        table = 'new_data'
    else:
        table = 'covid_dirty'
    df = spark.sql_to_df(db='covid_dirty', table=table)
    df.printSchema()

    continent_covid = df.select('iso_code', 'date', 'new_cases', 'total_cases',
                                'new_deaths', 'total_deaths',
                                'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
                                'total_boosters', 'new_vaccinations') \
                    .filter(df['continent'].isNull())

    country_covid = df.select('iso_code', 'date', 'new_cases', 'total_cases',
                              'new_deaths', 'total_deaths',
                              'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
                              'total_boosters', 'new_vaccinations') \
                    .filter(df['continent'].isNotNull())

    table_names = ['continent_covid', 'country_covid']
    tables = [continent_covid, country_covid]
    mode = "append"

    if not new_data:
        country_info = df.select('iso_code', 'location', 'population', 'gdp_per_capita') \
            .filter(df['continent'].isNotNull()) \
            .dropDuplicates(['location', 'iso_code'])

        continent_info = df.select('iso_code', 'location', 'population') \
            .filter(df['continent'].isNull()) \
            .dropDuplicates(['location', 'iso_code'])

        table_names += ['continent_info', 'country_info']
        tables += [continent_info, country_info]
        mode = "overwrite"

    print(f"\nUpdating database with '{mode}' mode.\n")

    spark.print_df_full(continent_covid)
    for i, j in enumerate(table_names):
        spark.df_to_sql(db='covid', table=j, df=tables[i], mode=mode, columntype='iso_code VARCHAR(10)')


def create_view(mysql):
    mysql.mycursor.execute('''CREATE OR REPLACE VIEW continent_name_perc_covid AS 
                        SELECT location, date, (total_cases/population)*100 percentage 
                        FROM continent_info JOIN continent_covid
                        ON continent_info.iso_code = continent_covid.iso_code 
                        WHERE population IS NOT NULL
                        AND location <> "European Union"
                        AND location <> "High income" 
                        AND location <> "Low income" 
                        AND location <> "Upper middle income" 
                        AND location <> "World" 
                        AND location <> "Lower middle income"''')

    mysql.mycursor.execute('''CREATE OR REPLACE VIEW country_name_covid AS 
                        SELECT location, population, total_cases, people_fully_vaccinated, 
                        total_cases/population covid_perc 
                        FROM country_info JOIN (
                            SELECT country_covid.*
                            FROM country_covid JOIN (
                                SELECT iso_code, MAX(date) max_date FROM country_covid 
                                WHERE people_fully_vaccinated IS NOT NULL
                                GROUP BY iso_code 
                            ) cc1 
                            ON country_covid.iso_code = cc1.iso_code
                            AND country_covid.date = cc1.max_date
                        ) cc2
                        ON country_info.iso_code = cc2.iso_code   
                        ORDER BY total_cases DESC''')

    mysql.mycursor.execute('''CREATE OR REPLACE VIEW malaysia_covid AS 
                        SELECT location, population, date, people_fully_vaccinated, total_cases
                        FROM country_covid JOIN country_info 
                        ON country_covid.iso_code = country_info.iso_code
                        WHERE location="Malaysia"''')

    mysql.mycursor.execute('''CREATE OR REPLACE VIEW world_covid AS 
                        SELECT date, total_cases
                        FROM continent_covid JOIN continent_info
                        ON continent_covid.iso_code = continent_info.iso_code
                        WHERE location= "World" 
                        ORDER BY date ASC''')

    mysql.mycursor.execute('''CREATE OR REPLACE VIEW country_gdp_death_covid AS 
                        SELECT location, gdp_per_capita, (total_deaths/total_cases)*100 death_perc
                        FROM country_info JOIN (
                            SELECT country_covid.*
                            FROM country_covid JOIN (
                                SELECT iso_code, MAX(date) max_date FROM country_covid 
                                GROUP BY iso_code 
                            ) cc1 
                            ON country_covid.iso_code = cc1.iso_code
                            AND country_covid.date = cc1.max_date
                        ) cc2
                        ON country_info.iso_code = cc2.iso_code    
                        WHERE gdp_per_capita IS NOT NULL
                        AND total_deaths IS NOT NULL
                        AND total_cases IS NOT NULL
                        ORDER BY total_cases DESC''')
    

def set_constraints(mysql):
    mysql.mycursor.execute('ALTER TABLE continent_info ADD CONSTRAINT PK_continent_info PRIMARY KEY (iso_code)')
    mysql.mycursor.execute('ALTER TABLE country_info ADD CONSTRAINT PK_country_info PRIMARY KEY (iso_code)')

    tables = ['continent_covid', 'country_covid']
    for table in tables:
        mysql.mycursor.execute(f'ALTER TABLE {table} ADD CONSTRAINT {table}_index UNIQUE (date, iso_code)')
        mysql.mycursor.execute(f'ALTER TABLE {table} MODIFY date DATE NOT NULL')
        mysql.mycursor.execute(f'ALTER TABLE {table} MODIFY iso_code VARCHAR(10) NOT NULL')
        mysql.mycursor.execute(f'ALTER TABLE {table} ADD id INT NOT NULL AUTO_INCREMENT PRIMARY KEY')

    # mysql.mycursor.execute(
    #     'ALTER TABLE continent_covid ADD CONSTRAINT FK_continent_covid FOREIGN KEY (iso_code) REFERENCES continent_info(iso_code)')
    # mysql.mycursor.execute(
    #     'ALTER TABLE continent_death ADD CONSTRAINT FK_continent_death FOREIGN KEY (iso_code) REFERENCES continent_info(iso_code)')
    # mysql.mycursor.execute(
    #     'ALTER TABLE continent_vac ADD CONSTRAINT FK_continent_vac FOREIGN KEY (iso_code) REFERENCES continent_info(iso_code)')
    # mysql.mycursor.execute(
    #     'ALTER TABLE country_covid ADD CONSTRAINT FK_country_covid FOREIGN KEY (iso_code) REFERENCES country_info(iso_code)')
    # mysql.mycursor.execute(
    #     'ALTER TABLE country_death ADD CONSTRAINT FK_country_death FOREIGN KEY (iso_code) REFERENCES country_info(iso_code)')
    # mysql.mycursor.execute(
    #     'ALTER TABLE country_vac ADD CONSTRAINT FK_country_vac FOREIGN KEY (iso_code) REFERENCES country_info(iso_code)')61


def prediction(spark):
    world_data = spark.sql_to_df('covid', 'world_covid')
    total_days = world_data.count()
    # train_days = int(total_days/2)
    x_days = np.arange(total_days)
    # x_days = np.arange(0, 100)
    # y_cases = np.sin(x_days)
    y_cases = world_data.select("total_cases").rdd.flatMap(lambda x: x).collect()

    # covid_model = ml_model.NeuralNetworkReg("covid_model")
    # covid_model.train(x_days, y_cases, hidden_layer_sizes=[100,100], max_iter=50000)
    # covid_model = ml_model.PolynomialRegressionModel("covid_model", 3)
    # covid_model.train(x_days, y_cases)
    # y_pred = covid_model.get_predictions(x_days)
    covid_model = ARIMA(x_days, y_cases).fit()
    y_pred = covid_model.get_prediction(x_days)
    plot_pred_actual(x_days, y_cases, y_pred)


def plot_pred_actual(x, y, y_pred):
    plt.plot(x, y, label="actual")
    plt.plot(x, y_pred, label="predict")
    plt.legend(['actual','predict'])
    plt.show()


# if __name__ == "__main__":
#     main()


@dag(dag_id='covid_python_operator_taskflow_api',
     default_args=default_args,
     start_date=datetime(2022, 9, 5),
     schedule_interval='*/2 * * * *', #'@daily', #
     catchup=False)
def covid_etl():
    get_latest_date_ = get_latest_date()
    csv_to_dirty_ = csv_to_dirty()
    new_data_view_ = new_data_view(get_latest_date_)
    dirty_sql_to_clean_ = dirty_sql_to_clean(new_data=True)
    [get_latest_date_, csv_to_dirty_] >> new_data_view_ >> dirty_sql_to_clean_

    # set_constraints(mysql)
    # create_view(mysql)

    # prediction(spark)
    # mysql.mycursor.execute("DESCRIBE country_covid;")
    # mysql.print('')

covid_dag = covid_etl()
