"""
etl_job.py
~~~~~~~~~~
Script para execução do job Spark:
    $SPARK_HOME/bin/spark-submit \
        --master local[*] \
        --py-files packages.zip $HOME/projects/pyspark-project-vgsales/jobs/etl_job.py
"""
import pyspark.pandas as pspd
import numpy as np
import pandas as pd

from dependencies.spark import start_spark


def main():
    # Inicializar a aplicação Spark e obter sua session e logger
    spark, log = start_spark(
        app_name='my_etl_job')
    log.warn('etl_job is up-and-running')

    # Execução do pipeline ETL
    data = extract_data(spark)
    #data_transformed = transform_data(data, "SUM_GLOBAL_SALES", "CHART_PIE")
    plataforms = data['Platform'].unique().to_list()
    load_charts_pie_1(transform_data(data, "COUNT_GENRES"), plataforms, "COUNT_GENRES")
    load_charts_pie_2(transform_data(data, "SUM_GLOBAL_SALES"), plataforms, "SUM_GLOBAL_SALES")
    load_charts_bar_1(transform_data(data, "COUNT_GENRES"), plataforms) 
    load_charts_bar_2(transform_data(data, "SUM_GLOBAL_SALES"), plataforms) 
    load_charts_bar_3(transform_data(data, "COUNT_GENRES_YEAR"), plataforms) 
    load_charts_bar_4(transform_data(data, "COUNT_GAMES_YEAR"), plataforms) 

    spark.stop()
    return None


def extract_data(spark):
    df = pspd.read_csv('data/raw/vgsales.csv')
    return df


def transform_data(df, chart_metric):
    # Deletando todas as rows com valor 'N/A'
    df_transformed = df.where(df['Year'] != 'N/A')
    df_transformed = df_transformed.where(df_transformed['Publisher'] != 'N/A').dropna()

    if(chart_metric == "SUM_GLOBAL_SALES"):
        df_transformed = pspd.DataFrame(df_transformed.sort_values(by=['Global_Sales'], ascending=False).groupby(['Platform', 'Genre'])['Global_Sales'].sum())
    elif (chart_metric == "COUNT_GENRES"):
        df_transformed = pspd.DataFrame(df_transformed.sort_values(by=['Genre'], ascending=False).groupby(['Platform', 'Genre'])['Genre'].count())
    elif (chart_metric == "COUNT_GENRES_YEAR"):
        df_transformed = pspd.DataFrame(df_transformed.sort_values(by=['Genre'], ascending=False).groupby(['Platform', 'Genre', 'Year'])['Genre'].count())
    elif (chart_metric == "COUNT_GAMES_YEAR"):
        df_transformed = pspd.DataFrame(df_transformed.sort_values(by=['Genre'], ascending=False).groupby(['Platform', 'Year'])['Genre'].count())

    return df_transformed

def round_value_chart(df, chart_metric):
    if(chart_metric == "SUM_GLOBAL_SALES"):
        df = df.where(df['Global_Sales'] > 1.5).dropna() # Somente vendas acima de 1.5 para não quebrar o gráfico
    elif (chart_metric == "COUNT_GENRES"):
        df = df.where(df['Genre'] >= 2).dropna() # Somente contagem maior ou igual a 2 para não quebrar o gráfico

    return df

def load_charts_pie_1(df, plataforms, chart_metric):
    for platform in plataforms:
        pie_df = df.xs(platform,level=0)
        pie_df = round_value_chart(pie_df, chart_metric) #Arredonda registros do gráfico retirando os menores valores
        plot = pie_df.to_pandas().plot.pie(y='Genre', figsize=(10,10), title=("Contagem de jogos por gênero: " + platform), fontsize=16, pctdistance=0.9, autopct='%1.1f%%',legend=False)
        fig = plot.get_figure()
        fig.savefig("../pyspark-project-vgsales/data/charts/"+"Pie_Count_Genre_per_platform_percent_"+platform+".png")

    return None


def load_charts_pie_2(df, plataforms, chart_metric):
    for platform in plataforms:
        pie_df = df.xs(platform,level=0)
        pie_df = round_value_chart(pie_df, chart_metric) #Arredonda registros do gráfico retirando os menores valores
        plot = pie_df.to_pandas().plot.pie(y='Global_Sales', figsize=(10,10), title=("Vendas Globais por gênero: " + platform), fontsize=16, pctdistance=0.9, autopct='%1.1f%%',legend=False)
        fig = plot.get_figure()
        fig.savefig("../pyspark-project-vgsales/data/charts/"+"Pie_Global_Sales_Genre_per_platform_percent_"+platform+".png")

    return None

def load_charts_bar_1(df, plataforms):
    for platform in plataforms:
        pie_df = df.xs(platform,level=0)
        plot = pie_df.to_pandas().plot.pie(y='Genre', figsize=(10,10), title=("Contagem de jogos por gênero: " + platform), fontsize=16, pctdistance=0.9, autopct='%1.1f%%',legend=False)
        fig = plot.get_figure()
        fig.savefig("../pyspark-project-vgsales/data/charts/"+"Bar_Count_Genre_per_platform_percent_"+platform+".png")

    return None

def load_charts_bar_2(df, plataforms):
    for platform in plataforms:
        pie_df = df.xs(platform,level=0)
        plot = pie_df.to_pandas().plot.bar(y='Global_Sales', figsize=(10,10), title=("Vendas Globais por gênero: " + platform), fontsize=16, legend=False)
        fig = plot.get_figure()
        fig.savefig("../pyspark-project-vgsales/data/charts/"+"Bar_Global_Sales_Genre_per_platform"+platform+".png")

    return None

def load_charts_bar_3(df, plataforms):
    for platform in plataforms:
        pie_df = df.xs(platform,level=0)
        pie_df = pie_df.sort_index(0, ascending=True,level=[1,0])        
        plot = pie_df.to_pandas().plot.bar(y='Genre', figsize=(10,10), title=("Contagem de jogos vendidos por ano: " + platform), fontsize=16, legend=False)
        fig = plot.get_figure()
        fig.savefig("../pyspark-project-vgsales/data/charts/"+"Bar_count_Genre_per_year"+platform+".png")

    return None


def load_charts_bar_4(df, plataforms):
    for platform in plataforms:
        pie_df = df.xs(platform,level=0)
        pie_df = pie_df.sort_index(0, ascending=True,level=0)
        plot = pie_df.to_pandas().plot.bar(y='Genre', figsize=(10,10), title=("Contagem de jogos vendidos por ano: " + platform), fontsize=16, legend=False)
        fig = plot.get_figure()
        fig.savefig("../pyspark-project-vgsales/data/charts/"+"Bar_count_Games_per_year"+platform+".png")

    return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()