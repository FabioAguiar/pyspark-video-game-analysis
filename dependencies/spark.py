"""
spark.py
~~~~~~~~
Module containing helper function for use with Apache Spark
"""

import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging

def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    """Inicie uma Spark session, obtenha o logger do Spark e carregue os arquivos de configuração.
    
    Inicie uma Spark session no nó worker e registre a aplicação com o cluster. 
    Observe que apenas o argumento app_name será aplicado quando for chamado 
    por um script enviado para o spark-submit.
    Todos os outros argumentos existem apenas para testar o script de dentro
    um console Python interativo.
    
    Esta função também procura por um arquivo que termina em 'config.json' que
    podem ser enviados com o worker do Spark. Se for encontrado, é aberto,
    o conteúdo analisado (supondo que contenha JSON válido para o Job ETL
    configuração) em um dict oom parâmetros de configuração do Job ETL,
    que são retornados como o último elemento na tupla retornada por
    esta função. Se o arquivo não puder ser encontrado, a tupla de retorno
    contém apenas a Spark session e os objetos do logger do Spark e nenhum
    para configuração.
    
    A função verifica o ambiente envolvente para ver se está sendo
    executado de dentro de uma sessão de console interativo ou de um
    ambiente que tem um conjunto de variáveis ​​de ambiente `DEBUG` (ex.
    configurando `DEBUG=1` como uma variável de ambiente como parte de uma depuração
    configuração dentro de um IDE como Visual Studio Code ou PyCharm.
    Neste cenário, a função usa todos os argumentos de função disponíveis
    para iniciar um driver PySpark a partir do pacote PySpark local em oposição
    para usar os padrões do cluster spark-submit e Spark. Isso também vai
    use importações de módulos locais, em oposição às do arquivo zip
    enviado para o spark por meio do sinalizador --py-files em spark-submit.
    
    :param app_name: Nome do aplicativo Spark.
    :param master: Detalhes da conexão do cluster (o padrão é local[*]).
    :param jar_packages: Lista de nomes de pacotes Spark JAR.
    :param files: Lista de arquivos para enviar ao cluster Spark (master e
        workers).
    :param spark_config: Dict de pares chave-valor de configuração.
    :return: Uma tupla de referências à sessão Spark, logger e
        dict de configuração (somente se disponível).
    """

    # detectar ambiente de execução
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name))
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # adicionar outros parâmetros de configuração
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # criar sessão e recuperar o objeto logger do Spark
    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    # obtenha o arquivo de configuração se enviado ao cluster com --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, 