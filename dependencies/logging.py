"""
logging
~~~~~~~
Este módulo contém uma classe que envolve o objeto log4j instanciado
pelo SparkContext ativo, permitindo o log Log4j para uso do PySpark.
"""


class Log4j(object):
    """Classe wrapper para o objeto Log4j JVM.
    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        # obter detalhes da aplicação Spark com os quais deseja prefixar todas as mensagens
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log de erro.
        :param: Mensagem de erro para exibir no log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log de alerta.
        :param: Mensagem de alerta para exibir no log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log de informação.
        :param: Mensagem de informação para wxibir no log
        :return: None
        """
        self.logger.info(message)
        return None