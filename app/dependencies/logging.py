class Log4j(object):
    """Wrapper class for Log4j JVM object.
    """

    def __init__(self, spark, level='ERROR'):
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        spark.sparkContext.setLogLevel(level)

        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log an error.
        """
        self.logger.error(message)

    def warn(self, message):
        """Log an warning.
        """
        self.logger.warn(message)

    def info(self, message):
        """Log information.
        """
        self.logger.info(message)
