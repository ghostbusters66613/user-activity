import click

from app.config.config import AppConfig
from app.models.events import RegisteredEvent, AppLoadEvent
from app.event_processor import EventProcessor
from app.data_storage import DataStorage
from app.metrics_builder import DataStatistics
from app.dependencies.spark import start_spark


@click.group(name='cli')
def cli():
    """Commands related to compiling"""
    pass


@cli.command(name='statistics', help='run parsing')
def statistics():
    spark, log = start_spark(app_name='user_activity')

    config = AppConfig().get_statistics_cfg()

    registered_path = f'{config["input_data_path"]}/' \
                      f'{RegisteredEvent().event_type}/*'
    app_loaded_path = f'{config["input_data_path"]}/' \
                      f'{AppLoadEvent().event_type}/*'

    loader = DataStorage(spark)
    registered_df = loader.load_parquet(registered_path)
    app_load_df = loader.load_parquet(app_loaded_path)

    statistics = DataStatistics()
    metric = statistics.\
        get_active_user_after_registration(registered_events_df=registered_df,
                                           app_loaded_events_df=app_load_df)

    log.info('job is finished')
    log.info(metric.percentage_active_users)
    spark.stop()

    click.echo(f'Metric: {metric.percentage_active_users}%')


@cli.command(name='parse', help='statistics on the data')
def parse():
    spark, log = start_spark(app_name='user_activity')

    config = AppConfig().get_parser_cfg()
    # log that main ETL job is starting
    log.warn('job is up-and-running')

    data_storage = DataStorage(spark)
    data = data_storage.load_json(config['input_data_path'])

    registered_df, app_load_df = EventProcessor().process(data)

    registered_path = f'{config["output_path"]}/' \
                      f'{RegisteredEvent().event_type}'
    app_loaded_path = f'{config["output_path"]}/' \
                      f'{AppLoadEvent().event_type}'

    data_storage.save_to_parquet(registered_df, registered_path)
    data_storage.save_to_parquet(app_load_df, app_loaded_path)

    log.info('Parsing has finished')
    spark.stop()
    click.echo('Parsing has been finished')


if __name__ == '__main__':
    cli()
