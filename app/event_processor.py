from app.models.events import AppLoadEvent, RegisteredEvent


class EventProcessor:
    def process(self, events_df):
        registered_df = RegisteredEvent().transform(events_df)
        app_load_df = AppLoadEvent().transform(events_df)

        return registered_df, app_load_df
