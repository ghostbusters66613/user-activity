import json
from typing import Dict


class AppConfig:
    """Provides config for the app
    """
    CFG_PATH = '/app/app/config/config.json'

    def __init__(self):
        with open(self.CFG_PATH) as f:
            self._config = json.load(f)

    def get_parser_cfg(self) -> Dict:
        """Get parser config

        Returns:
            dict

        """
        return self._config['parser']

    def get_statistics_cfg(self) -> Dict:
        """Get statistics config

        Returns:
            dict
        """
        return self._config['statistics']
