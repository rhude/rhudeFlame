import logging
import requests
import json

from config import Config

config = Config()
logging.basicConfig(level=logging.DEBUG)


class Flame:
    def __init__(self):
        self.on = False
        self.error = False
        try:
            self.update()
        except Exception as e:
            logging.error(f"Exception getting state from Neelay: {config.flame_neelay_url} {e}")
            self.error = True

    def set(self, updated_config):
        logging.debug(f"Flame received new configuration: {updated_config}")
        response = requests.post(config.flame_neelay_url, updated_config)
        if response.status_code == 200:
            self.on = updated_config['on']
            self.error = False
            return
        else:
            raise Exception(f"Neelay returned {response.status_code}")
            self.error = True

    def update(self):
        logging.debug(f"Getting flame state {config.flame_neelay_url}")
        try:
            response_json = requests.get(config.flame_neelay_url)
        except Exception as e:
            logging.debug(f"Error getting flame state {config.flame_neelay_url} {e}")
            self.error = True
            raise Exception("Error getting flame state.")
        response = json.loads(response_json)
        self.on = response['on']

