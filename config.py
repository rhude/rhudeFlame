from os import getenv


class Config:
    def __init__(self):
        self.device_type = getenv('DEVICE_TYPE', "strand")
        self.num_pixels = int(getenv('LED_NUM_PIXELS', 10))
        self.pixel_pin = getenv('LED_BOARD_PIN', 18)
        self.brightness = getenv('LED_BRIGHTNESS', 1)
        self.publish_interval = getenv('PUBLISH_INTERVAL', 30)
        self.jwt_ttl = getenv('JWT_TTL', 82800)
        self.flame_neelay_url = getenv('FLAME_NEELAY_URL', 'http://localhost:5000/state')
