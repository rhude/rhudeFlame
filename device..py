import argparse
import datetime
import json
import os
import ssl
import time
import jwt
import paho.mqtt.client as mqtt
import logging
from config import Config

logging.basicConfig(level=logging.DEBUG)
config = Config()

if config.device_type == 'strand':
    from strand import Strand

elif config.device_type == 'cam':
    from cam import Cam

elif config.device_type == 'flame':
    from flame import Flame


PUBLISH_INTERVAL = config.publish_interval  # Seconds
JWT_TTL = config.jwt_ttl # Seconds


def create_jwt(project_id, private_key_file, algorithm):
    """Create a JWT (https://jwt.io) to establish an MQTT connection."""
    token = {
        'iat': datetime.datetime.utcnow(),
        'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=JWT_TTL),
        'aud': project_id
    }
    with open(private_key_file, 'r') as f:
        private_key = f.read()
    logging.debug(f'Creating JWT using {algorithm} from private key file {private_key_file}, Expiry in {JWT_TTL} seconds')
    return jwt.encode(token, private_key, algorithm=algorithm)


def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return '{}: {}'.format(rc, mqtt.error_string(rc))


class Device(object):
    """Represents the state of a single device."""

    def __init__(self):
        self.led_on = False
        self.connected = False
        self.pattern = None
        self.color_spectrum = None
        self.error = False
        self.errormsg = None
        self.aws_access_key_id = None
        self.aws_secret_access_key = None
        self.aws_default_region = None
        self.aws_kinesis_stream_name = None
        self.device_state = {}
        if config.device_type == 'strand':
            self.strand = Strand()
        elif config.device_type == 'cam':
            self.cam = Cam()
        elif config.device_type == 'flame':
            self.flame = Flame()

    def get_status(self):
        logging.debug('Refreshing device status...')
        if config.device_type == 'flame':
            try:
                self.flame.update()
            except Exception as e:
                logging.error(f"Device state update exception {e}")
            self.device_state = {
                'on': self.flame.on,
                'error': self.flame.error
            }

    def update(self, updated_config):
        logging.debug(f"Updating device config: {updated_config}")
        if config.device_type == 'strand':
            # Check on/off
            if updated_config['on'] is False:
                self.off()
            else:
                try:
                    self.strand.set(updated_config)
                except Exception as e:
                    logging.error(e)
        elif config.device_type == 'cam':
            try:
                self.cam.set(updated_config)
            except Exception as e:
                logging.error(e)
        elif config.device_type == 'flame':
            try:
                self.flame.set(updated_config)
            except Exception as e:
                logging.error(e)

    def off(self):
        logging.debug('Turning all LEDs off...')
        self.strand.off()

    @staticmethod
    def parse_config(config_json):
        updated_config = json.loads(config_json)
        if config.device_type == 'strand':
            led_state = updated_config['on']
            brightness = updated_config['brightness']
            color_spectrum = updated_config['color']['spectrumRGB']
            mode = updated_config['currentModeSettings']['pattern']
            if color_spectrum > 0:
                # Because we can have both mode and color set, a color of 0 indicates mode should be used.
                color_spectrum = color_spectrum
                mode = None
            else:
                mode = mode
                color_spectrum = None
            config_result = {
                'on': led_state,
                'brightness': brightness,
                'color_spectrum': color_spectrum,
                'mode': mode
            }
        elif config.device_type == 'cam':
            stream_duration = updated_config['streamDuration']
            aws_access_key_id = updated_config['AWSAccessKeyID']
            aws_secret_access_key = updated_config['AWSSecretAccessKey']
            aws_default_region = updated_config['AWSDefaultRegion']
            aws_kinesis_stream_name = updated_config['AWSKinesisStreamName']
            logging.debug(f"Stream launch request for {stream_duration} seconds. AWS_ACCESS_KEY: {aws_access_key_id}, KinesisStream: {aws_kinesis_stream_name}")
            config_result = {
                'on': True,
                'streamDuration': stream_duration,
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key,
                'aws_default_region': aws_default_region,
                'aws_kinesis_stream_name': aws_kinesis_stream_name
            }
        elif config.device_type == 'flame':
            flame_state = updated_config['on']
            config_result = {
                'on': flame_state
            }
        logging.debug(f"Device Configuration: {config_result}")
        return config_result


def parse_command_line_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Google IoT project.')
    parser.add_argument(
        '--project_id',
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
        required=False,
        help='GCP cloud project name.')
    parser.add_argument(
        '--registry_id',
        default=os.environ.get("GOOGLE_IOT_REGISTRY"),
        required=False, help='Cloud IoT registry id')
    parser.add_argument(
        '--device_id',
        default=os.environ.get("DEVICE_ID"),
        required=False,
        help='Cloud IoT device id')
    parser.add_argument(
        '--private_key_file', required=False, help='Path to private key file.', default='.keys/device.key')
    parser.add_argument(
        '--algorithm',
        choices=('RS256', 'ES256'),
        required=False,
        help='Which encryption algorithm to use to generate the JWT.',
        default='RS256')
    parser.add_argument(
        '--cloud_region', default='us-central1', help='GCP cloud region')
    parser.add_argument(
        '--ca_certs',
        default='.keys/roots.pem',
        help='CA root certificate. Get from https://pki.google.com/roots.pem')
    parser.add_argument(
        '--num_messages',
        type=int,
        default=100,
        help='Number of messages to publish.')
    parser.add_argument(
        '--mqtt_bridge_hostname',
        default='mqtt.googleapis.com',
        help='MQTT bridge hostname.')
    parser.add_argument(
        '--mqtt_bridge_port', type=int, default=8883, help='MQTT bridge port.')
    parser.add_argument(
        '--message_type', choices=('event', 'state'),
        default='state',
        help=('Indicates whether the message to be published is a '
              'telemetry event or a device state message.'))

    return parser.parse_args()


class MQTT:

    def __init__(self, project_id, cloud_region, registry_id, private_key_file, algorithm,
                 ca_certs, mqtt_bridge_hostname, mqtt_bridge_port, device_id, message_type):
        self.project_id = project_id
        self.cloud_region = cloud_region
        self.registry_id = registry_id
        self.private_key_file = private_key_file
        self.algorithm = algorithm
        self.ca_certs = ca_certs
        self.device_id = device_id
        self.message_type = message_type

        self.mqtt_bridge_hostname = mqtt_bridge_hostname
        self.mqtt_bridge_port = mqtt_bridge_port
        self.mqtt_telemetry_topic = '/devices/{}/{}'.format(self.device_id, self.message_type)

        self._mqttclient = mqtt.Client(
            client_id='projects/{}/locations/{}/registries/{}/devices/{}'.format(
                self.project_id,
                self.cloud_region,
                self.registry_id,
                self.device_id))

        self._mqttclient.on_connect = self.on_connect
        self._mqttclient.on_disconnect = self.on_disconnect
        self._mqttclient.on_publish = self.on_publish
        self._mqttclient.on_subscribe = self.on_subscribe
        self._mqttclient.on_message = self.on_message
        self._mqttclient.on_log = self.on_log

        self._mqttclient.enable_logger()
        self.connected = False

        self.authentication()
        self._mqttclient.tls_set(ca_certs=self.ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

        self.device = Device()
        self.connect()

    def authentication(self):
        self._mqttclient.username_pw_set(
            username='unused',
            password=create_jwt(
                self.project_id,
                self.private_key_file,
                self.algorithm))
        # Get JWT minting time and store here.
        self.jwt_mint_time = datetime.datetime.utcnow()
        logging.debug(f"Created new JWT: {self.jwt_mint_time}")

    def connect(self):
        logging.debug("Connecting to MQTT")
        self._mqttclient.connect(self.mqtt_bridge_hostname, self.mqtt_bridge_port)
        logging.debug("Starting loop...")
        self._mqttclient.loop_start()
        self.subscribe()
        time.sleep(5)
        #logging.debug(f"Connected: {self._mqttclient.is_connected}")
        self.wait_for_connection(5)

    def subscribe(self):
        # This is the topic that the device will publish telemetry events
        # (temperature data) to.
        mqtt_telemetry_topic = '/devices/{}/events'.format(self.device_id)

        # This is the topic that the device will receive configuration updates on.
        mqtt_config_topic = '/devices/{}/config'.format(self.device_id)

        # Subscribe to the config topic.
        self._mqttclient.subscribe(mqtt_config_topic, qos=1)

    def publish(self, payload):
        payload = json.dumps(payload)
        logging.debug(f'Publishing payload {payload}')
        self._mqttclient.publish(self.mqtt_telemetry_topic, payload, qos=1)

    def wait_for_connection(self, timeout):
        """Wait for the device to become connected."""
        total_time = 0
        while not self.connected and total_time < timeout:
            time.sleep(5)
            total_time += 1

        if not self.connected:
            raise RuntimeError('Could not connect to MQTT bridge.')

    def on_connect(self, unused_client, unused_userdata, unused_flags, rc):
        """Callback for when a device connects."""
        logging.info(f'Connection Result:{error_str(rc)}')
        self.connected = True

    def on_disconnect(self, unused_client, unused_userdata, rc):
        """Callback for when a device disconnects."""
        logging.info(f'Disconnected:{error_str(rc)}')
        self._mqttclient.loop_stop()
        self.connected = False

    def on_publish(self, unused_client, unused_userdata, unused_mid):
        """Callback when the device receives a PUBACK from the MQTT bridge."""
        logging.debug('Published message acked.')

    def on_subscribe(self, unused_client, unused_userdata, unused_mid,
                     granted_qos):
        """Callback when the device receives a SUBACK from the MQTT bridge."""
        logging.info(f"Subscribed: {granted_qos}")
        if granted_qos[0] == 128:
            logging.error('Subscription failed.')

    def on_message(self, unused_client, unused_userdata, message):
        """Callback when the device receives a message on a subscription."""
        payload = message.payload
        logging.debug(f"Received message {payload} on topic {message.topic} with QoS {str(message.qos)}")

        # The device will receive its latest config when it subscribes to the
        # config topic. If there is no configuration for the device, the device
        # will receive a config with an empty payload.
        if not payload:
            return
        updated_config = self.device.parse_config(payload)
        self.device.update(updated_config)

    def on_log(self, mqttc, obj, level, string):
        logging.debug(f"{string}")


def main():
    args = parse_command_line_args()

    logging.info(f"Google Project: {args.project_id}")
    logging.info(f"Google IoT Registry: {args.registry_id}")
    logging.info(f"Device ID: {args.device_id}")

    mqttclient = MQTT(project_id=args.project_id,
                      cloud_region=args.cloud_region,
                      registry_id=args.registry_id,
                      private_key_file=args.private_key_file,
                      algorithm=args.algorithm,
                      ca_certs=args.ca_certs,
                      mqtt_bridge_hostname=args.mqtt_bridge_hostname,
                      mqtt_bridge_port=args.mqtt_bridge_port,
                      device_id=args.device_id,
                      message_type=args.message_type)

    mqttclient.subscribe()

    while True:
        mqttclient.device.get_status()
        mqttclient.publish(mqttclient.device.device_state)
        jwt_age = (datetime.datetime.utcnow() - mqttclient.jwt_mint_time).total_seconds()
        logging.debug(f"JWT Age: {jwt_age} seconds.")
        if jwt_age > JWT_TTL:
            mqttclient._mqttclient.loop_stop()
            mqttclient.authentication()
            mqttclient._mqttclient.loop_stop()

        time.sleep(PUBLISH_INTERVAL)

    mqttclient.__mqttclient.disconnect()

    logging.debug('Finished loop successfully. Goodbye!')


if __name__ == '__main__':
    main()
