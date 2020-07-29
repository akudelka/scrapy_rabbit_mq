import pika
import copy


RABBITMQ_SETTINGS = {
    'user': 'guest',
    'password': 'guest',
    'prefetch_count': 1,
    'connection': {
        'host': "localhost",
        'port': 5672,
    },
}


def from_settings(settings, queue_name):
    queue_settings = settings.get('RABBITMQ_SETTINGS', RABBITMQ_SETTINGS)

    user = queue_settings.get('user', RABBITMQ_SETTINGS['user'])
    password = queue_settings.get('password', RABBITMQ_SETTINGS['password'])

    credential_params = pika.PlainCredentials(user, password)

    connection_settings = copy.deepcopy(RABBITMQ_SETTINGS['connection'])
    for (key, value) in queue_settings.get('connection', RABBITMQ_SETTINGS['connection']).items():
        connection_settings[key] = value

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(credentials=credential_params, **connection_settings))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=True)

    if queue_settings.get('prefetch_count'):
        channel.basic_qos(prefetch_count=queue_settings.get('prefetch_count'))
    return channel, connection
