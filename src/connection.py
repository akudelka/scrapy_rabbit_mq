import pika


RABBITMQ_SETTINGS = {
    'user': 'guest',
    'password': 'guest',
    'queue_name': 'scrapy_queue',
    'prefetch_count': 1,
    'connection': {
        'host': "localhost",
        'port': 5672,
    },
}


def from_settings(settings):
    queue_settings = settings.get('RABBITMQ_SETTINGS', RABBITMQ_SETTINGS)

    user = queue_settings.get('user', RABBITMQ_SETTINGS['user'])
    password = queue_settings.get('password', RABBITMQ_SETTINGS['password'])

    credential_params = pika.PlainCredentials(user, password)

    connection_settings = RABBITMQ_SETTINGS['connection']
    for (key, value) in queue_settings.get('connection', RABBITMQ_SETTINGS).items():
        connection_settings['key'] = value

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(credentials=credential_params, **connection_settings))
    channel = connection.channel()

    queue_name = queue_settings.get('queue_name', RABBITMQ_SETTINGS['scrapy_queue'])
    channel.queue_declare(queue=queue_name, durable=True)

    if queue_settings.get('prefetch_count'):
        channel.basic_qos(prefetch_count=queue_settings.get('prefetch_count'))
    return channel
