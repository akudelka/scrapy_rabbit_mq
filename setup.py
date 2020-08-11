import sys
import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

packages = [
    'scrapy_rabbit_mq'
]

requirements = [
    "Scrapy==2.2.0",
    "pika==1.1.0"
]


setup(
    name='scrapy_rabbit_mq',
    author='Abrar Khan',
    description='RabbitMQ Publisher for Scrapy and feed message from Rabbit MQ',
    version='0.1.2',
    author_email='abrar.nitk@gmail.com',
    license='MIT',
    url='https://github.com/JoeyRead/scrapy_rabbit_mq',
    install_requires=requirements,
    keywords=['scrapy_rabbit_mq'],
    packages=packages,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.8',
    ]
)
