import sys
import os
import io
import scrapy_rabbit_mq

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


def read_file(filename):
    with io.open(filename) as fp:
        return fp.read().strip()


def read_requirements(filename):
    return [line.strip() for line in read_file(filename).splitlines()
            if not line.startswith('#')]


setup(
    name='scrapy_rabbit_mq',
    author='Abrar Khan',
    description='RabbitMQ Publisher for Scrapy and feed message from Rabbit MQ',
    version='0.1.1',
    author_email='abrar.nitk@gmail.com',
    license='MIT',
    url='https://github.com/JoeyRead/scrapy_rabbit_mq',
    install_requires=read_requirements('requirements.txt'),
    keywords=['scrapy_rabbit_mq'],
    packages=packages,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.8',
    ]
)
