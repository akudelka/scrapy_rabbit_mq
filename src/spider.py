from abc import ABC

from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider, CrawlSpider
import connection


class RabbitMQUtilSpider(object):

    def __init__(self):
        self.server = None

    def start_requests(self):
        return self.next_request()

    def setup_queue(self, crawler=None):
        self.logger.info('setting up with the queue')
        if self.server is not None:
            return

        if self.crawler is None:
            raise ValueError("Crawler is required")

        if not self.rabbitmq_key:
            self.rabbitmq_key = '{}:start_urls'.format(self.name)

        settings = crawler.settings
        self.server = connection.from_settings(settings=settings, queue_name=self.rabbitmq_key)
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def next_request(self):
        self.logger.info('reading url from queue')
        method_frame, header_frame, url = self.server.basic_get(queue=self.rabbitmq_key)

        if url:
            url = str(url, 'utf-8')
            yield self.make_requests_from_url(url)
            self.server.basic_ack(method_frame.delivery_tag)
            self.logger.debug('Request completed')

    def schedule_next_request(self):
        for req in self.next_request():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        self.logger.info('spider_idle')
        self.schedule_next_request()
        self.logger.info('spider_idle_called_next_request')
        raise DontCloseSpider


class RabbitMqSpider(RabbitMQUtilSpider, Spider):
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        obj = super(RabbitMqSpider, cls).from_crawler(crawler, *args, **kwargs)
        obj.setup_queue(crawler)
        return obj
