from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider, CrawlSpider
import connection


class RabbitMQUtilSpider(object):

    def __init__(self):
        self.server = None
        self.connection = None

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
        self.server, self.connection = connection.from_settings(settings=settings, queue_name=self.rabbitmq_key)
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

        # This will be called just after item has been scraped, reason is to call this not to stop crawler
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)

    def next_request(self):
        method_frame, header_frame, url = self.server.basic_get(queue=self.rabbitmq_key)

        if url:
            url = str(url, 'utf-8')
            yield self.make_requests_from_url(url)
            self.server.basic_ack(method_frame.delivery_tag)

    def schedule_next_request(self):
        for req in self.next_request():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """
        Schedules a requests if available, otherwise waits, If there is no request available in the queue
        so it will not close the spider
        :return:
        """
        self.schedule_next_request()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """ After item has been scrapped, avoid waiting for scheduler to schedule next request
        :param args:
        :param kwargs:
        :return:
        """
        self.schedule_next_request()

    def closed(self, reason):
        if self.server:
            self.server.close()

        if self.connection:
            self.connection.close()
        self.logger.info('Closing spider name: %s, reason: %s', getattr(self, 'name', None), reason)


class RabbitMqSpider(RabbitMQUtilSpider, Spider):
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        obj = super(RabbitMqSpider, cls).from_crawler(crawler, *args, **kwargs)
        obj.setup_queue(crawler)
        return obj
