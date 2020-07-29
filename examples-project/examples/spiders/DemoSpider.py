import scrapy

from src.spider import RabbitMqSpider


class DemoScrapy(RabbitMqSpider):
    rabbitmq_key = 'hello'
    name = "demo_spider"

    def parse(self, response):
        print("Hahaha")
        print(response.url)
