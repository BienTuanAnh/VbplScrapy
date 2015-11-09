# -*- coding: utf-8 -*-

from scrapy.http import HtmlResponse, Response
import time
from scrapy.conf import settings
import re
import random


from scrapy.conf import settings


class ProxyMiddleware(object):
    def process_request(self, request, spider):
        request.meta['proxy'] = settings.get('HTTP_PROXY')

class RandomUserAgentMiddleware(object):
    def process_request(self, request,spider):
        userAgent = random.choice(settings.get('USER_AGENT_LIST'))
        if userAgent:
            request.headers.setdefault("User-Agent", userAgent)

