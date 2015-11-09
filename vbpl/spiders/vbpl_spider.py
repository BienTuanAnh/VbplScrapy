# -*- coding: utf-8 -*-

import scrapy
from scrapy.spiders import CrawlSpider , Rule
from scrapy.selector import Selector
from scrapy.linkextractors import LinkExtractor
from scrapy.http import Request
# from bs4 import BeautifulSoup, Comment
from scrapy.conf import settings
#from selenium import webdriver
import time
import datetime
from ..items import DocumentItem
from ..items import HistoryItem
from ..items import RelatedDocumentItem
from ..items import VbplItem
#from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import re
import itertools
# from pymongo import MongoClient
class TrangVangVietNamSpider(CrawlSpider):
	name = "vbpl"
	allowed_domains = [
		"http://vbpl.vn/",
		"vbpl.vn"

	]

	start_urls = [
		"http://vbpl.vn/Pages/portal.aspx"
		# 'http://vbpl.vn/Pages/portal.aspx'
		# "http://vbpl.vn/TW/Pages/vbpq-vanbanlienquan.aspx?ItemID=92637",
		# "http://vbpl.vn/TW/Pages/vbpq-toanvan.aspx?ItemID=32507",
		# "http://vbpl.vn/TW/Pages/vbpq-toanvan.aspx?ItemID=23776",
		# "http://vbpl.vn/TW/Pages/vbpq-toanvan.aspx?ItemID=13716",
		# "http://vbpl.vn/TW/Pages/vbpq-toanvan.aspx?ItemID=13716"
	]

	__queue = [
		r'Pages\/chitiethoidap.aspx.*',
		r'Pages\/hoidap.aspx',
		r'\/.*\.(pdf|doc)',
		r'\/huongdan\/CSDLVanbanphapluat.html',
		r'\/_layouts\/authenticate.aspx.*',
		r'\/Pages\/sitemap.aspx',
		r'http:\/\/vbpl.vn\/TW\/Pages\/vbpqen.aspx',
		r'\/.*vbpq-timkiem\.aspx.*'

		
	]
	# client = MongoClient(settings.get('MONGODB_URI'))
	# db = client[settings.get('MONGODB_DATABASE')]

	# cursor = db[settings.get("CRAWLER_COLLECTION")].find({}, {"url": 1})
	# for i in cursor:
	# 	if 'url' in i:
	# 		__queue.append(i['url'])

	rules = [
		# Extract TW and location
		Rule(
			LinkExtractor(
				allow=(), 
				deny=__queue,
				restrict_xpaths=[
					"//div[@class='box-toplink']"
				])
			),

		Rule(LinkExtractor(
			allow=('\/noidung\/news\/Lists\/.*'),
			deny=__queue,
			restrict_xpaths=[]
			)
		),

		# Extract earch pages (TW+ location)
		Rule(
			LinkExtractor(
				allow=(
					'\/.*\/Pages\/vanban.aspx\?fromyear=\d{2}\/\d{2}\/\d{4}.*toyear=\d{2}\/\d{2}\/\d{4}.*',
					'\/TW\/Pages\/vanban.aspx\?cqbh=\d+.*',
					'\/TW\/Pages\/vanban.aspx\?idLoaiVanBan=\d+.*'
					),
				deny=__queue,
				restrict_xpaths=[]
				)
			),

	    Rule(
	    	LinkExtractor(
	    		allow=(
	    		r'\/TW\/Pages\/vbpq-toanvan.aspx.*',
	    	), 
	    	deny=__queue,
	    	restrict_xpaths=[
	    	]), 
	    	callback='parse_fulltext_data'
	    	)
	    ]

	history_count = itertools.count()
	related_document_count = itertools.count()

	#
	document_crawled = []

	def extract(self,sel,xpath,split = ' '):
		try:
			data = sel.xpath(xpath).extract()
			text = filter(lambda element: element.strip(),map(lambda element: element.strip(), data))
			return split.join(text)
			# return re.sub(r"\s+", "", ''.join(text).strip(), flags=re.UNICODE)
		except Exception, e:
			raise Exception("Invalid XPath: %s" % e)

	def parse_related_document(self, response):

		vbpl_item = response.meta['vbpl_item']
		
		rows_labels = response.xpath("//div[@class='content']/table/tbody/tr")

		related_document_list = list()

		for row_label in rows_labels:
			# Extract relating tpye
			relating_type = self.extract(row_label, "td[@class='label']//text()", '')

			rows_docs = row_label.xpath("td/ul[@class='listVB']/li/div[@class='item']/p[@class='title']/a")

			for row_doc in rows_docs:
				
				related_document_item = RelatedDocumentItem()
				related_document_item['document_id'] = vbpl_item['document']['document_id']

				# Extract related document url
				related_doc_url = self.extract(row_doc, "@href", '')
				related_document_item['related_document_id'] =  re.search("\d+", related_doc_url).group()

				related_doc_url_full = response.urljoin(related_doc_url)

				# Extract title
				related_document_item['title'] = self.extract(row_doc, "text()")
				related_document_item['relating_type'] = relating_type

				related_document_list.append(related_document_item)

				yield scrapy.Request(related_doc_url_full, callback=self.parse_fulltext_data)

		# Add to vbpl item
		vbpl_item['related_documents'] = related_document_list

		yield vbpl_item


	def parse_history_data(self, response):
		vbpl_item = response.meta['vbpl_item']

		rows = response.xpath("//tr[@class='odd']")

		history_list = list()

		for row in rows:
			history_item = HistoryItem()
			history_item['history_id'] = self.history_count.next()
			history_item['document_id'] = vbpl_item['document']['document_id']

			history_item['title'] = self.extract(row, "//tr/td[@class='title']//text()",'')

			history_item['date'] = self.extract(row, "td[1]//text()", '')
			history_item['status'] = self.extract(row, "td[2]//text()", '')
			history_item['original_document'] = self.extract(row, "td[3]//text()", '')
			history_item['ineffective_part'] = self.extract(row, "td[4]//text()", '')

			# Add to vbpl item
			history_list.append(history_item)
		
		vbpl_item['histories'] = history_list

		scrapy.Request(response.url.replace('lichsu', 'vanbanlienquan'),
			meta = {'vbpl_item': vbpl_item},
			callback=self.parse_related_document)

		return



	def parse_attribute_data(self, response):
		vbpl_item = response.meta['vbpl_item']

		vbpl_item['document']['effect'] = self.extract(response, "//div[@class='vbInfo']/ul/li[1]/text()", '')

		vbpl_item['document']['title'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[1]/td[@class='title']//text()", '')

		vbpl_item['document']['official_number'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[2]/td[1]/following-sibling::td[1]//text()", '')
		vbpl_item['document']['issued_date'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[2]/td[3]/following-sibling::td[1]//text()", '')

		vbpl_item['document']['legislation_type'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[3]/td[1]/following-sibling::td[1]//text()", '')
		vbpl_item['document']['effective_date'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[3]/td[3]/following-sibling::td[1]//text()", '')

		vbpl_item['document']['source'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[4]/td[1]/following-sibling::td[1]//text()", '')
		vbpl_item['document']['gazette_date'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[4]/td[3]/following-sibling::td[1]//text()", '')

		vbpl_item['document']['department'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[5]/td[1]/following-sibling::td[1]//text()", '')
		vbpl_item['document']['field'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[5]/td[3]/following-sibling::td[1]//text()", '')		

		vbpl_item['document']['issuing_office'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[6]/td[1]/following-sibling::td[1]//text()", '')
		vbpl_item['document']['chairman'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[6]/td[3]/following-sibling::td[1]//text()", '')

		vbpl_item['document']['effective_area'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[7]/td[1]/following-sibling::td[1]//text()", '')


		# parse history
		scrapy.Request(response.url.replace('thuoctinh', 'lichsu'),
			meta = {'vbpl_item': vbpl_item},
			callback=self.parse_history_data)

		return


	
	def parse_fulltext_data(self, response):

		document_id = re.search('\d+', response.url).group()

		if document_id not in self.document_crawled:

			self.document_crawled.append(document_id)

			# Init vbpl item
			vbpl_item = VbplItem()

			vbpl_item['document'] = DocumentItem()

			# Get document id
			vbpl_item['document']['document_id'] = document_id

			# Get document content
			vbpl_item['document']['content'] = self.extract(response, "//div[@id='toanvancontent']//text()")

			# parse attribute
			scrapy.Request(response.url.replace('toanvan', 'thuoctinh'),
			 meta = {'vbpl_item': vbpl_item},
			 callback=self.parse_attribute_data)

			return

			# # parse history
			# yield scrapy.Request(url.replace('toanvan', 'lichsu'),
			# 	meta = {'document_id': document_id},
			# 	callback=self.parse_history_data)

			# parse related document
			# yield scrapy.Request(url.replace('toanvan', 'vanbanlienquan'),
			# 	meta = {'document_id': document_id},
			# 	callback=self.parse_related_document)























		








