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
from py2neo import Graph
from py2neo import Node
from py2neo import Relationship
from py2neo import authenticate
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
	]

	__queue = [
		r'Pages\/chitiethoidap.aspx.*',
		r'Pages\/hoidap.aspx',
		r'\/.*\.(pdf|doc)',
		r'\/huongdan\/CSDLVanbanphapluat.html',
		r'\/_layouts\/authenticate.aspx.*',
		r'\/Pages\/sitemap.aspx',
		r'http:\/\/vbpl.vn\/TW\/Pages\/vbpqen.aspx',
		r'\/.*vbpq-timkiem\.aspx.*',
		r'\/pages\/dieu-uoc-home.aspx'

		
	]

	authenticate("localhost:7474", "neo4j", "123456")
	graph = Graph()
	for record in graph.cypher.execute("match (a:LegalNormativeDocument) return a.id as id"):
		__queue.append("http://vbpl.vn/noidung/news/Lists/ThongBao/View_Detail.aspx?ItemID={}".format(record.id))
	# client = MongoClient(settings.get('MONGODB_URI'))
	# db = client[settings.get('MONGODB_DATABASE')]

	# cursor = db[settings.get("CRAWLER_COLLECTION")].find({}, {"url": 1})
	# for i in cursor:
	# 	if 'url' in i:
	# 		__queue.append(i['url'])

	rules = [
		# Extract news
		Rule(
			LinkExtractor(
			allow=('\/noidung\/news\/Lists\/.*'),
			deny=__queue,
			restrict_xpaths=[]
			)
		),

		# Extract TW and location
		Rule(
			LinkExtractor(
				allow=(), 
				deny=__queue,
				restrict_xpaths=[
					"//div[@class='box-toplink']"
				])
			),

		# Extract earch pages (TW+ location)
		Rule(
			LinkExtractor(
			allow = (
				'\/.*\/Pages\/vanban.aspx\?fromyear=\d{2}\/\d{2}\/\d{4}.*toyear=\d{2}\/\d{2}\/\d{4}',
				'\/TW\/Pages\/vanban.aspx\?cqbh=\d+',
				'\/TW\/Pages\/vanban.aspx\?idLoaiVanBan=\d+'
				),
			deny=__queue
			)
		),

		Rule(
			LinkExtractor(
				allow=(
					'\/.*\/Pages\/vanban.aspx\?fromyear=\d{2}\/\d{2}\/\d{4}.*toyear=\d{2}\/\d{2}\/\d{4}.*Page=\d+',
					'\/TW\/Pages\/vanban.aspx\?cqbh=\d+.*Page=\d+',
					'\/TW\/Pages\/vanban.aspx\?idLoaiVanBan=\d+.*Page=\d+'
					),
				deny=__queue,
				restrict_xpaths=[]
				)
			),

	    Rule(
	    	LinkExtractor(
	    		allow=(
	    		r'\/TW\/Pages\/vbpq-toanvan.aspx\?ItemID=\d+',
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

				# related_doc_url_full = response.urljoin(related_doc_url)

				# Extract title
				related_document_item['title'] = self.extract(row_doc, "text()")
				related_document_item['relating_type'] = relating_type

				related_document_list.append(related_document_item)

				# yield scrapy.Request(related_doc_url_full, callback=self.parse_fulltext_data)

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

		yield scrapy.Request(response.url.replace('lichsu', 'vanbanlienquan'),
			meta = {'vbpl_item': vbpl_item},
			callback=self.parse_related_document)



	def parse_attribute_data(self, response):
		SO_KI_HIEU = 'S\xe1\xbb\x91 k\xc3\xbd hi\xe1\xbb\x87u'
		NGAY_BAN_HANH = 'Ng\xc3\xa0y ban h\xc3\xa0nh'
		LOAI_VAN_BAN = 'Lo\xe1\xba\xa1i v\xc4\x83n b\xe1\xba\xa3n'
		NGAY_CO_HIEU_LUC = 'Ng\xc3\xa0y c\xc3\xb3 hi\xe1\xbb\x87u l\xe1\xbb\xb1c'
		NGUON_THU_THAP = 'Ngu\xe1\xbb\x93n thu th\xe1\xba\xadp'
		NGAY_DANG_CONG_BAO = 'Ng\xc3\xa0y \xc4\x91\xc4\x83ng c\xc3\xb4ng b\xc3\xa1o'
		NGANH = 'Ng\xc3\xa0nh'
		LINH_VUC = 'L\xc4\xa9nh v\xe1\xbb\xb1c'
		CO_QUAN_BAN_HANH = 'C\xc6\xa1 quan ban h\xc3\xa0nh/ Ch\xe1\xbb\xa9c danh / Ng\xc6\xb0\xe1\xbb\x9di k\xc3\xbd'
		PHAM_VI = 'Ph\xe1\xba\xa1m vi'

		vbpl_item = response.meta['vbpl_item']

		vbpl_item['document']['effect'] = self.extract(response, "//div[@class='vbInfo']/ul/li[1]/text()", '')

		vbpl_item['document']['title'] = self.extract(response, "//div[@class='vbProperties']/table/tbody/tr[1]/td[@class='title']//text()", '')



		rows = response.xpath("//div[@class='vbProperties']/table/tbody/tr/td")

		for row in rows:
			label = self.extract(row,"text()", '').encode('UTF-8')
			if label == SO_KI_HIEU: vbpl_item['document']['official_number'] = self.extract(row, "following-sibling::td[1]//text()")
			elif label == NGAY_BAN_HANH: vbpl_item['document']['issued_date'] = self.extract(row, "following-sibling::td[1]//text()")
			elif label == LOAI_VAN_BAN: vbpl_item['document']['legislation_type'] = self.extract(row, "following-sibling::td[1]//text()")
			elif label == NGAY_CO_HIEU_LUC: vbpl_item['document']['effective_date'] = self.extract(row, "following-sibling::td[1]//text()")
			elif label == NGUON_THU_THAP: vbpl_item['document']['source'] = self.extract(row, "following-sibling::td[1]//text()")
			elif label == NGAY_DANG_CONG_BAO: vbpl_item['document']['gazette_date'] = self.extract(row, "following-sibling::td[1]//text()")
			elif label == NGANH: vbpl_item['document']['department'] = self.extract(row, "following-sibling::td[1]//text()")
			elif label == LINH_VUC: vbpl_item['document']['field'] = self.extract(row, "following-sibling::td[1]//text()")
			elif label == CO_QUAN_BAN_HANH: 
				vbpl_item['document']['issuing_office'] = self.extract(row, "following-sibling::td[1]//text()")
				vbpl_item['document']['signer_title'] = self.extract(row, "following-sibling::td[2]//text()")
				vbpl_item['document']['signer_name'] = self.extract(row, "following-sibling::td[3]//text()")
			elif label == PHAM_VI: vbpl_item['document']['effective_area'] = self.extract(row, "following-sibling::td[1]//text()")


		# parse history
		yield scrapy.Request(response.url.replace('thuoctinh', 'lichsu'),
			meta = {'vbpl_item': vbpl_item},
			callback=self.parse_history_data)

	
	def parse_fulltext_data(self, response):
		se = re.search('\d+', response.url)

		if se is not None:
			document_id = se.group()

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
				yield scrapy.Request(response.url.replace('toanvan', 'thuoctinh'),
				 meta = {'vbpl_item': vbpl_item},
				 callback=self.parse_attribute_data)


			# # parse history
			# yield scrapy.Request(url.replace('toanvan', 'lichsu'),
			# 	meta = {'document_id': document_id},
			# 	callback=self.parse_history_data)

			# parse related document
			# yield scrapy.Request(url.replace('toanvan', 'vanbanlienquan'),
			# 	meta = {'document_id': document_id},
			# 	callback=self.parse_related_document)























		








