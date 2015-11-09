# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class VbplItem(scrapy.Item):
    document = scrapy.Field()
    histories = scrapy.Field()
    related_documents = scrapy.Field()


class DocumentItem(scrapy.Item):
    document_id = scrapy.Field()
    content = scrapy.Field()

    title = scrapy.Field()

    official_number = scrapy.Field()
    legislation_type = scrapy.Field()
    source = scrapy.Field()
    department = scrapy.Field()
    issuing_office = scrapy.Field()
    effective_area = scrapy.Field()
    issued_date = scrapy.Field()
    effective_date = scrapy.Field()
    effect = scrapy.Field()
    gazette_date = scrapy.Field()
    field = scrapy.Field()
    chairman = scrapy.Field()

class HistoryItem(scrapy.Item):
 	history_id = scrapy.Field()
 	document_id = scrapy.Field()
 	title = scrapy.Field()
 	date = scrapy.Field()
 	status = scrapy.Field()
 	original_document = scrapy.Field()
 	ineffective_part = scrapy.Field()

class RelatedDocumentItem(scrapy.Item):
 	related_document_id = scrapy.Field()
 	document_id = scrapy.Field()
 	title = scrapy.Field()
 	relating_type = scrapy.Field()





    
