# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html



# import pymongo
# from pymongo import MongoClient
# from scrapy.conf import settings
# from scrapy import log
# from middleware.sqlite4lsmmiddlewares import LSMEngine
# class VbplPipeline(object):
#     def __init__(self):
#         connection = MongoClient(settings.get('MONGODB_URI'))
#         db = connection[settings['MONGODB_DATABASE']]
#         # db.authenticate(settings['MONGODB_USERNAME'], settings['MONGODB_PASSWORD'])
#         self.collection = db[settings['CRAWLER_COLLECTION']]

	# def process_item(self, item, spider):
#     	data = dict(item)
#     	if data['url'] not in LSMEngine.db:
#     		LSMEngine.db['url'] = '1'
#     		self.collection.insert(data)
#         return item

from py2neo import Graph
from py2neo import Node
from py2neo import Relationship
from py2neo import authenticate

class VbplPipeline(object):
	def __init__(self):
		authenticate("localhost:7474", "neo4j", "123456")
		self.graph = Graph()
	def process_item(self, item, spider):
		document = item['document']
		histories = item['histories']
		related_documents = item['related_documents']

		print("Putting LegalNormativeDocument to Neo4j: {}".format(document['document_id']))

		document_node = self.graph.merge_one("LegalNormativeDocument", "id", document['document_id'])
		document_node.properties['content'] = document['content']
		document_node.properties['title'] = document['title']
		document_node.properties['official_number'] = document['official_number']
		document_node.properties['legislation_type'] = document['legislation_type']
		document_node.properties['source'] = document['source']
		document_node.properties['department'] = document['department']
		document_node.properties['issuing_office'] = document['issuing_office']
		document_node.properties['effective_area'] = document['effective_area']
		document_node.properties['effective_date'] = document['effective_date']
		document_node.properties['gazette_date'] = document['gazette_date']
		document_node.properties['field'] = document['field']
		document_node.properties['chairman'] = document['chairman']
		document_node.push()

		for history in histories:
			history_node = self.graph.merge_one("History", "id", history['history_id'])
			# history_node.properties['document_id'] = history['document_id']
			history_node.properties['title'] = history['title']
			history_node.properties['date'] = history['date']
			history_node.properties['status'] = history['status']
			history_node.properties['original_document'] = history['original_document']
			history_node.properties['ineffective_part'] = history['ineffective_part']
			history_node.push()

			# Add 'HAS' relationship
			self.graph.create_unique(Relationship(document_node, "HAS", history_node))

		for related_document in related_documents:
			# related_document_node.properties['document_id'] = related_document['document_id']
			related_document_node = self.graph.merge_one("RelatedDocument", "id", related_document['related_document_id'])
			related_document_node.properties['title'] = related_document['title']
			related_document_node.properties['relating_type'] = related_document['relating_type']
			related_document_node.push()

			# Add "HAS" relationship
			self.graph.create_unique(Relationship(document_node, "HAS", related_document_node))

		return item

