# -*- coding: utf-8 -*-

'''
    database interface for storage and cache purposes
'''

import pandas as pd

__author__ = 'arka'

__license__ = "MIT"
__version__ = "1.1.0"
__maintainer__ = "Arkaprava Ghosh"
__email__ = "arkaprava.mail@gmail.com"
__status__ = "Development"

import sys
import pymongo

class DBManager:
    __PORT__ = 27017
    __SCHEMA__ = {
        'bsonType': 'object',
        'additionalProperties': True,
        'required': ['name', 'id', 'prices'],
        'properties': {
            'id': {
                'bsonType': 'string'
            },
            'name': {
                'bsonType': 'string'
            },
            'prices': {
                'bsonType': 'array',
                'items': {
                    'bsonType': 'object',
                    'properties': {
                        'price': {
                            'bsonType': 'int'
                        },
                        'bookingDate': {
                            'bsonType': 'string'
                        },
                        'snapshotDate': {
                            'bsonType': 'string'
                        }
                    }
                }
            },
            'coordinates': {
                'bsonType': 'object',
                'properties': {
                    'lat': {
                        'bsonType': 'double'
                    },
                    'lng': {
                        'bsonType': 'double'
                    }
                }
            },
            'reviews': {
                'bsonType': 'array',
                'items': {
                    'bsonType': 'object',
                    'properties': {
                        'id': {
                            'bsonType': 'string'
                        },
                        'metadata': {
                            'bsonType': 'object',
                            'properties': {
                                'title': {
                                    'bsonType': 'string'
                                },
                                'upvote': {
                                    'bsonType': 'int'
                                },
                                'reviewText': {
                                    'bsonType': 'string'
                                }
                            }
                        },
                        'images': {
                            'bsonType': 'array',
                            'items': {
                                'bsonType': 'string'
                            }
                        }
                    }
                }
            }
        }      
    }

    def __init__(self, dbName, tableName, logging):
        self.dbName = dbName
        self.tableName = tableName
        self.logging = logging
        
        try:
            client = pymongo.MongoClient("localhost", self.__PORT__)
            self.db = client[self.dbName]
            self.__create_collection(collection_name=self.tableName)
            self.collection = self.db[self.tableName]
            self.collection.create_index('id', unique=True)
        except:
            sys.exit()

    def __create_collection(self, collection_name='prices'):
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(
                collection_name,
                validator = {
                    '$jsonSchema': self.__SCHEMA__
                }
            )

    def query_price(self, hotel_id, bookingDate, snapshotDate):
        '''
            checks if price for hotel is already cached for given
            booking date and snspshot date
            price for same booking date may vary if taken from different
            snapshot dates
        '''
        try:
            cursor = self.collection.find({'id': hotel_id, 'prices.bookingDate': bookingDate, 'prices.snapshotDate': snapshotDate})
            docs = list(cursor)

            if len(docs) > 0:
                return docs[0]
            else:
                return None
        except:
            self.logging.error("error querying for document(/s).", exc_info=True)
            return None

    def __insert_doc(self, doc):
        '''
            insert single document
            decide whether:
                - new document
                - document exists, price update
                - document exists, review update
                - document exists, both price and review update
        '''
        try:
            cursor = self.collection.find({'id': doc['id']})
            docs = list(cursor)
        except:
            self.logging.error(f"error querying for existing document(/s) for hotel id {doc['id']}", exc_info=True)
        else:
            if len(docs) == 0:
                # no document exists -> insert new document
                try:
                    self.collection.insert_one(doc)
                except:
                    self.logging.warning(f"error inserting new document for hotel id {doc['id']}", exc_info=True)
            else:
                # doc exists -> update document
                old_doc = docs[0]

                old_prices = old_doc.prices
                new_prices = doc.prices

                prices_df = pd.DataFrame(old_prices + new_prices)
                prices_df.drop_duplicates(subset=['bookingDate', 'snapshotDate'], inplace=True)

                prices = prices_df.to_dict('records')

                old_reviews = old_doc.reviews
                new_reviews = doc.reviews

                reviews_df = pd.DataFrame(old_reviews + new_reviews)
                reviews_df.drop_duplicates(subset=['id'], inplace=True)

                reviews = reviews_df.to_dict('records')

                try:
                    self.collection.update_one(
                        {'id': doc['id']}, 
                        {
                            "$set": {
                                "prices": prices,
                                "reviews": reviews 
                            }
                        }
                    )
                except:
                    self.logging.warning(f"error updating document for hotel id {doc['id']}", exc_info=True)


    def insert(self, docs):
        for doc in docs:
            self.__insert_doc(doc)

    def insert_bulk(self, docs):
        if len(docs) == 0:
            return
        
        if len(docs) == 1:
            try:
                self.collection.insert_one(docs[0])
            except:
                self.logging.warning("error inserting document.", exc_info=True)
        else:
            '''
                insert_many with ordered=false tries to insert each of the documents
                individually, preferably in parallel. unlike ordered=true, it does not 
                insert in order. hence, if one insertion fails, the subsequent ones don't.

                11000 - duplicate error code
            '''
            try:
                self.collection.insert_many(docs, ordered=False)
            except pymongo.errors.BulkWriteError as ex:
                errors = ex.details['writeErrors']
                duplicateErrors = list(filter(lambda x: x['code'] == 11000, errors))
                errors = list(filter(lambda x: x['code'] != 11000, errors))

                if len(errors) > 0:
                    self.logging.error("error inserting document(/s).", exc_info=True)
                else:
                    self.logging.info(f"{len(duplicateErrors)} duplicates found. successfully inserted {len(docs) - len(errors) - len(duplicateErrors)} documents.")
            except Exception as ex:
                self.logging.error("error inserting documents.", exc_info=True)
