import re
import pymongo
import logging
import asyncio
from datetime import datetime
import motor.motor_asyncio
from time import time, sleep
import copy
import bson
import pprint

# pre-written queries
queries = {
    "open_expire": {"result.cause":"enter_expire"},
    "exit_expire": {"result.cause":"exit_expire"},
    "closed": {"result.cause":"closed"},
}

class MongoClient():

    
    def __init__(self, _host, _port, _db='bot') -> None:
        self.client = motor.motor_asyncio.AsyncIOMotorClient(host=_host, port=_port)
        self.logger = logging.getLogger('app.{}'.format(__name__))

        # Drop the db if it is no the main one
        if _db != 'bot':
            self.client.drop_database(_db)
        self.db_bot = self.client[_db]

    async def count(self, col, query={}) -> None:
        return await self.db_bot[col].count_documents(query)

    async def do_find(self, col, query) -> None:
        """
        This function reads the selected item from the given collection

        Args:
            col ([type]): [description]
            item ([type]): [description]
        """
        result = self.db_bot[col].find(query)
        docs = await result.to_list(None)
        self.logger.info(f"do_find [{col}]: total found document: {len(docs)}")
        return docs


    async def do_insert_one(self, col, item) -> None:
        """
        Args:
            col (str): Name of the collection: [live-trade | hist-trade | observer]
            item (dict): Dictionary item
        """
        
        # Add timestamp as the "_id" of the document if there is already
        if '_id' not in item.keys():
            item['_id'] = int(time() * 1000)
        else:
            item['_id'] = bson.Int64(item['_id'])
            

        result = await self.db_bot[col].insert_one(item)
        
        self.logger.info(f"do_insert_one [{col}]: inserted id {result.inserted_id}")
        return result


    async def do_insert_many(self, col, item_dict) -> None:
        """
        This function writes the selected item into the collection 

        Args:
            col (string): db collection
            item_dict (dict): Dictionary of GenericObject
        """        

        insert_list = []
        for pair, obj in item_dict.items():
            obj.load('pair',pair)
            #timestamp = int(time() * 1000)
            #print("int value:",timestamp)
            obj.load('_id',int(time() * 1000))
            insert_list.append(obj.get())
            sleep(0.01)
            
        result = await self.db_bot[col].insert_many(insert_list)
        self.logger.info(f"do_insert_many [{col}]: inserted ids {result.inserted_ids}")
        return result


    async def do_update(self, col, query, update) -> None:
        """
        Args:
            col (string): db collection
            query (dict): json query
            update (dict): update rule
        """
        # TODO: Update an item in a deeper position in the hierarchy
        result = await self.db_bot[col].update_one(query, update)
        self.logger.info(f"do_update [{col}]: ")
        return result


    async def do_delete_many(self, col, query) -> None:
        """
        Args:
            col (string): db collection
            query (dict): json query
        """
        # TODO: Test needed
        prev_count = await self.count(col)
        result = self.db_bot[col].delete_many(query)
        after_count = await self.count(col)
        self.logger.info(f"do_delete [{col}]: prev count {prev_count}, after count {after_count}")
        return result

# Specific Functions:
    async def get_last_doc(self, col, query) -> None:
        """
        This function reads the selected item from the given collection

        Args:
            col ([type]): [description]
            item ([type]): [description]
        """
        result = self.db_bot[col].find(query).sort('_id', pymongo.DESCENDING).limit(1)
        async for document in result:
            last_doc = dict(document)
        # NOTE: Add proper error handling
        return last_doc


async def test1():
    from objects import GenericObject

    to1, to2, to3, to4= GenericObject('trade'), GenericObject('trade'), GenericObject('trade'), GenericObject('trade')
    to1.load('status','open_enter')
    to2.load('status','open_enter')
    to3.load('status','open_exit')
    to4.load('status','closed')

    trade_dict = dict({'BTCUSDT':to1, 'XRPUSDT':to2, 'AVAXUSDT':to3, 'USDTTRY':to4})

    # Insert
    print('Before insert many:')
    result = await mongocli.do_insert_many("live-trades",trade_dict)
    
    # Find
    lto_list = await mongocli.do_find('live-trades',{})
    lto_list_updated = copy.deepcopy(lto_list)

    print('lto_list:',lto_list)
    
    # Update the values in the lto_list
    for i, lto in enumerate(lto_list):
        if lto['status'] == 'open_enter':
            lto_list_updated[i]['status'] = 'open_exit'
        elif lto['status'] == 'open_exit':
            lto_list_updated[i]['status'] = 'closed'
        pass

    
    # Write the lto_list to the
    for lto, lto_upd in zip(lto_list,lto_list_updated):
        if lto['status'] != lto_upd['status']:
            result_update = await mongocli.do_update(
                "live-trades",
                {'_id': lto['_id']},
                {'$set': {'status': lto_upd['status']}})
        print(lto['pair'],'result_update:',result_update)
    
    
    # Delete
    result_delete = await mongocli.do_delete_many('live-trades',{'status':'closed'})
    
    return True


async def test2():

    # Find
    lto_list = await mongocli.get_last_doc('hist-trades',{})
    
    return True


if __name__ == "__main__":
    mongocli = MongoClient("localhost", 27017, 'test-bot')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test2())
