from .exceptions import NotImplementedException
from pymongo import MongoClient, DESCENDING
import logging
import asyncio
import motor.motor_asyncio
from .enums import *
from time import time, sleep
import copy
import bson
class MongoClient():

    
    def __init__(self, _host, _port, _db='bot', clean=False) -> None:
        self.client = motor.motor_asyncio.AsyncIOMotorClient(host=_host, port=_port)

        # TODO: Implement normal client as well. It is hard to test with asycn cli
        #self.normal_client = MongoClient(host=_host, port=_port)
        self.logger = logging.getLogger('app.{}'.format(__name__))
        self.logger.debug('Mongo client initiated')

        # Drop the db if it is no the main one
        if clean:
            self.client.drop_database(_db)
        self.db_bot = self.client[_db]


    async def get_collection_names(self):
        return await self.db_bot.list_collection_names()


    async def count(self, col, query={}) -> None:
        return await self.db_bot[col].count_documents(query)


    async def do_find(self, col, query) -> None:
        """
        This function reads the selected item from the given collection

        Args:
            col ([type]): [description]
            item ([type]): [description]
        """
        docs = []
        if type(query) == dict:
            result = self.db_bot[col].find(query)
            docs = await result.to_list(None)
            self.logger.debug(f"do_find [{col}]: total found document: {len(docs)}")
        elif type(query) == list:
            async for doc in self.db_bot[col].aggregate(query):
                docs=doc
        return docs


    async def do_aggregate(self, col, query) -> None:
        docs = []
        if type(query) == list:
            async for doc in self.db_bot[col].aggregate(query):
                docs.append(doc)
        else:
            raise NotImplementedException('do_aggregate requires type list as input')
        return docs


    async def do_insert_one(self, col, item) -> None:
        """
        Args:
            col (str): Name of the collection: [live-trade | hist-trade | observer]
            item (dict): Dictionary item
        """


        result = await self.db_bot[col].insert_one(item)
        
        self.logger.debug(f'do_insert_one [{col}]: inserted id "{result.inserted_id}"')
        return result


    async def do_insert_many(self, col, item_list) -> None:
        """
        This function writes the selected item into the collection 

        Args:
            col (string): db collection
            item_list (list): Dictionary of GenericObject
        """        
        result = await self.db_bot[col].insert_many(item_list)
        self.logger.debug(f"do_insert_many [{col}]: inserted ids {result.inserted_ids}")
        return result


    async def do_update(self, col, query, update) -> None:
        """
        Args:
            col (string): db collection
            query (dict): json query
            update (dict): update rule
        """
        result = await self.db_bot[col].update_one(query, update)
        if '_id' in query.keys(): self.logger.debug(f"do_update [{col}]: \"{query['_id']}\"")
        return result


    async def do_delete_many(self, col, query) -> None:
        """
        Args:
            col (string): db collection
            query (dict): json query
        """
        # TODO: Remove the count statments or optimize them, they look ugly
        prev_count = await self.count(col)
        result = self.db_bot[col].delete_many(query)
        after_count = await self.count(col)
        self.logger.debug(f"do_delete [{col}]: prev count {prev_count}, after count {after_count}")
        return result

# Specific Methods:
    async def get_n_docs(self, col, query={}, order=DESCENDING, n=1) -> None:
        """
        This function reads the selected item from the given collection

        Args:
            col ([type]): [description]
            item ([type]): [description]
        """
        result = self.db_bot[col].find(query).sort('_id', order).limit(n)
        doc_list = []
        async for document in result:
            doc_list.append(dict(document))

        assert 'doc_list' in locals(), "No last document!"
        assert len(doc_list) > 0, "No document"
        return doc_list


async def test1():
    from objects import GenericObject

    to1, to2, to3, to4= GenericObject('trade'), GenericObject('trade'), GenericObject('trade'), GenericObject('trade')
    to1.load('status',STAT_OPEN_ENTER)
    to2.load('status',STAT_OPEN_ENTER)
    to3.load('status',STAT_OPEN_EXIT)
    to4.load('status',STAT_CLOSED)

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
        if lto['status'] == STAT_OPEN_ENTER:
            lto_list_updated[i]['status'] = STAT_OPEN_EXIT
        elif lto['status'] == STAT_OPEN_EXIT:
            lto_list_updated[i]['status'] = STAT_CLOSED
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
    result_delete = await mongocli.do_delete_many('live-trades',{'status':STAT_CLOSED})
    
    return True


async def test2():

    pipe = [
        {"$match":{"result.cause":{"$eq":"exit_expire"}}},
        {"$group": {"_id": '', "sum": {"$sum": '$result.profit'}}},
    ]
    exit_expire_profit = await mongocli.do_find("hist-trades", pipe)
    print('observer.result.profit: exit_expire : {}'.format(exit_expire_profit['sum']))
    
    pipe = [
        {"$match":{"result.cause":{"$eq":"closed"}}},
        {"$group": {"_id": '', "sum": {"$sum": '$result.profit'}}},
    ]
    closed_profit = await mongocli.do_find("hist-trades", pipe)
    print('observer.result.profit: closed : {}'.format(closed_profit['sum']))

    last_balance = await mongocli.get_n_doc("observer")
    for balance in last_balance['balances']:
        if balance['asset'] == 'USDT':
            usdt_balance = balance['total']
            break
    print('Final equity : {}'.format(usdt_balance))

    return True


if __name__ == "__main__":
    mongocli = MongoClient("localhost", 27017, 'test-bot')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test2())
