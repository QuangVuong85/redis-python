import redis
import logging
import json
import sys
from pprint import pprint


def buy_item(r: redis.Redis, itemid: str) -> None:
    with r.pipeline() as pipe:
        pipe.multi()
        pipe.hset(itemid, "quantity", 10)
        pipe.hset(itemid, "price", 1000)
        pipe.execute()


def is_redis_available(r):
    try:
        r.ping()
        print('Successfully connected to redis')
    except redis.exceptions.ConnectionError as r_con_error:
        print('Redis connection error:', r_con_error)
        sys.exit(0)


def is_redis_close(r):
    try:
        r.close()
        print('Successfully closed connection to redis')
    except redis.exceptions.ConnectionError as r_con_error:
        print('Redis connection error:', r_con_error)
        sys.exit(0)


def main():
    # init
    r = redis.Redis(host='localhost', port=6379, db=0, password='123ABC456')
    logging.basicConfig(filename='logs/runtime.log',
                        filemode='a',
                        format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
                        level=logging.INFO,
                        datefmt='%d-%m-%Y %H:%M:%S')
    logging.info(r.info())

    # check connection
    is_redis_available(r)

    # clear all key in db
    r.flushall()

    # set
    r.mset({"name": "vuongdq85", "age": 18})
    print(r.get('name'))
    print(r.get('age'))

    restaurant_484272 = {
        "name": "Cafe",
        "type": "Drink",
        "address": {
            "street": {
                "line1": "180 Hoang Quoc Viet",
                "line2": "49/16 Tran Cung",
            },
            "city": "Ha Noi",
            "state": "HN",
            "zip": 100000,
        }
    }
    r.set(484272, json.dumps(restaurant_484272))
    pprint(json.loads(r.get(484272)))

    # transactions
    buy_item(r, "list:123")
    pprint(r.hvals("list:123"))

    # close connection
    is_redis_close(r)


if __name__ == '__main__':
    main()
