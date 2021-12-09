{'$and': [
    {'seller_nick': {'$in': ['方太官方旗舰店']}}, 
    {'begin_time': {'$gte': datetime(2021, 12, 4, 16, 0), '$lt': datetime(2021, 12, 5, 16, 0)}}
]}

{'$and': [{'seller_nick': {'$in': ['方太官方旗舰店']}}, {'begin_time': {'$gte': new ISODate('2021-12-05T16:00:00Z'), '$lt': new ISODate('2021-12-06T16:00:00Z')}}]}

db.dialog.find({'$and': [{'seller_nick': {'$in': ['方太官方旗舰店']}}, {'begin_time': {'$gte': new ISODate('2021-12-05T16:00:00Z'), '$lt': new ISODate('2021-12-06T16:00:00Z')}}]}).count()

db.dialog.find({'$and': [{'seller_nick': {'$in': ['方太官方旗舰店']}}, {'begin_time': {'$gte': new ISODate('2021-12-05T16:00:00Z'), '$lt': new ISODate('2021-12-06T16:00:00Z')}}]}).count()

db.dialog.find({'$and': [{'seller_nick': {'$in': ['方太官方旗舰店']}}]}).explain()

db.dialog.find({'$and': [{'seller_nick': {'$in': ['方太官方旗舰店']}}, {'begin_time': {'$gte': new ISODate('2021-12-05T16:00:00Z'), '$lt': new ISODate('2021-12-06T16:00:00Z')}},{'score': {'$gt':0}}]}).count()



