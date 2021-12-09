-- 会话总量
-- MongoDB
db.dialog.aggregate([
    {'$match':{
        '$and': [
            {'seller_nick': {'$in': ['方太官方旗舰店']}}, 
            {'begin_time': {'$gte': new ISODate('2021-11-30T16:00:00Z'), '$lt': new ISODate('2021-12-08T16:00:00Z')}},
            ]
        }
    },
    {'$group':{
        '_id':{ $dateToString: { 'date': "$begin_time",'format': "%Y-%m-%d", 'timezone':'+08' } },
        'count': { $sum: 1 }
    }},
    {'$sort':{'_id':1}}
])

-- 原始
SELECT toYYYYMMDD(begin_time) AS day, COUNT(1)
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN 20211201 AND 20211208
AND seller_nick IN ['方太官方旗舰店']
GROUP BY toYYYYMMDD(begin_time)
ORDER BY day DESC

-- 统计
SELECT toYYYYMMDD(`date`) AS day, sum(session_count)
FROM ods.qc_session_count_all
WHERE toYYYYMMDD(`date`) BETWEEN 20211201 AND 20211208
AND shop_name IN ['方太官方旗舰店']
-- AND department_id != ''
GROUP BY toYYYYMMDD(`date`)
ORDER BY day ASC

-- 扣分会话
-- MongoDB
db.dialog.aggregate([
    {'$match':{
        '$and': [
            {'seller_nick': {'$in': ['方太官方旗舰店']}}, 
            {'begin_time': {'$gte': new ISODate('2021-11-30T16:00:00Z'), '$lt': new ISODate('2021-12-08T16:00:00Z')}},
            {
                '$or':[
                    {'mark_score': {'$gt':0}},
                    {'score': {'$gt':0}},
                    {'rule_stats.score':{'$gt':0}}
                ]
            }]
        }
    },
    {'$group':{
        '_id':{ $dateToString: { 'date': "$begin_time",'format': "%Y-%m-%d", 'timezone':'+08' } },
        'count': { $sum: 1 }
    }},
    {'$sort':{'_id':1}}
])

-- 原始
SELECT toYYYYMMDD(begin_time) AS day, COUNT(1)
FROM dwd.xdqc_dialog_all
WHERE toYYYYMMDD(begin_time) BETWEEN 20211201 AND 20211208
AND seller_nick IN ['方太官方旗舰店']
AND (mark_score>0 or score>0 or arraySum(rule_stats_score)>0)
GROUP BY toYYYYMMDD(begin_time)
ORDER BY day ASC

-- 统计
SELECT toYYYYMMDD(`date`) AS day, sum(subtract_score_count)
FROM ods.qc_session_count_all
WHERE toYYYYMMDD(`date`) BETWEEN 20211201 AND 20211208
AND shop_name IN ['方太官方旗舰店']
-- AND department_id != ''
GROUP BY toYYYYMMDD(`date`)
ORDER BY day ASC



-- AI质检异常会话量
db.dialog.aggregate([
    {'$match':{
        '$and': [
            {'seller_nick': {'$in': ['方太官方旗舰店']}}, 
            {'begin_time': {'$gte': new ISODate('2021-11-30T16:00:00Z'), '$lt': new ISODate('2021-12-08T16:00:00Z')}},
            {'score': {'$gt':0}},]
        }
    },
    {'$group':{
        '_id':{ $dateToString: { 'date': "$begin_time",'format': "%Y-%m-%d", 'timezone':'+08' } },
        'count': { $sum: 1 }
    }},
    {'$sort':{'_id':1}}
])


-- 人工抽检量
db.dialog.aggregate([
    {'$match':{
        '$and': [
            {'seller_nick': {'$in': ['方太官方旗舰店']}}, 
            {'begin_time': {'$gte': new ISODate('2021-11-30T16:00:00Z'), '$lt': new ISODate('2021-12-08T16:00:00Z')}},
            {'mark_ids': {'$exists': true, '$not': {'$size': 0} }},]
        }
    },
    {'$group':{
        '_id':{ $dateToString: { 'date': "$begin_time",'format': "%Y-%m-%d", 'timezone':'+08' } },
        'count': { $sum: 1 }
    }},
    {'$sort':{'_id':1}}
])


-- 人工质检扣分量(人工质检扣分会话)
db.dialog.aggregate([
    {'$match':{
        '$and': [
            {'seller_nick': {'$in': ['方太官方旗舰店']}}, 
            {'begin_time': {'$gte': new ISODate('2021-11-30T16:00:00Z'), '$lt': new ISODate('2021-12-08T16:00:00Z')}},
            {'mark_score': {'$gt':0}},
        }
    },
    {'$group':{
        '_id':{ $dateToString: { 'date': "$begin_time",'format': "%Y-%m-%d", 'timezone':'+08' } },
        'count': { $sum: 1 }
    }},
    {'$sort':{'_id':1}}
])


-- AI质检问题Top10
-- AI质检