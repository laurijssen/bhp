import pprint
import json
import pymongo
import pyodbc

client = pymongo.MongoClient("mongodb://...../?connect=replicaset", connect=True)

def copyCollection(source, dest):
    pipeline = [ { "$match": {} },
                 { "$out": dest }
               ]
    source.aggregate(pipeline)

def remove(col, dealernumber):
    col.delete_many({ 'dealernumber': { '$eq': '{}'.format(dealernumber) } })

def remove_first(col, lab, dealer, order):
    col.delete_one({'labcode': lab, 'dealernumber': dealer, 'ordernumber': order})

def example_functions(col):
    filter = { "dealernumber": { "$eq": "11928" } }

    for o in col.find(filter, { "dealernumber": 1, "source": 1 }).sort("dealernumber"):
        print(str(o))
    
    pprint.pprint(list(db.preorders.aggregate([{ "$match": {"dealernumber": "11928"} }, { "$unwind": "$detail" }, 
                                               {"$group": { "_id": None, "total": { "$sum": "$detail.articlecount" }}}])))
   
    print(str(col.count_documents({"dealernumber": "14501"})))

    remove(db.testpreorders, '14501')

def read_from_sql_server_compare_mongo(col):
    with pyodbc.connect('Driver={SQL Server};'
                      'Server=w2kdb02;'
                      'Database=Rops;'
                      'Trusted_Connection=yes;') as conn:
        cursor = conn.cursor()

        cursor.execute("select....'")

        orders = []

        for record in cursor.fetchall():
            tuple = (record[0], record[1], record[2], record[3])
            orders.append(tuple)

        print("filtered orders found", len(orders))
        count = 0
        for order in orders:
            l = order[0]
            d = order[1]
            o = order[2]
            i = order[3]

            #print(l, d, o, i)

            qry = { 'labcode': l, 'dealernumber': d, 'ordernumber': o }
            preorders = col.find(qry)

            for po in preorders:
                if l != po['labcode'] or d != po['dealernumber'] or o != po['ordernumber'] or i.lower() != po['preorderid']:
                    count += 1

        print('$not: $eq', count)

def remove_oldest_orders(col):
    ''' remove registered before last one if any '''
    pos = col.aggregate([{ 
                            "$group": { 
                                "_id": { "order": "$order", "dealer": "$dealer" }, 
                                "uniqueIds": { "$addToSet": "$_id" }, 
                                "count": { "$sum": 1 }, 
                                "maxt": { "$max": "$registeredtime" } 
                            } 
                        }, 
                        { 
                            "$match": { "count": { "$gt": 1 } } 
                        }], 
                        allowDiskUse = True)

    preorders = list(pos)

    for po in preorders:
        id = po['_id']
        maxt = po['maxt']

        print('duplicate order max ', maxt)
        print('order ', id['labcode'], id['dealernumber'], id['ordernumber'])

        preorders_found = col.find({ 'dealernumber': id['dealernumber'], 'labcode': id['labcode'], 
                                        'ordernumber': id['ordernumber'], 'registeredtime': { '$ne': maxt } })

        for p in preorders_found:
            print('delete ', id['labcode'], id['dealernumber'], id['ordernumber'], p['registeredtime'])
            deleted = col.delete_many({ 'dealernumber': id['dealernumber'], 'labcode': id['labcode'], 
                                        'ordernumber': id['ordernumber'], 'registeredtime': p['registeredtime'] })

            print('deleted records ', deleted.deleted_count)

def process_csv(col):
        with open(r'/tmp/preorders.csv', 'r') as f:
            reader = csv.reader(f)

            for i, row in enumerate(reader):
                lab = row[0]
                dealer = row[1]
                order = row[2]
                uuid = row[3]

                if list(col.find({"labcode": lab, "dealernumber": dealer, "ordernumber": order})) and len(list(col.find({"preorderid": uuid}))) == 0:
                    col.update_many({ "labcode": lab, "dealernumber": dealer, "ordernumber": order }, { "$set": { "preorderid": uuid } })

            print("num preorders {}".format(len(list(col.find({ "preorderid": { "$exists": True } })))))

def update_orders_producinglab(col):
    with pyodbc.connect('Driver={SQL Server};'
                      'Server=.;'
                      'Database=.;'
                      'Trusted_Connection=yes;') as conn:

        qry = {}

        preorders = col.find(qry)
        print(str(col.estimated_document_count()))

        for po in preorders:
            cursor = conn.cursor()
            producinglab = po['producinglab']
            fileid = po["fileid"]

            if producinglab:
                cursor.execute(f"select 1 from [order] where fileid='{fileid}' and producinglab is null")

                count = 0

                for o in cursor.fetchall():
                    count += 1

                if count > 0:
                    print('\'' + fileid + '\'')
                    cursor.execute(f"update [order] set ProducingLab = '{producinglab}' where fileid='{fileid}'")

                cursor.close()
                conn.commit()

print(client.list_database_names())

db = client["rops"]

col = db.preorders

try:
    update_orders_producinglab(col)
    print('the end')
except:
    print('error')

#read_from_sql_server_compare_mongo(col)
#remove_oldest_orders(col)

