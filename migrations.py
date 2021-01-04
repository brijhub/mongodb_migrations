import pandas as pd
from pymongo import MongoClient
import datetime
from bson import ObjectId

def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
        print('connected')
    else:
        conn = MongoClient(host, port)


    return conn[db]


def _connect_mongo_new(host, port, db):
    """ A util for making a connection to mongo """
    conn = MongoClient(host, port, db)
    return conn[db]


def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    # Collection
    cursor_object = db[collection].find({'isActive':True, 'isPublished':True})

    output = pd.DataFrame()

    for doc in cursor_object:
        if (not doc['name']=='None' and 'icon' in doc and doc['icon']):
            # Media Collection
            cursor_media = db['Media'].find({'_id': doc['icon']})
            for media_doc in cursor_media:
                exclude_keys = ['_id']
                new_d = {k: media_doc[k] for k in set(list(media_doc.keys())) - set(exclude_keys)}
                new_d['name'] = doc['name']
                output = output.append(new_d, ignore_index=True)
            
    return output


def get_result_df(collection):
    result_df = read_mongo(username='smart_cookie_user_ds_fixes', password = 'ICgz8Ej9J46A',
            host = '43.241.63.107', port = '27017', db = 'smart-cookie-mvp-ds-fixes',
            collection = collection)
    result_df['createdBy'] = ObjectId("5fec514b8ec9efd65f9b11dc")
    result_df['updatedBy'] = ObjectId("5fec514b8ec9efd65f9b11dc")
    result_df['createdAt'] = datetime.datetime.now()
    result_df['updatedAt'] = datetime.datetime.now()
    result_df['isActive'] = True
    result_df['isPublished'] = True
    return result_df




# output_df = get_result_df('Cuisine')
# output_df.to_csv('cuisine_images.csv', index=False)

output_df = get_result_df('MealType')
output_df.to_csv('mealtype_images.csv', index=False)

# print("Start dumping process")
# db_new = _connect_mongo(host='43.241.63.107', port='27017', username='slurrp_user', password='3rrV6JAH9A50', db='slurrp')
# cursor_media_new = db_new['Media']
# start_time = datetime.datetime.now()
# cursor_media_new.insert_many(output_df.to_dict('records'))
# end_time = datetime.datetime.now()
# print("Total time to dump records:-{}".format(end_time-start_time))

# mongo_uri = 'mongodb://%s:%s/%s' % ('127.0.0.1', '27017', 'slurrp_dummy')
# conn = MongoClient(mongo_uri)
# db_new = conn['slurrp_dummy']
# db_new = _connect_mongo_new(host='127.0.0.1', port=27017, db='slurrp_dummy')
