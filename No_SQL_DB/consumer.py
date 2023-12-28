from pymongo import MongoClient

def get_results_mongo(collection_name):
    mongo_host = 'localhost'
    mongo_port = 27017
    mongo_database = 'lr_5_data'

    # Connect to MongoDB
    mongo_client = MongoClient(mongo_host, mongo_port)
    mongo_db = mongo_client[mongo_database]
    collection = mongo_db[collection_name]

    try:
        # Find all documents in the collection
        result = collection.find()

        # Print all documents
        print(f'All documents in {collection_name}:')
        for doc in result:
            return doc
    except Exception as e:
        print(f"Error during the find operation: {str(e)}")
    finally:
        mongo_client.close()









def get_collection_column_names(database_name, collection_name):
    # Connect to MongoDB
    mongo_host = 'localhost'
    mongo_port = 27017

    mongo_client = MongoClient(mongo_host, mongo_port)
    mongo_db = mongo_client[database_name]

    # Get indexes for the collection
    indexes = mongo_db[collection_name].index_information()

    # Extract column names from the indexes
    column_names = [field for field in indexes.keys() if field != '_id']

    # Close MongoDB connection
    mongo_client.close()

    return column_names

