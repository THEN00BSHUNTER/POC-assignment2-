from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime
import dotenv
import os
import redis
import json

dotenv.load_dotenv()
app = Flask(__name__)
# Make sure to replace the connection string with your own
client = MongoClient(os.getenv('MONGO_HOST'))

db = client['sample_mflix']
movies = db['movies']
cache = db['cache']

# connect to redis
redis_client = redis.StrictRedis(host='redis-10134.c326.us-east-1-3.ec2.cloud.redislabs.com',
                                 port=10134,
                                 password=os.getenv('REDIS_PASS'))


@app.route('/')
def hello_world():
    return 'Welcome to movie database, you can get the top n movies with the most comments by adding /topncomments to the url.'


@app.route('/topncomments')
def top_n_comments():
    n_count = int(request.args.get('n_count'))
    start_year = int(request.args.get('start_year'))
    end_year = int(request.args.get('end_year'))

    # create a cache key to get records quickly
    cache_key = 'top_{}_comments_between_{}_{}'.format(n_count, start_year, end_year)

    redis_result = redis_client.get(cache_key)
    if redis_result is not None:
        print("Found cached result in Redis")
        return json.loads(redis_result), 200

    # check if the result is in mongo cache collection
    mongo_cache_result = cache.find_one({'key': cache_key})
    if mongo_cache_result is not None:
        print("Found cached result in MongoDB")
        return json.loads(mongo_cache_result['value']), 200

    if n_count is None:
        n_count = 10

    if start_year is None:
        error_message = {'error': 'No start year was provided'}
        return jsonify(error_message), 400

    if end_year is None:
        error_message = {'error': 'No end year was provided'}
        return jsonify(error_message), 400

    top_movies = mongo_pipeline_exicute(n_count, start_year, end_year)

    # save the result in redis cache
    redis_client.set(cache_key, json.dumps(top_movies))
    # save the result in mongo cache collection
    cache.insert_one({'key': cache_key, 'value': json.dumps(top_movies)})
    return jsonify(top_movies), 200


@app.route('/topncomments_no_redis_cache')
def top_n_comments_no_redis_cache():
    n_count = int(request.args.get('n_count'))
    start_year = int(request.args.get('start_year'))
    end_year = int(request.args.get('end_year'))

    # create a cache key to get records quickly
    cache_key = 'top_{}_comments_between_{}_{}'.format(n_count, start_year, end_year)

    # check if the result is in mongo cache collection
    mongo_cache_result = cache.find_one({'key': cache_key})
    if mongo_cache_result is not None:
        print("Found cached result in MongoDB")
        return json.loads(mongo_cache_result['value']), 200

    if n_count is None:
        n_count = 10

    if start_year is None:
        error_message = {'error': 'No start year was provided'}
        return jsonify(error_message), 400

    if end_year is None:
        error_message = {'error': 'No end year was provided'}
        return jsonify(error_message), 400

    top_movies = mongo_pipeline_exicute(n_count, start_year, end_year)

    # save the result in mongo cache collection
    cache.insert_one({'key': cache_key, 'value': json.dumps(top_movies)})
    return jsonify(top_movies), 200


@app.route('/topncomments_no_cache')
def top_n_comments_no_cache():
    n_count = int(request.args.get('n_count'))
    start_year = int(request.args.get('start_year'))
    end_year = int(request.args.get('end_year'))

    if n_count is None:
        n_count = 10

    if start_year is None:
        error_message = {'error': 'No start year was provided'}
        return jsonify(error_message), 400

    if end_year is None:
        error_message = {'error': 'No end year was provided'}
        return jsonify(error_message), 400

    top_movies = mongo_pipeline_exicute(n_count, start_year, end_year)

    return jsonify(top_movies), 200


def mongo_pipeline_exicute(n_count, start_year, end_year):
    # MongoDB aggregation pipeline to group by movie_id and count the comments
    pipeline = [
        {"$match": {
            "num_mflix_comments": {"$exists": True, "$ne": 0},  # Filter out movies with no comments
            "year": {"$gte": start_year, "$lte": end_year}  # Filter movies based on the release year range
        }},
        {"$project": {
            "title": 1,
            "year": 1,
            "num_comments": "$num_mflix_comments",
        }},  # Project title, year, and number of comments
        {"$sort": {"num_comments": -1}},  # Sort documents by the number of comments in descending order
        {"$limit": n_count}  # Limit the result to the top 10 documents
    ]

    top_movies = list(movies.aggregate(pipeline))
    # replace ObjectId with string
    for movie in top_movies:
        movie['_id'] = str(movie['_id'])

    return top_movies


if __name__ == '__main__':
    app.run(debug=True)
