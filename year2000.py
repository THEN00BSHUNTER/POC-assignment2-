from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime
import dotenv
import os

dotenv.load_dotenv()

# Make sure to replace the connection string with your own
client = MongoClient(os.getenv('MONGO_LINK'))

db = client['sample_mflix']
comments = db['comments']

# Define the start and end dates for the year 2000
start_date = datetime.datetime(2000, 1, 1)
end_date = datetime.datetime(2001, 1, 1)

# MongoDB aggregation pipeline to group by movie_id and count the comments
pipeline = [
    {"$match": {"date": {"$gte": start_date, "$lt": end_date}}},
    {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
]

top_movies = list(comments.aggregate(pipeline))
print(top_movies)
print("________________________________________________________________")
# Extract 'ObjectId' values and store them in a list
ids_list = [str(item['_id']) for item in top_movies]

print(ids_list)
print("________________________________________________________________")

# Print the top 10 movie_ids with the most comments
for movie in top_movies:
    print(f"Movie ID: {movie['_id']}, Number of comments: {movie['count']}")
print("________________________________________________________________")

movies_collection = db["movies"]

# List of movie IDs to fetch details for
movie_ids_to_fetch = ids_list

for movie_id in movie_ids_to_fetch:
    movie_details = movies_collection.find_one({"_id": ObjectId(movie_id)})
    if movie_details:
        print(f"Movie ID: {movie_id}")
        print(f"Title: {movie_details.get('title')}")
        print(f"Genres: {movie_details.get('genres')}")
        print(f"Release Year: {movie_details.get('year')}")
        print("------")
    else:
        print(f"Movie details not found for Movie ID: {movie_id}")
print("________________________________________________________________")
