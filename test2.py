from pymongo import MongoClient
from bson import ObjectId

# Connect to your MongoDB Atlas cluster
# Make sure to replace the connection string with your own
client = MongoClient("Link")

# Access the "sample_mflix" database and the "comments" collection
db = client.sample_mflix
comments_collection = db.comments

# Aggregation pipeline to group by movie_id and count the number of comments
pipeline = [
    {
        "$group": {
            "_id": {"$toString": "$movie_id"},
            "count": {"$sum": 1}
        }
    },
    {
        "$sort": {"count": -1}
    },
    {
        "$limit": 10
    }
]

# Execute the aggregation pipeline
result = comments_collection.aggregate(pipeline)

# Display the top 10 movie_id with the most comments
for entry in result:
    movie_id = entry["_id"]
    comment_count = entry["count"]
    print(f"Movie ID: {movie_id}, Comments Count: {comment_count}")

# Close the MongoDB connection
client.close()
