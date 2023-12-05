from pymongo import MongoClient
from bson.objectid import ObjectId

# Create a MongoDB client
client = MongoClient("Link")

# Get the "movies" collection
db = client["sample_mflix"]
movies_collection = db["movies"]


# List of movie IDs to fetch details for
movie_ids_to_fetch = [
    "573a13bff29313caabd5e91e",
    "573a13a3f29313caabd0d1e3",
    "573a13a5f29313caabd159a9",
    "573a13abf29313caabd25582",
    "573a13b3f29313caabd3b647",
    "573a139bf29313caabcf3a45",
    "573a13bcf29313caabd57db6",
    "573a13b0f29313caabd3505e",
    "573a13a0f29313caabd05ae1",
    "573a13acf29313caabd289b3"
]

# Fetch and print details for each movie ID
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


