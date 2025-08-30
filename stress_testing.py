import pickle
import random
import time
import pandas as pd
import numpy as np
# Load the model and mappings from the pickle file
def load_model(pickle_file):
    with open(pickle_file, 'rb') as f:
        data = pickle.load(f)
    return data

# Recommendation function
def load_model_and_recommend(model_data, user_id, n_recommendations=20):
    model = model_data['model']
    user_map = model_data['user_map']
    item_map = model_data['item_map']

    # If user not found, select a random user
    if user_id not in user_map:
        print(f"User ID {user_id} not found. Selecting a random user.")
        user_id = random.choice(list(user_map.keys()))  # Select a random user ID

    user_idx = user_map[user_id]

    # Check if user_idx is within bounds of the trained model
    if user_idx >= model.user_factors.shape[0]:
        #print(f"User index {user_idx} for user ID {user_id} is out of bounds. Returning random movies.")
        return random.sample(list(item_map.keys()), n_recommendations)

    scores = model.item_factors @ model.user_factors[user_idx]

    # Get top N recommendations (item indices)
    top_items = np.argsort(scores)[-n_recommendations:][::-1]

    # Only recommend items that exist in the item_map
    recommended_movies = []
    for i in top_items:
        if i in item_map.values():  # Check if the item index exists in item_map
            movie_name = list(item_map.keys())[list(item_map.values()).index(i)]
            recommended_movies.append(movie_name)

    # If less than 20 recommendations, fill with random movies
    if len(recommended_movies) < n_recommendations:
        #print(f"Filling with random movies. Found {len(recommended_movies)} recommendations.")
        # Fill the rest with random movies
        additional_movies = random.sample(list(item_map.keys()), n_recommendations - len(recommended_movies))
        recommended_movies.extend(additional_movies)

    return recommended_movies




# Step 1: Load the dataset to sample 2000 random users
data = pd.read_csv('user_movie_interaction.csv')  # Replace with your actual dataset path

# Step 2: Sample 2000 random user IDs from the dataset
sampled_users = random.sample(list(data['User ID'].unique()), 2000)

# Load the ALS model and mappings
model_data = load_model('nachi_model.pkl')  # Path to your pickle file

# Step 3: Stress testing by calling the recommendation function for 2000 users
total_inference_time = 0
total_recommendations = 0
valid_user_count = 0  # Track the number of valid users processed

# Track total number of recommendations for all users
for user_id in sampled_users:
    start_time = time.time()  # Start timer

    # Get recommendations for the user
    recommended_movies = load_model_and_recommend(model_data, user_id, n_recommendations=20)

    end_time = time.time()  # End timer

    if recommended_movies:
        valid_user_count += 1  # Count valid user recommendations only

        # Calculate inference time
        total_inference_time += (end_time - start_time)

        # Keep track of the total number of recommendations returned
        total_recommendations += len(recommended_movies)

# Step 4: Calculate average inference time per user (only for valid users)
if valid_user_count > 0:
    average_inference_time = total_inference_time / valid_user_count
else:
    average_inference_time = 0

# Print out the results
print(f"Total inference time: {total_inference_time:.4f} seconds")
print(f"Average inference time per user: {average_inference_time:.6f} seconds")
print(f"Total recommendations returned for all users: {total_recommendations}")
print(f"Total valid users processed: {valid_user_count}")
