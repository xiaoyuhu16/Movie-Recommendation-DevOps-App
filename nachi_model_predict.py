# Step 8: Load the model and make recommendations for a user
import pickle
import numpy as np
import random

def load_model_and_recommend(pickle_file, user_id, n_recommendations=20):
    with open(pickle_file, 'rb') as f:
        data = pickle.load(f)

    model = data['model']
    user_map = data['user_map']
    item_map = data['item_map']

    if user_id not in user_map:
        #print("User ID not found.")
        return random.sample(list(item_map.keys()), n_recommendations)

    user_idx = user_map[user_id]
    scores = model.item_factors @ model.user_factors[user_idx]

    # Get top N recommendations (item indices)
    top_items = np.argsort(scores)[-n_recommendations:][::-1]

    # Only recommend items that exist in the item_map
    recommended_movies = []
    for i in top_items:
        if i in item_map.values():  # Check if the item index exists in item_map
            movie_name = list(item_map.keys())[list(item_map.values()).index(i)]
            recommended_movies.append(movie_name)
            
    # If less than 20 recommendations, fill with random movies to understand user preference
    if len(recommended_movies) < n_recommendations:
        #print(f"Filling with random movies. Found {len(recommended_movies)} recommendations.")
        additional_movies = random.sample(list(item_map.keys()), n_recommendations - len(recommended_movies))
        recommended_movies.extend(additional_movies)

    return recommended_movies

# Example: Get top 10 recommendations for a user
user_id_example = 6  # Replace desired user id
recommended_movies = load_model_and_recommend('nachi_model.pkl', user_id_example, n_recommendations=20)
print("Recommended Movies:", recommended_movies)

