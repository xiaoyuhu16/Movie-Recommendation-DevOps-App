import pickle
import time
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from implicit.als import AlternatingLeastSquares
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

def train_and_evaluate(input_csv_path, output_model_path):
    # Step 1: Load the user-movie interaction data
    data = pd.read_csv(input_csv_path)

    # Map user and movie IDs to indices
    user_ids = data['User ID'].astype('category').cat.codes
    movie_ids = data['Movie ID'].astype('category').cat.codes

    # Create mappings for users and movies
    user_map = {id: idx for idx, id in enumerate(data['User ID'].astype('category').cat.categories)}
    item_map = {id: idx for idx, id in enumerate(data['Movie ID'].astype('category').cat.categories)}

    # Step 2: Build the item-by-user interaction matrix
    interaction_matrix = csr_matrix((data['Normalized Minutes Watched'], (movie_ids, user_ids)))

    # Step 3: Split data into training and test sets
    unique_users = data['User ID'].unique()
    train_users, test_users = train_test_split(unique_users, test_size=0.2, random_state=42)

    # Create training and test matrices
    train_matrix = interaction_matrix.copy().tolil()
    test_matrix = interaction_matrix.copy().tolil()

    for user_id in test_users:
        if user_id in user_map:
            user_idx = user_map[user_id]
            train_matrix[:, user_idx] = 0

    for user_id in train_users:
        if user_id in user_map:
            user_idx = user_map[user_id]
            test_matrix[:, user_idx] = 0

    train_matrix = train_matrix.tocsr()
    test_matrix = test_matrix.tocsr()

    # Step 4: Train the ALS model
    model = AlternatingLeastSquares(factors=50, regularization=0.1, iterations=20)
    start_time = time.time()
    model.fit(train_matrix)
    end_time = time.time()
    print(f"Time taken for training: {end_time - start_time:.2f} seconds")

    # Step 5: Evaluate the model
    def evaluate_model(model, test_matrix, user_map):
        predicted_ratings = []
        actual_ratings = []

        for user_id in test_users:
            if user_id not in user_map:
                continue

            user_idx = user_map[user_id]

            if user_idx >= model.user_factors.shape[0]:
                #print(f"Skipping user {user_id} (index {user_idx}) - not in training set.")
                continue

            actual_user_ratings = test_matrix[:, user_idx].toarray().flatten()
            relevant_items = np.where(actual_user_ratings > 0)[0]

            if len(relevant_items) == 0:
                continue

            predicted_ratings_user = model.item_factors @ model.user_factors[user_idx]
            predicted_ratings.extend(predicted_ratings_user[relevant_items])
            actual_ratings.extend(actual_user_ratings[relevant_items])

        rmse = np.sqrt(mean_squared_error(actual_ratings, predicted_ratings))
        #print(f"RMSE on test set: {rmse:.4f}")
        return rmse

    # Step 6: Call the evaluation function
    rmse = evaluate_model(model, test_matrix, user_map)

    # Step 7: Save the ALS model and mappings
    with open(output_model_path, 'wb') as f:
        pickle.dump({
            'model': model,
            'user_map': user_map,
            'item_map': item_map,
            'train_matrix': train_matrix,
            'test_matrix': test_matrix
        }, f)

    print(f"Model and related data saved to {output_model_path}")
    return rmse

# This function can now be called in your pipeline
# Step 8: Load the model and make recommendations for a user
if __name__=="main":
    def load_model_and_recommend(pickle_file, user_id, n_recommendations=10):
        with open(pickle_file, 'rb') as f:
            data = pickle.load(f)

        model = data['model']
        user_map = data['user_map']
        item_map = data['item_map']

        if user_id not in user_map:
            print("User ID not found.")
            return []

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
            else:
                print(f"Movie index {i} not found in item_map, skipping...")

        return recommended_movies

    # Example: Get top 10 recommendations for a user
    user_id_example = 6  # Replace with an actual user ID
    recommended_movies = load_model_and_recommend('nachi_model.pkl', user_id_example, n_recommendations=10)
    print("Recommended Movies:", recommended_movies)
