import pickle
import numpy as np
from scipy.sparse import csr_matrix
import pandas as pd
from sklearn.metrics import mean_squared_error
from read_kafka import process_kafka
from normalize_kafka import normalize_minutes
from group_kafka import aggregate_ratings_minutes


def online_eval(movie_runtime_map, model_file_path, online_evaluation_csv):
    process_kafka(
        input_csv_path=online_evaluation_csv,
        output_csv_path='pipeline_output/processed_online_evaluation.csv'
    )

    aggregate_ratings_minutes(
        input_csv_path='pipeline_output/processed_online_evaluation.csv',
        output_csv_path='pipeline_output/grouped_online_evaluation.csv'
    )

    normalize_minutes(
        kafka_input_path='pipeline_output/grouped_online_evaluation.csv',
        movie_runtime_map_path=movie_runtime_map,
        kafka_output_path='pipeline_output/normalized_online_evaluation.csv'
    )

    # Call evaluate_online_model function
    online_rmse = evaluate_online_model(
        model_file_path=model_file_path,
        online_evaluation_csv='pipeline_output/normalized_online_evaluation.csv'
    )

    return online_rmse


# Step 8: Online evaluation with production data and pre-trained model from step 4
def evaluate_online_model(model_file_path, online_evaluation_csv):
    # Evaluate the trained ALS model on online evaluation data by predicting interactions and calculating RMSE.
    try:
        # Load the trained model and mappings
        with open(model_file_path, 'rb') as f:
            model_data = pickle.load(f)
    except FileNotFoundError:
        print(f"Trained model file {model_file_path} not found.")
        return None
    except Exception as e:
        print(f"An error occurred while loading the model: {e}")
        return None

    # Extract model and mappings
    model = model_data['model']
    user_map = model_data['user_map']
    item_map = model_data['item_map']

    # Load the normalized online evaluation data
    try:
        online_data = pd.read_csv(online_evaluation_csv)
    except FileNotFoundError:
        print("Normalized online evaluation CSV file not found.")
        return None
    except Exception as e:
        print(f"An error occurred while loading online evaluation data: {e}")
        return None

    # Convert the keys in user_map to strings
    user_map = {str(key): value for key, value in user_map.items()}

    # Map user IDs and movie IDs to indices using the mappings
    online_data['user_idx'] = online_data['User ID'].map(user_map)
    online_data['movie_idx'] = online_data['Movie ID'].map(item_map)

    # Drop rows with unknown users or movies
    online_data = online_data.dropna(subset=['user_idx', 'movie_idx'])

    # Convert indices to integers
    online_data['user_idx'] = online_data['user_idx'].astype(int)
    online_data['movie_idx'] = online_data['movie_idx'].astype(int)

    # Create interaction matrix for online data
    online_interaction_matrix = csr_matrix(
        (online_data['Normalized Minutes Watched'], (online_data['movie_idx'], online_data['user_idx']))
    )
    # Evaluate the model on online data
    test_coo = online_interaction_matrix.tocoo()

    # Get user and item factors
    user_factors = model.user_factors
    item_factors = model.item_factors

    # Compute predicted scores using the dot product of user and item factors
    predicted_scores = np.sum(user_factors[test_coo.row] * item_factors[test_coo.col], axis=1)

    # Actual scores
    actual_scores = test_coo.data

    # Calculate RMSE
    online_rmse = np.sqrt(mean_squared_error(actual_scores, predicted_scores))

    return online_rmse

