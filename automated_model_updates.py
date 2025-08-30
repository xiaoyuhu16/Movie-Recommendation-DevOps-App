import os
import re
from datetime import datetime, date
from read_kafka import process_kafka
from normalize_kafka import normalize_minutes, create_movie_runtime_map
from group_kafka import aggregate_ratings_minutes
from nachi_als_model_training import train_and_evaluate
from pipeline_logger import update_metrics_table
from data_collection import collect_data_for_training_into_csv

# Paths and file settings
data_folder = "training_data_collected"
models_folder = "models"
latest_data_file = "latest_data.txt"
latest_model_file = "latest_model.txt"


def load_latest_data():
    # Read the latest data file path from latest_data.txt
    with open(latest_data_file, "r") as f:
        input_csv_path = f.read().strip()

    # Extract unique_id from the filename using regex
    match = re.search(r"(?:^|/)data_(.+)\.csv", input_csv_path)
    unique_id = match.group(1) if match else None

    if unique_id is None:
        raise ValueError("Could not extract unique_id from latest data filename.")

    return input_csv_path, unique_id


def save_latest_model_path(model_path):
    # Save the model path to latest_model.txt
    with open(latest_model_file, "w") as f:
        f.write(model_path)


def run_automated_updates():
    # run data_collection
    collect_data_for_training_into_csv()

    # Create models folder if it doesn't exist
    os.makedirs(models_folder, exist_ok=True)

    # Load latest data file for processing
    input_csv_path, unique_id = load_latest_data()
    if not os.path.exists(input_csv_path):
        print(f"Data file not found: {input_csv_path}")
        return

    # Step 1: Process Kafka Stream
    process_kafka(input_csv_path=input_csv_path,
                  output_csv_path=f'training_data_collected/processed_data_{unique_id}.csv')
    print("Task 1 completed")

    # Step 2: Group User Ratings and Minutes Watched
    aggregate_ratings_minutes(input_csv_path=f'training_data_collected/processed_data_{unique_id}.csv',
                              output_csv_path=f'training_data_collected/grouped_data_{unique_id}.csv')
    print("Task 2 completed")

    # Step 3: Update movie_runtime_map.json
    create_movie_runtime_map(movie_info_path=f'training_data_collected/movie_info_{unique_id}.csv',
                             output_path='training_data_collected/movie_runtime_map.json')
    print("Task 3 completed")

    # Step 4: Normalize Movie Minutes Watched
    movie_runtime_map = "training_data_collected/movie_runtime_map.json"  # Path to runtime map
    normalize_minutes(kafka_input_path=f'training_data_collected/grouped_data_{unique_id}.csv',
                      movie_runtime_map_path=movie_runtime_map,
                      kafka_output_path=f'training_data_collected/normalized_data_{unique_id}.csv')
    print("Task 4 completed")

    # Step 5: Train and Evaluate Model
    model_path = os.path.join(models_folder, f"model_{unique_id}.pkl")
    rmse = train_and_evaluate(input_csv_path=f'training_data_collected/normalized_data_{unique_id}.csv',
                              output_model_path=model_path)
    print(f"Task 5 completed. RMSE of trained model: {rmse:.4f}")

    # Update latest_model.txt
    save_latest_model_path(os.path.join("/", model_path))

    # Push to production


run_automated_updates()
