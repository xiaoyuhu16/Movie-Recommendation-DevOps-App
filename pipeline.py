from datetime import datetime
from read_kafka import process_kafka
from normalize_kafka import normalize_minutes,create_movie_runtime_map
from group_kafka import aggregate_ratings_minutes
from nachi_als_model_training import train_and_evaluate
from pipeline_logger import update_metrics_table
from online_eval_pipeline import online_eval
import time
import os
import argparse


def main(input_csv_path, movie_runtime_map, model_file_path,  run_online_eval, online_evaluation_csv=None):
    print("Starting pipeline execution..")

    start_time = time.time()

    # Task 1: Process Kafka Stream
    print("Executing Task 1: Processing and Cleaning Kafka Stream")

    process_kafka(
        input_csv_path=input_csv_path,
        output_csv_path='pipeline_output/processed_kafka_stream.csv'
    )

    print("Task 1 completed")

    print("Executing Task 2: Grouping User Ratings and Minutes Watched")
    aggregate_ratings_minutes(
        input_csv_path='pipeline_output/processed_kafka_stream.csv',
        output_csv_path='pipeline_output/grouped_kafka_stream.csv'
    )
    print("Task 2 completed")

    # Step 2: Create Movie Runtime Map
    #print("Executing Task 3: Creating Movie Runtime Map")

    #create_movie_runtime_map(
    #    movie_info_path=movie_info_path,
    #    output_path='pipeline_output/movie_runtime_map.json'
    #) Providing the hardcoded movie_runtime_map to the pipeline module directly

    #print("Task 3 completed")

    # Task 2: Normalize Kafka Stream
    print("Executing Task 3: Normalizing Movie Minutes Watched")

    normalize_minutes(
        kafka_input_path='pipeline_output/grouped_kafka_stream.csv',
        movie_runtime_map_path=movie_runtime_map,
        kafka_output_path='pipeline_output/normalized_kafka_stream.csv'
    )
    print("Task 3 completed")

    print("Executing Task 4: Training and Evaluating Model")
    rmse = train_and_evaluate(
        input_csv_path='pipeline_output/normalized_kafka_stream.csv',
        output_model_path='pipeline_output/nachi_model.pkl'
    )
    print(f"Task 4 completed. RMSE of trained model: {rmse:.4f}")

    end_time = time.time()
    total_duration = end_time - start_time
    # Collect metrics
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'total_execution_time': total_duration,
        'input_file': {
            'path': input_csv_path,
            'size': os.path.getsize(input_csv_path)
        },
        'model_file': {
            'path': model_file_path,
            'size': os.path.getsize(model_file_path)
        },
        'model_rmse': rmse
    }

    # Update metrics table
    table_path = 'pipeline_output/pipeline_logs.csv'
    update_metrics_table(metrics, table_path)

    print(f"Pipeline duration: {total_duration:.2f} seconds")
    print(f"Model RMSE: {rmse:.4f}")
    print(f"Metrics added to: {table_path}")

    # Run online evaluation if specified
    if run_online_eval:
        print("Executing Online Evaluation")
        if not online_evaluation_csv:
            print("Online evaluation CSV file not provided.")
            return
        online_rmse = online_eval(movie_runtime_map, model_file_path, online_evaluation_csv)
        print(f"Online Evaluation RMSE: {online_rmse:.4f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the pipeline with specified input and output paths.")
    parser.add_argument("--input_csv", help="Path to the input CSV file",default="pipeline_input_data/kafka_stream.csv")
    parser.add_argument("--movie_info", help="Path to the input csv containing movies info",default="pipeline_input_data/movie_runtime_map.json")
    parser.add_argument("--model_file", help="Path where the model file will be saved",default="pipeline_output/nachi_model.pkl")
    parser.add_argument("--run_online_eval", action="store_true", help="Flag to run online evaluation")
    parser.add_argument("--online_evaluation_csv",
                        help="Path to the online evaluation CSV file (if running online evaluation)", default=None)


    args = parser.parse_args()

    main(args.input_csv, args.movie_info, args.model_file, args.run_online_eval, args.online_evaluation_csv)
