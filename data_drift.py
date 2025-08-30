import pandas as pd
import json
import argparse
from read_kafka import extract_fields

def detect_data_drift(online_data_csv, historical_data_csv, movie_info, output_csv, threshold):
    print("Running data drift detection script")

    # Load data
    historic_data = pd.read_csv(historical_data_csv, header=None, names=['Timestamp', 'User ID', 'Request'], on_bad_lines="skip")
    historic_data[['Movie ID', 'Minutes Watched', 'Rating']] = historic_data.apply(extract_fields, axis=1)
    historic_data['Minutes Watched'] = pd.to_numeric(historic_data['Minutes Watched'], errors='coerce')
    
    online_data = pd.read_csv(online_data_csv, header=None, names=['Timestamp', 'User ID', 'Request'], on_bad_lines="skip")
    # online_data = online_data[:173227]
    online_data[['Movie ID', 'Minutes Watched', 'Rating']] = online_data.apply(extract_fields, axis=1)
    online_data['Minutes Watched'] = pd.to_numeric(online_data['Minutes Watched'], errors='coerce')

    # Load movie info CSV and parse each line as JSON
    with open(movie_info, 'r') as f:
        movie_data = json.load(f)

    # Add runtime column by mapping Movie ID to runtime from movie_data
    historic_data['Runtime'] = historic_data['Movie ID'].map(movie_data)
    online_data['Runtime'] = online_data['Movie ID'].map(movie_data)

    # Calculate the average runtime of movies watched in both datasets
    historical_avg_runtime = historic_data['Runtime'].dropna().mean()
    online_avg_runtime = online_data['Runtime'].dropna().mean()

    # Calculate average "Minutes Watched" for both datasets
    historical_avg_minutes_watched = historic_data['Minutes Watched'].mean()
    online_avg_minutes_watched = online_data['Minutes Watched'].mean()

    print(f"Average runtime of movies watched - Historical Data: {historical_avg_runtime} minutes")
    print(f"Average runtime of movies watched - Online Data: {online_avg_runtime} minutes")
    print(f"Average 'Minutes Watched' - Historical Data: {historical_avg_minutes_watched}")
    print(f"Average 'Minutes Watched' - Online Data: {online_avg_minutes_watched}")
    
    # Calculate the drift ratios
    runtime_drift_ratio = (online_avg_runtime / historical_avg_runtime) if historical_avg_runtime != 0 else None
    minutes_watched_drift_ratio = (online_avg_minutes_watched / historical_avg_minutes_watched) if historical_avg_minutes_watched != 0 else None

    print(f"Runtime Drift Ratio: {runtime_drift_ratio}")
    print(f"Minutes Watched Drift Ratio: {minutes_watched_drift_ratio}")

    # Save results to CSV if drift ratio exceeds threshold
    if (runtime_drift_ratio and abs(runtime_drift_ratio - 1) >= threshold - 1) or \
       (minutes_watched_drift_ratio and abs(minutes_watched_drift_ratio - 1) >= threshold - 1):
        drift_data = pd.DataFrame({
            "Metric": [
                "Average Runtime of Movies Watched", "Ratio of Average Runtime",
                "Average Minutes Watched", "Ratio of Minutes Watched"
            ],
            "Historical Data": [
                historical_avg_runtime, None,
                historical_avg_minutes_watched, None
            ],
            "Online Data": [
                online_avg_runtime, runtime_drift_ratio,
                online_avg_minutes_watched, minutes_watched_drift_ratio
            ]
        })
        
        drift_data.to_csv(output_csv, index=False)
        print(f"Data drift detected and saved to {output_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data drift detection with specified input and output paths.")
    parser.add_argument("--online_data_csv", help="Path to the new data CSV file",default="data/online_evaluation_kafka_stream.csv")
    parser.add_argument("--historical_data_csv", help="Path to the histric data CSV file",default="pipeline_input_data/kafka_stream.csv")
    parser.add_argument("--movie_info", help="Path to the input csv containing movies info",default="pipeline_input_data/movie_runtime_map.json")
    parser.add_argument("--output_csv", help="Path where the data drift will be sent",default="data_drift/data_drift.csv")
    parser.add_argument("--threshold",  help="Threshold for drift ratio detection (default is 1.3 for 30% change)", default=1.3, type=float)
    args = parser.parse_args()

    detect_data_drift(args.online_data_csv, args.historical_data_csv, args.movie_info, args.output_csv, args.threshold)