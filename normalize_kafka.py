import pandas as pd
import os
import json
import re
import random

# Load user dataset
#user_data = pd.read_csv('implicit_movie_feedback.csv')

# Function to clean and format the string to valid JSON
def clean_and_format_json(line):
    # Add double quotes around keys
    cleaned_line = re.sub(r"(\w+):", r'"\1":', line)  # Add quotes around keys
    # Add double quotes around values if they are not properly quoted (except true, false, null)
    cleaned_line = re.sub(r"(?<!\")(\b\w+\b)(?!\")", r'"\1"', cleaned_line)  # Add quotes around some values
    return cleaned_line

# Function to safely parse cleaned JSON lines from the movie info CSV
def clean_and_parse_json_from_csv_line(line):
    try:
        cleaned_line = clean_and_format_json(line.strip())  # Clean the line
        return json.loads(cleaned_line)  # Load as JSON
    except json.JSONDecodeError as e:
        print(f"Unable to parse JSON: {line}\nError: {e}")
        return None


#print(movie_runtime_map)

# Function to normalize minutes watched based on movie runtime
def normalize_minutes_watched(row,movie_runtime_map):

    movie_id = row['Movie ID']
    minutes_watched = row['Total_Minutes_Watched']

    # Check if the movie's runtime exists in the dictionary
    if movie_id in movie_runtime_map:
        runtime = movie_runtime_map[movie_id]
        if runtime > 0:  # Avoid division by zero
            return round(minutes_watched / runtime,2)
    # Return a random float between 0 and 1 if movie ID is not found or runtime is 0
    return round(random.uniform(0, 1), 2)

# Apply the normalization function to the Minutes Watched column
def normalize_minutes(kafka_input_path,movie_runtime_map_path,kafka_output_path):
# Load movie info CSV and parse each line as JSON
    with open(movie_runtime_map_path, 'r') as f:
        movie_runtime_map = json.load(f)

    user_data = pd.read_csv(kafka_input_path)
    #user_data['Normalized Minutes Watched'] = user_data.apply(normalize_minutes_watched, axis=1)
# Apply normalization
    user_data['Normalized Minutes Watched'] = user_data.apply(lambda row: normalize_minutes_watched(row, movie_runtime_map), axis=1)

    # Save or print the result
    user_data.to_csv(kafka_output_path, index=False)

import json

def create_movie_runtime_map(movie_info_path, output_path):
    # Load existing movie_runtime_map if the JSON file already exists
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as f:
            movie_runtime_map = json.load(f)
    else:
        movie_runtime_map = {}

    # Process the movie_info file to add new movies to the runtime map
    with open(movie_info_path, 'r', encoding='utf-8') as file:
        next(file)  # Skip header if there's one
        for line in file:
            line_split = line.strip().split(",")
            if len(line_split) < 2:
                continue
            movie_id = line_split[0]
            try:
                runtime = int(line_split[1])
                # Add to movie_runtime_map only if the movie_id is not already present
                if movie_id not in movie_runtime_map:
                    movie_runtime_map[movie_id] = runtime
            except ValueError:
                print(f"Invalid runtime for movie ID: {movie_id}")

    # Save the updated movie_runtime_map to the JSON file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(movie_runtime_map, f, indent=4)

    print(f"Movie runtime map created and saved to {output_path}")
    return movie_runtime_map