import pandas as pd


# Define a function to extract fields from the request column
def extract_fields(row):
    if 'GET /rate/' in row['Request']:
        # Handling rating entries
        parts = row['Request'].split('/')
        movie_id = parts[-1].split('=')[0]
        rating = parts[-1].split('=')[1]
        minutes_watched = None
    elif 'GET /data/m/' in row['Request']:
        # Handling movie watching entries
        parts = row['Request'].split('/')
        movie_id = '+'.join(parts[3].split('+')[:])  # Exclude the minute part
        minutes_watched = parts[-1].split('.')[0]  # Remove '.mpg' and get minute
        rating = None
    else:
        movie_id, minutes_watched, rating = None, None, None

    return pd.Series([movie_id, minutes_watched, rating])

# Apply the function to each row
def process_kafka(input_csv_path, output_csv_path):
    # Load data, splitting on commas
    # Input csv, output processed csv
    try:
        # Define column names
        column_names = ['Timestamp', 'User ID', 'Request']

        # Read the CSV, skipping bad lines and selecting only the first three columns
        data = pd.read_csv(
            input_csv_path,
            header=None,
            dtype=str,
            usecols=[0, 1, 2],  # Read only the first three columns
            names=column_names,  # Assign column names
            on_bad_lines='skip',  # Skip lines with incorrect number of columns
            skip_blank_lines=True
        )

        # Filter only the rows with valid requests
        data = data[data['Request'].apply(is_valid_request)]


        # Extract fields using the extract_fields function
        extracted_fields = data.apply(extract_fields, axis=1)
        extracted_fields.columns = ['Movie ID', 'Minutes Watched', 'Rating']

        processed_data = pd.concat([data, extracted_fields], axis=1)

        # Drop the 'Request' column
        processed_data.drop('Request', axis=1, inplace=True)

        # Save the processed data to the output CSV file
        processed_data.to_csv(output_csv_path, index=False)
        print(f"Processed data saved to {output_csv_path}")

    except pd.errors.EmptyDataError:
        print("The input CSV file is empty.")
    except FileNotFoundError:
        print(f"The file {input_csv_path} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")


# Helper function to validate request type
def is_valid_request(request):
    """ Check if a request contains valid movie-watching or rating activity. """
    return 'GET /data/m/' in request or 'GET /rate/' in request

