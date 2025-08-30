import pandas as pd

# Load the processed data
# Load the processed data
def aggregate_ratings_minutes(input_csv_path,output_csv_path):
    data = pd.read_csv(input_csv_path)

    # Convert relevant fields
    data['Minutes Watched'] = pd.to_numeric(data['Minutes Watched'], errors='coerce').fillna(0)
    data['Rating'] = pd.to_numeric(data['Rating'], errors='coerce')

    # Group by User ID and Movie ID to calculate total minutes watched and average rating
    grouped_data = data.groupby(['User ID', 'Movie ID']).agg(
        Total_Minutes_Watched=('Minutes Watched', 'max'),  # Max of minutes watched
        Average_Rating=('Rating', lambda x: x.mean() if not x.isnull().all() else None)  # Mean rating or NaN if all are NaN
    ).reset_index()

    # Figure out normalization of
    # Save or print the result
    grouped_data.to_csv(output_csv_path, index=False)