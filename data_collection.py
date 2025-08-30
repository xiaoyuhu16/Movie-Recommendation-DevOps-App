import os
import requests
from kafka import KafkaConsumer
import uuid

# Generate a random UUID
unique_id = uuid.uuid4()

KAFKA_IP = "128.2.204.215"
topic = 'movielog10'
data_folder = "service_test"
data_file_name = os.path.join(data_folder, f"data_{unique_id}.csv")
movieInfo_file_name = os.path.join(data_folder, f"movie_info_{unique_id}.csv")
max_data = 50000

def collect_data_for_training_into_csv():
    # Ensure the data folder exists
    os.makedirs(data_folder, exist_ok=True)

    with open(data_file_name, "w") as data_file:
        data_file.write("")  # Clears the file

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

    print('Reading Kafka Broker')
    message_count = 0  # Initialize counter
    collected_movie_ids = set()  # Keep track of processed movie IDs

    for message in consumer:
        message = message.value.decode()

        # data quality checks
        message_parts = message.split(",")
        if len(message_parts) != 3:
            continue
        
        # Increment the message count
        message_count += 1
        request = message_parts[2].split("/")
        movie_id = ''

        # this form of request: <time>,<userid>,GET /data/m/<movieid>/<minute>.mpg
        if len(request) >= 5 and request[3]:
            movie_id = request[3]

        # this form of request: <time>,<userid>,GET /rate/<movieid>=<rating>
        elif len(request) == 3 and "=" in request[2]:
            movie_id, rating = request[2].split("=")

        with open(data_file_name, "a") as data_file:
            data_file.write(message + "\n")

        # If movie_id is found and has not been recorded before, fetch and save movie runtime
        if movie_id and movie_id not in collected_movie_ids:
            movie_info_req = requests.get(f"http://{KAFKA_IP}:8080/movie/{movie_id}")
            if movie_info_req.status_code == 200:
                runtime = movie_info_req.json().get("runtime")
                if runtime:
                    with open(movieInfo_file_name, "a") as movie_info_file:
                        movie_info_file.write(f"{movie_id},{runtime}\n")
                    collected_movie_ids.add(movie_id)  # Mark movie ID as processed

        # Check if the file has reached the maximum number of rows
        if message_count >= max_data:
            with open("latest_data.txt", "w") as txt_file:
                txt_file.write(data_file_name)
            break  # Stop collecting data
