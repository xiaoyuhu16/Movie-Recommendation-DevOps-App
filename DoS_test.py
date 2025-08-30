import os
import uuid
from kafka import KafkaConsumer

KAFKA_IP = "128.2.204.215"
topic = 'movielog10'

data_folder = "service_test"
max_data = 1000
threshold = 0.5  # threshold for unusual recommendation request rate

def collect_data_into_csv():
    """
    Collects data from the Kafka topic and writes up to max_data messages into a new CSV file.
    The file is named with a unique UUID to avoid collisions.
    Returns the path to the newly created data file.
    """
    unique_id = uuid.uuid4()
    data_file_name = os.path.join(data_folder, f"data_{unique_id}.csv")
    os.makedirs(data_folder, exist_ok=True)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

    print('Reading Kafka Broker...')
    count = 0
    with open(data_file_name, "w") as data_file:
        for message in consumer:
            count += 1
            msg_str = message.value.decode()
            data_file.write(msg_str + "\n")
            if count >= max_data:
                break

    print(f"Collected {count} messages into {data_file_name}")
    return data_file_name

def detect_unusual_recommendation_rate(data_file_path, threshold=0.5):
    """
    Reads the data file and checks the fraction of lines that contain "recommendation request".
    If the fraction exceeds 'threshold', we consider it unusual.
    
    :param data_file_path: The path to the CSV file created by collect_data_into_csv
    :param threshold: A float representing the fraction threshold.
    :return: (is_unusual, rec_request_count, total_lines)
    """
    rec_request_count = 0
    total_lines = 0

    with open(data_file_path, "r") as f:
        for line in f:
            total_lines += 1
            if "recommendation request" in line:
                rec_request_count += 1

    fraction = rec_request_count / total_lines if total_lines > 0 else 0
    is_unusual = (fraction > threshold)
    return is_unusual, rec_request_count, total_lines

if __name__ == "__main__":
    # Step 1: Collect data
    data_file_path = collect_data_into_csv()

    # Step 2: Analyze the collected data
    unusual, count, total = detect_unusual_recommendation_rate(data_file_path, threshold=threshold)

    # Step 3: Print results
    if unusual:
        print(f"Unusual rate of recommendation requests detected! {count} out of {total} lines (>{threshold*100}% ).")
    else:
        print(f"Recommendation request rate is normal. {count} out of {total} lines.")