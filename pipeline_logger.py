import os
import csv

def update_metrics_table(metrics, table_path):
    fieldnames = ['timestamp', 'total_execution_time', 'input_file_path', 'input_file_size', 'model_file_path', 'model_file_size', 'model_rmse']

    # Check if file exists
    file_exists = os.path.isfile(table_path)

    with open(table_path, mode='a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        # If file doesn't exist, write header
        if not file_exists:
            writer.writeheader()

        # Write the new row
        writer.writerow({
            'timestamp': metrics['timestamp'],
            'total_execution_time': metrics['total_execution_time'],
            'input_file_path': metrics['input_file']['path'],
            'input_file_size': metrics['input_file']['size'],
            'model_file_path': metrics['model_file']['path'],
            'model_file_size': metrics['model_file']['size'],
            'model_rmse': metrics['model_rmse']
        })