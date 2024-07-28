import pandas as pd
import re
import os

# Function to parse the correlation data
def parse_metric_data(metric_data):
    lines = metric_data.strip().split('\n')
    metric_name = lines[0].strip()
    
    # Initialize the variables to hold the correlation values
    pearson_coeff = spearman_coeff = kendall_coeff = None
    
    # Extract the individual coefficients
    for line in lines[2:]:
        if 'Pearson coefficients:' in line:
            pearson_coeff = line.split(':')[1].strip()
        elif 'Spearman coefficients:' in line:
            spearman_coeff = line.split(':')[1].strip()
        elif 'Kendall coefficients:' in line:
            kendall_coeff = line.split(':')[1].strip()
    
    return {
        'Metric': metric_name,
        'Pearson Coefficients': pearson_coeff,
        'Spearman Coefficients': spearman_coeff,
        'Kendall Coefficients': kendall_coeff
    }

# Define the paths
input_file_path = './correlations/correlations.txt'
output_file_path = './Task2/task2_organized.xlsx'

# Ensure the output directory exists
os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

# Read the input file
with open(input_file_path, 'r') as file:
    data = file.read()

# Split data into metric sections
metrics_data = re.split(r'Correlation coefficients for metric ', data)[1:]

# Parse data for each metric
parsed_data = [parse_metric_data(metric_data) for metric_data in metrics_data]

# Extract metrics and their coefficients
metrics = [data['Metric'] for data in parsed_data]
pearson_coefficients = [data['Pearson Coefficients'] for data in parsed_data]
spearman_coefficients = [data['Spearman Coefficients'] for data in parsed_data]
kendall_coefficients = [data['Kendall Coefficients'] for data in parsed_data]

# Create a DataFrame in the specified format
output_data = {
    'Metrics': [', '.join(metrics)],
    'Pearson Coefficients': pearson_coefficients,
    'Spearman Coefficients': spearman_coefficients,
    'Kendall Coefficients': kendall_coefficients
}

df_metrics = pd.DataFrame(output_data['Metrics'], columns=['Metrics'])
df_pearson = pd.DataFrame(output_data['Pearson Coefficients'], columns=['Pearson Coefficients'])
df_spearman = pd.DataFrame(output_data['Spearman Coefficients'], columns=['Spearman Coefficients'])
df_kendall = pd.DataFrame(output_data['Kendall Coefficients'], columns=['Kendall Coefficients'])

# Concatenate DataFrames to get the final format
final_df = pd.concat([df_metrics, df_pearson, df_spearman, df_kendall], axis=0, ignore_index=True)

# Write the DataFrame to an Excel file using openpyxl
final_df.to_excel(output_file_path, index=False, header=False, engine='openpyxl')

print(f"Data has been organized and written to {output_file_path}")
