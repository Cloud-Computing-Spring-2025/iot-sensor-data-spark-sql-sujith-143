
```markdown
# IoT Sensor Data Analysis with PySpark

This project involves the analysis of IoT sensor data using **Apache Spark** (PySpark). The dataset contains sensor readings, including temperature, humidity, and timestamp information. The tasks focus on loading, processing, filtering, aggregating, and analyzing the data with Spark SQL, Window functions, and Pivot operations.

## Project Tasks

### Task 1: Load & Basic Exploration
- **Load the CSV**: Read the sensor data from `sensor_data.csv` into a DataFrame with inferred or specified schema.
- **Basic Exploration**:
  - Show the first 5 rows of the dataset.
  - Count the total number of records.
  - Retrieve distinct locations from the data.
- **Output**: Save the DataFrame (or key query result) as `task1_output.csv`.

### Task 2: Filtering & Simple Aggregations
- **Filtering**: Filter out rows where the temperature is below 18°C or above 30°C.
  - Count how many rows are in-range vs. out-of-range.
- **Aggregations**: 
  - Group by location and compute the average temperature and average humidity.
  - Sort the results by average temperature in descending order to find the hottest location.
- **Output**: Save the aggregated results to `task2_output.csv`.

### Task 3: Time-Based Analysis
- **Timestamp Conversion**: Convert the timestamp column into a proper timestamp format.
- **Group by Hour**: Extract the hour of the day and compute the average temperature for each hour of the day.
- **Output**: Save the results of hourly average temperatures to `task3_output.csv`.

### Task 4: Window Function
- **Rank Sensors by Average Temperature**:
  - Compute the average temperature for each sensor.
  - Rank the sensors by their average temperature using a window function (ranking in descending order).
- **Output**: Save the ranked sensors to `task4_output.csv`.

### Task 5: Pivot & Interpretation
- **Pivot by Hour and Location**:
  - Compute the average temperature for each location and hour of the day.
  - Pivot the data so that locations are rows, hours are columns, and the average temperature is the value in each cell.
- **Output**: Save the pivoted DataFrame to `task5_output.csv`.

## Requirements

- **Python 3.x** (tested with version 3.12)
- **Apache Spark** (PySpark 3.x)


Install required libraries with:
```bash
pip install pyspark
```

## Running the Code

1. Ensure that the `sensor_data.csv` file is in the same directory as the script or provide the full path to the file in the script.
2. Save the script as `iot_sensor.py`.
3. Run the script using Python:
   ```bash
   python iot_sensor.py
   ```
4. The output CSV files will be saved in the current working directory:
   - `task1_output.csv`
   - `task2_output.csv`
   - `task3_output.csv`
   - `task4_output.csv`
   - `task5_output.csv`

## Explanation of the Code

### Task 1: Loading & Basic Exploration
The dataset is read using `spark.read.csv()`, and basic queries like displaying the first 5 rows, counting the records, and retrieving distinct locations are executed. The DataFrame is saved as `task1_output.csv`.

### Task 2: Filtering & Aggregations
Rows with temperatures outside the range of 18°C to 30°C are filtered out. The remaining data is aggregated by location to compute average temperature and humidity. The results are saved as `task2_output.csv`.

### Task 3: Time-Based Analysis
The timestamp string is converted into a proper timestamp format using `to_timestamp()`. The average temperature for each hour is computed and saved as `task3_output.csv`.

### Task 4: Window Function
A window function is used to rank sensors by their average temperature. The sensors are ordered by their average temperature in descending order, and the top-ranked sensors are saved as `task4_output.csv`.

### Task 5: Pivot & Interpretation
The data is pivoted by location and hour of the day to compute the average temperature for each combination. The pivoted data is saved as `task5_output.csv`.
