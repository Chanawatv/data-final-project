import pandas as pd

df = pd.read_csv("./airflow/dags/src/data/raw.csv")

# Drop duplicates
columnToCheck = [col for col in df.columns if col!='timestamp']
print(columnToCheck)
print(len(df))
df = df.drop_duplicates(subset=columnToCheck)
print(len(df))

# Create a new column 'latitude' in the DataFrame
df['latitude'] = [float(coord[2:-2].split("', '")[1]) for coord in df['coords']]

# Create a new column 'longitude' in the DataFrame
df['longitude'] = [float(coord[2:-2].split("', '")[0]) for coord in df['coords']]
# print(df.info())


#Bangkok
# Define the latitude and longitude range
min_latitude = 13.48
max_latitude = 13.96
min_longitude = 100.32
max_longitude = 100.94

# Create a boolean mask based on the condition
mask = (
    (df['latitude'] >= min_latitude) & (df['latitude'] <= max_latitude) &
    (df['longitude'] >= min_longitude) & (df['longitude'] <= max_longitude)
)

# Drop the rows that do not satisfy the condition
df = df[mask]

# Reset the index if desired
df = df.reset_index(drop=True)

# print(df.info())
print(df.shape)  # Output: (num_rows, num_columns)

df.to_csv('./airflow/dags/src/data/rawOutlier.csv', index=False)

