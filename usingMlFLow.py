#Import the libraries
import numpy as np
import pandas as pd
import datetime
import joblib
import csv

df = pd.read_csv("./airflow/data/cleaned.csv")
# print(df.head(5))
print(len(df))

#import required modules
import mlflow
from sklearn.metrics import silhouette_score
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
#create dataset
# training_data=[(float(df['coords'][i][2:-2].split("', '")[1]),float(df['coords'][i][2:-2].split("', '")[0])) for i in range(len(df['coords']))]
training_data = list(zip(df['latitude'], df['longitude']))

#create a new experiment
now = datetime.datetime.now()
experiment_name = 'ClusteringWithMlflow at {}'.format(now)
try:
    exp_id = mlflow.create_experiment(name=experiment_name)
except Exception as e:
    exp_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    mlflow.delete_experiment(exp_id)
    exp_id = mlflow.create_experiment(name=experiment_name)

#run the code for different number of clusters
range_of_k = range(20,80,5)
m = 1.01
# range_of_k = [30,50]
silhouette_scores = []

for k in range_of_k :
    with mlflow.start_run(experiment_id=exp_id, run_name=f"Run with k = {k}"):
        untrained_model = KMeans(n_clusters=k)
        trained_model=untrained_model.fit(training_data)
        cluster_labels = trained_model.labels_
        score=silhouette_score(training_data, cluster_labels)
        #save parameter
        mlflow.log_param('value_of_k', k)
        #save metric
        mlflow.log_metric('silhoutte_score', score)
        print('run with k =',k,'score =',score)
        silhouette_scores.append(score)
       
        # ... code to perform clustering and compute silhouette score ...

        # plot clustered data with different colors for each cluster
        fig, ax = plt.subplots()
        scatter = ax.scatter([p[0] for p in training_data], [p[1] for p in training_data], c=cluster_labels)

        # add legend and labels
        legend = ax.legend(*scatter.legend_elements(), title="Cluster")
        ax.add_artist(legend)
        ax.set_xlabel('Latitude')
        ax.set_ylabel('Longitude')
        ax.set_title(f'Clustered Data (k={k}, silhouette score={score:.5f})')

        # save plot as artifact of MLflow run
        mlflow.log_figure(fig, "clustered_data.png")
        #save model
        mlflow.sklearn.log_model(trained_model, "Clustering_Model")
        #end current run
        if k>30 and silhouette_scores[-1] < silhouette_scores[-2]*m and silhouette_scores[-2] < silhouette_scores[-3]*m:
            break
        mlflow.end_run()

# Get the experiment ID
experiment = mlflow.get_experiment_by_name(experiment_name)
exp_id = experiment.experiment_id
print(exp_id)
# Get all runs in the experiment
runs = mlflow.search_runs(experiment_ids=[exp_id])

# Sort runs by silhouette score in descending order
sorted_runs = runs.sort_values(by='metrics.silhoutte_score', ascending=False)

# Retrieve the best run and its details
best_k = int(sorted_runs['params.value_of_k'].iloc[0])
best_score = sorted_runs['metrics.silhoutte_score'].iloc[0]

# Print the best run details
runid = sorted_runs['run_id'].iloc[0]
print('runid:',runid)
print(f"Best Run - k: {best_k}, Silhouette Score: {best_score}")

model = joblib.load('./mlruns/{}/{}/artifacts/Clustering_Model/model.pkl'.format(exp_id,runid))
print(min(model.labels_))
print(max(model.labels_))

df['labels'] = model.labels_
centroid = pd.DataFrame(data = model.cluster_centers_, columns= ['latitude','longitude'])
index = [i for i in range(best_k)]
centroid['Cluster'] = index
centroid = centroid.reindex(columns=['Cluster', 'latitude', 'longitude'])
centroid.to_csv('./airflow/data/centroid.csv', index=False)
df.to_csv('./airflow/data/labeled.csv', index=False)