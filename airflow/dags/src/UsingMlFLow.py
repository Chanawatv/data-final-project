#Import the libraries
import numpy as np
import pandas as pd

df = pd.read_csv("./airflow/dags/src/data/rawOutlier.csv")
# print(df.head(5))
print(len(df['coords']))

#import required modules
import mlflow
from sklearn.metrics import silhouette_score
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
#create dataset
# training_data=[(float(df['coords'][i][2:-2].split("', '")[1]),float(df['coords'][i][2:-2].split("', '")[0])) for i in range(len(df['coords']))]
training_data = list(zip(df['latitude'], df['longitude']))

# training_data = training_data[:2000]
#create a new experiment
experiment_name = 'ClusteringWithMlflow'
try:
    exp_id = mlflow.create_experiment(name=experiment_name)
except Exception as e:
    exp_id = mlflow.get_experiment_by_name(experiment_name).experiment_id


#run the code for different number of clusters
range_of_k = range(30,76) 
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
        mlflow.end_run()



# Plot the relationship between k and silhouette score
# plt.plot(range_of_k, silhouette_scores, marker='o')
# plt.xlabel('k')
# plt.ylabel('Silhouette Score')
# plt.title('Silhouette Score vs. k')
# plt.grid(True)
# plt.show()

# Get the experiment ID
experiment = mlflow.get_experiment_by_name(experiment_name)
exp_id = experiment.experiment_id

# Get all runs in the experiment
runs = mlflow.search_runs(experiment_ids=[exp_id])

# Sort runs by silhouette score in descending order
sorted_runs = runs.sort_values(by='metrics.silhoutte_score', ascending=False)

# Retrieve the best run and its details
best_k = sorted_runs['params.value_of_k'].iloc[0]
best_score = sorted_runs['metrics.silhoutte_score'].iloc[0]

# Print the best run details
print(f"Best Run - k: {best_k}, Silhouette Score: {best_score}")

