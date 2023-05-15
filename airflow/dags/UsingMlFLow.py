#Import the libraries
import numpy as np
import pandas as pd

df = pd.read_csv('https://publicapi.traffy.in.th/dump-csv-chadchart/bangkok_traffy.csv')
print(df.columns)

#import required modules
import mlflow
from sklearn.metrics import silhouette_score
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
#create dataset
training_data=[(float(df['coords'][i].split(',')[0]),float(df['coords'][i].split(',')[1])) for i in range(len(df['coords']))]
training_data = training_data[:2000]
#create a new experiment
experiment_name = 'ClusteringWithMlflow'
try:
    exp_id = mlflow.create_experiment(name=experiment_name)
except Exception as e:
    exp_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
#run the code for different number of clusters
range_of_k = range(2,10) 
for k in range_of_k :
    with mlflow.start_run(experiment_id=exp_id):
        untrained_model = KMeans(n_clusters=k)
        trained_model=untrained_model.fit(training_data)
        cluster_labels = trained_model.labels_
        score=silhouette_score(training_data, cluster_labels)
        #save parameter
        mlflow.log_param('value_of_k', k)
        #save metric
        mlflow.log_metric('silhoutte_score', score)
                
        # ... code to perform clustering and compute silhouette score ...

        # plot clustered data with different colors for each cluster
        fig, ax = plt.subplots()
        scatter = ax.scatter([p[0] for p in training_data], [p[1] for p in training_data], c=cluster_labels)

        # add legend and labels
        legend = ax.legend(*scatter.legend_elements(), title="Cluster")
        ax.add_artist(legend)
        ax.set_xlabel('Latitude')
        ax.set_ylabel('Longitude')
        ax.set_title(f'Clustered Data (k={k}, silhouette score={score:.2f})')

        # save plot as artifact of MLflow run
        mlflow.log_figure(fig, "clustered_data.png")
        #save model
        mlflow.sklearn.log_model(trained_model, "Clustering_Model")
        #end current run
        mlflow.end_run()