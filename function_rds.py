import boto3
from datetime import datetime, timedelta, timezone
import function_pyplot

def get_rds_clusters(rds_client, clusters=None):
    """
    Fetch the list of RDS clusters. If a list of cluster IDs is provided, 
    only return the clusters that match the specified IDs.

    :param rds_client: Boto3 RDS client
    :param clusters: List of cluster IDs to filter (default: None, fetch all clusters)
    :return: List of cluster IDs
    """
    # Fetch the list of all RDS clusters
    response = rds_client.describe_db_clusters()
    all_clusters = response.get('DBClusters', [])

    # Convert the cluster details into a list of cluster IDs
    all_cluster_ids = [cluster['DBClusterIdentifier'] for cluster in all_clusters]

    # If clusters parameter is provided, filter the list
    if clusters:
        # Only include clusters that exist in the fetched list
        filtered_clusters = [cluster_id for cluster_id in clusters if cluster_id in all_cluster_ids]
        return filtered_clusters

    # If no clusters parameter is provided, return all cluster IDs
    return all_cluster_ids

def get_instances_in_cluster(rds_client, cluster_identifier):
    # Fetch the list of instances in the specified cluster
    response = rds_client.describe_db_instances()
    instances = response.get('DBInstances', [])
    cluster_instances = [
        instance['DBInstanceIdentifier']
        for instance in instances
        if instance.get('DBClusterIdentifier') == cluster_identifier
    ]
    return cluster_instances

def get_rds_metrics(cloudwatch_client, dimension, dimension_value, namespace="AWS/RDS", metric_name="CPUUtilization", 
                         period=300, statistics=['Average'], extended_statistics=[], start_time=None, end_time=None):
    """
    Fetch RDS metrics from CloudWatch.

    :param cloudwatch_client: Boto3 CloudWatch client
    :param instance: The RDS instance identifier
    :param namespace: CloudWatch namespace (default: AWS/RDS)
    :param metric_name: Metric name (default: CPUUtilization)
    :param period: Period in seconds (default: 300)
    :param statistics: Statistics type (default: Average)
    :param extended_statistics: Extended statistics (default: empty list)
    :param start_time: Start time for metrics (default: 1 month ago)
    :param end_time: End time for metrics (default: now)
    :return: Metrics data
    """

    if not start_time:
        start_time = datetime.now() - timedelta(days=30)
    if not end_time:
        end_time = datetime.now()

    max_datapoints = 1440
    total_seconds = (end_time - start_time).total_seconds()
    datapoints_requested = total_seconds // period

    tokyo_timezone = timezone(timedelta(hours=9))

    if datapoints_requested <= max_datapoints:
        datapoints = []
        extended_datapoints = []
        response = cloudwatch_client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=[
                {
                    'Name': dimension,
                    'Value': dimension_value
                }
            ],
            Period=period,
            StartTime=start_time,
            EndTime=end_time,
            Statistics=statistics
        )
        datapoints.extend(response.get('Datapoints', []))
        # Add extended statistics if requested
        if extended_statistics:
            response = cloudwatch_client.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=[
                    {
                        'Name': dimension,
                        'Value': dimension_value
                    }
                ],
                Period=period,
                StartTime=start_time,
                EndTime=end_time,
                ExtendedStatistics=extended_statistics
            )
            extended_datapoints.extend(response.get('Datapoints', []))
        
        # Convert timestamps to Tokyo timezone
        for dp in datapoints:
            dp['Timestamp'] = dp['Timestamp'].astimezone(tokyo_timezone)
        for dp in extended_datapoints:
            dp['Timestamp'] = dp['Timestamp'].astimezone(tokyo_timezone)
        return sorted(datapoints, key=lambda x: x['Timestamp']), sorted(extended_datapoints, key=lambda x: x['Timestamp'])
    
    # Split the time range if datapoints exceed the limit
    datapoints = []
    extended_datapoints = []
    split_duration = timedelta(seconds=period * max_datapoints)
    current_start_time = start_time

    while current_start_time < end_time:
        current_end_time = min(current_start_time + split_duration, end_time)
        # print(f"Fetching metrics from {current_start_time} to {current_end_time}")
        # Ensure we don't exceed the max datapoints
        response = cloudwatch_client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=[
                {
                    'Name': dimension,
                    'Value': dimension_value
                }
            ],
            Period=period,
            StartTime=current_start_time,
            EndTime=current_end_time,
            Statistics=statistics
        )
        datapoints.extend(response.get('Datapoints', []))
        # Add extended statistics if requested
        if extended_statistics:
            response = cloudwatch_client.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=[
                    {
                        'Name': dimension,
                    'Value': dimension_value
                    }
                ],
                Period=period,
                StartTime=current_start_time,
                EndTime=current_end_time,
                ExtendedStatistics=extended_statistics
            )
            extended_datapoints.extend(response.get('Datapoints', []))

        current_start_time = current_end_time

    # Convert timestamps to Tokyo timezone
    for dp in datapoints:
        dp['Timestamp'] = dp['Timestamp'].astimezone(tokyo_timezone)
    for dp in extended_datapoints:
        dp['Timestamp'] = dp['Timestamp'].astimezone(tokyo_timezone)
    return sorted(datapoints, key=lambda x: x['Timestamp']), sorted(extended_datapoints, key=lambda x: x['Timestamp'])


def generate_instance_level_metrics_graphics_for_cluster(cluster, instances, cloudwatch_client, metric_name, 
                                      period,
                                      statistics, extended_statistics,
                                      start_time, end_time,
                                      is_show, is_save
                                      ):
    print(f"Processing {metric_name}")        
    # Initialize dictionaries to store metrics for all instances under the same cluster
    all_metrics = {}
    all_extended_metrics = {}
    
    # Loop through each instance in the cluster and fetch metrics
    for instance in instances:
        # print(f"Processing instance {instance} in cluster {cluster}")
        metrics, extended_metrics = get_rds_metrics(
            cloudwatch_client, "DBInstanceIdentifier", instance,
            metric_name=metric_name,
            period=period,
            statistics=statistics,
            extended_statistics=extended_statistics,
            start_time=start_time,
            end_time=end_time
        )
        if len(metrics) == 0 or len(extended_metrics) == 0:
            print(f"No metrics found for instance {instance} in cluster {cluster}")
            continue
        # print(f"Metrics for instance {instance}:")
        # print(f"{metrics}")
        # print(f"{extended_metrics}")
        # Store metrics for the instance
        all_metrics[instance] = metrics
        all_extended_metrics[instance] = extended_metrics

    # Generate metrics graphics for all instances in the same single cluster
    if len(all_metrics) == 0 or len(all_extended_metrics) == 0:
        print(f"No metrics found for any instances in cluster {cluster}")
        return
    
    function_pyplot.cluster_graphical_metrics_plot(
        metric_name, cluster, instances,
        all_metrics, statistics,
        all_extended_metrics, extended_statistics,
        is_show, is_save
    )

    function_pyplot.cluster_graphical_metrics_plotly(
        metric_name, cluster, instances,
        all_metrics, statistics,
        all_extended_metrics, extended_statistics,
        is_show, is_save
    )

def generate_cluster_level_metrics_graphics_for_cluster(cluster, cloudwatch_client, metric_name, 
                                      period,
                                      statistics, extended_statistics,
                                      start_time, end_time,
                                      is_show, is_save
                                      ):
    print(f"Processing {metric_name}") 
    all_metrics = {}
    all_extended_metrics = {}     
    
    metrics, extended_metrics = get_rds_metrics(
            cloudwatch_client, "DBClusterIdentifier", cluster,
            metric_name=metric_name,
            period=period,
            statistics=statistics,
            extended_statistics=extended_statistics,
            start_time=start_time,
            end_time=end_time
        )
    if len(metrics) == 0 or len(extended_metrics) == 0:
        print(f"No metrics found for cluster {cluster}")
        return
    # print(f"Metrics for instance {instance}:")
    # print(f"{metrics}")
    # print(f"{extended_metrics}")
    # Store metrics for the instance

    instances = ["NO-INSTANCE"]
    all_metrics["NO-INSTANCE"] = metrics
    all_extended_metrics["NO-INSTANCE"] = extended_metrics
    
    function_pyplot.cluster_graphical_metrics_plot(
        metric_name, cluster, instances,
        all_metrics, statistics,
        all_extended_metrics, extended_statistics,
        is_show, is_save
    )

    function_pyplot.cluster_graphical_metrics_plotly(
        metric_name, cluster, instances,
        all_metrics, statistics,
        all_extended_metrics, extended_statistics,
        is_show, is_save
    )
