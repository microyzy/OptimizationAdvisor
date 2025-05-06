import boto3
from datetime import datetime, timedelta, timezone
import function_pyplot
from statistics import mean
import os  # Add this import for directory handling

SERVICE_TAG_NAME = "Service"

def get_rds_clusters(rds_client, clusters=None, service_tags=None):
    """
    Fetch the list of RDS clusters. If a list of cluster IDs or service tags is provided, 
    only return the clusters that match the specified IDs and/or tags.

    :param rds_client: Boto3 RDS client
    :param clusters: List of cluster IDs to filter (default: None, fetch all clusters)
    :param service_tags: List of service tag values to filter (default: None, fetch all clusters)
    :return: List of dictionaries with cluster ID and tag value of SERVICE_TAG_NAME
    """
    # Fetch the list of all RDS clusters
    response = rds_client.describe_db_clusters()
    all_clusters = response.get('DBClusters', [])

    # Initialize the target cluster list
    target_clusters = []

    for cluster in all_clusters:
        cluster_id = cluster['DBClusterIdentifier']
        # Fetch tags for the cluster
        tags_response = rds_client.list_tags_for_resource(
            ResourceName=cluster['DBClusterArn']
        )
        tags = {tag['Key']: tag['Value'] for tag in tags_response.get('TagList', [])}

        # Check if the cluster matches the provided filters
        if clusters and cluster_id not in clusters:
            continue
        if service_tags and tags.get(SERVICE_TAG_NAME) not in service_tags:
            continue

        # Add the cluster to the target list with its ID and SERVICE_TAG_NAME value
        target_clusters.append({
            'ClusterId': cluster_id,
            'ServiceTag': tags.get(SERVICE_TAG_NAME)
        })

    return target_clusters

def reformat_bytes_metrics(data_point, is_extended=False):
    if data_point['Unit'] != "Bytes":
        return
    
    if is_extended == False:
        data_point['Unit'] = "MB"
        if data_point.get('Average'):
            data_point['Average'] = data_point['Average'] / (1024 * 1024) 
        if data_point.get('Maximum'):
            data_point['Maximum'] = data_point['Maximum'] / (1024 * 1024)
        if data_point.get('Minimum'):
            data_point['Minimum'] = data_point['Minimum'] / (1024 * 1024)
        if data_point.get('Sum'):
            data_point['Sum'] = data_point['Sum'] / (1024 * 1024)
    else:
        data_point['Unit'] = "MB"
        if data_point.get('ExtendedStatistics'):
            if data_point['ExtendedStatistics'].get('p90'):
                data_point['ExtendedStatistics']['p90'] = data_point['ExtendedStatistics']['p90'] / (1024 * 1024)
            if data_point['ExtendedStatistics'].get('p99'):
                data_point['ExtendedStatistics']['p99'] = data_point['ExtendedStatistics']['p99'] / (1024 * 1024)
            if data_point['ExtendedStatistics'].get('p80'):
                data_point['ExtendedStatistics']['p80'] = data_point['ExtendedStatistics']['p80'] / (1024 * 1024)
            if data_point['ExtendedStatistics'].get('p50'):
                data_point['ExtendedStatistics']['p50'] = data_point['ExtendedStatistics']['p50'] / (1024 * 1024)
            

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
            reformat_bytes_metrics(dp, is_extended=False)
        for dp in extended_datapoints:
            dp['Timestamp'] = dp['Timestamp'].astimezone(tokyo_timezone)
            reformat_bytes_metrics(dp, is_extended=True)
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
        reformat_bytes_metrics(dp, is_extended=False)
    for dp in extended_datapoints:
        dp['Timestamp'] = dp['Timestamp'].astimezone(tokyo_timezone)
        reformat_bytes_metrics(dp, is_extended=True)
    return sorted(datapoints, key=lambda x: x['Timestamp']), sorted(extended_datapoints, key=lambda x: x['Timestamp'])


def generate_instance_level_metrics_graphics_for_cluster(cluster, instances, cloudwatch_client, metric_name, 
                                      period,
                                      statistics, extended_statistics,
                                      start_time, end_time,
                                      is_show, is_save,
                                      service_tag=None
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
    
    # aggregate all metrics for the cluster
    aggregate_result = aggregate_cluster_metrics(start_time, end_time, service_tag, cluster, metric_name, all_metrics, all_extended_metrics, statistics, extended_statistics)

    # generate graphics for all instances in the cluster
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

    return aggregate_result

def generate_cluster_level_metrics_graphics_for_cluster(cluster, cloudwatch_client, metric_name, 
                                      period,
                                      statistics, extended_statistics,
                                      start_time, end_time,
                                      is_show, is_save,
                                      service_tag=None
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

    # aggregate all metrics for the cluster
    aggregate_result = aggregate_cluster_metrics(start_time, end_time, service_tag, cluster, metric_name, all_metrics, all_extended_metrics, statistics, extended_statistics)
    
    # generate graphics for all instances in the cluster
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

    return aggregate_result

def aggregate_cluster_metrics(start_time, end_time, service_tag, cluster, metric_name, 
                              all_metrics, all_extended_metrics, statistics, extended_statistics):
    """
    Aggregate metrics for the cluster and save them to a file.
    one record per instance, metrics_name
    """

    all_aggregate_result = {}

    unit = None

    for instance_id in all_metrics:
        aggregate_result = {}
        metrics = all_metrics[instance_id]
        extended_metrics = all_extended_metrics[instance_id]

        if unit is None:
            unit = metrics[0]["Unit"]

        aggregate_result["StartTime"] = start_time.strftime('%Y/%m/%d %H:%M:%S')
        aggregate_result["EndTime"] = end_time.strftime('%Y/%m/%d %H:%M:%S')
        aggregate_result["ServiceTag"] = service_tag
        aggregate_result["Cluster"] = cluster
        aggregate_result["Instance"] = instance_id
        aggregate_result["MetricName"] = metric_name
        aggregate_result["MetricUnit"] = unit

        for statistic in statistics:
            values = [dp[statistic] for dp in metrics if statistic in dp]
            if statistic == "Average":
                # avg = sum(values) / len(values) if values else 0
                avg_value = mean(values)
                aggregate_result[f"avg"] = avg_value
            if statistic == "Maximum":
                max_value = max(values)
                aggregate_result[f"max"] = max_value
            if statistic == "Minimum":
                min_value = min(values)
                aggregate_result[f"min"] = min_value
            if statistic == "Sum":
                sum_value = sum(values)
                aggregate_result[f"sum"] = sum_value
        
        for extended_statistic in extended_statistics:
            values = [dp["ExtendedStatistics"][extended_statistic] for dp in extended_metrics if extended_statistic in dp["ExtendedStatistics"]]
            avg_p_value = mean(values)
            max_p_value = max(values)
            aggregate_result[f"{extended_statistic}_avg"] = avg_p_value
            aggregate_result[f"{extended_statistic}_max"] = avg_p_value
        
        all_aggregate_result[instance_id] = aggregate_result
        # print(all_aggregate_result)
    
    return all_aggregate_result