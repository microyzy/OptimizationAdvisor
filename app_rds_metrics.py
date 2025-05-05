import boto3
import function_rds
from datetime import datetime, timedelta
import json
import os
import aws_cred

def output_line_agg_result(agg, title=True):
    """
    Generate a formatted string for the aggregated result.
    """
    columns = [
        "ServiceTag", "Cluster", "Instance", "MetricName", "MetricUnit",
        "avg", "max", "min", "sum",
        "p99_avg", "p99_max",
        "p90_avg", "p90_max",
        "p80_avg", "p80_max",
        "p50_avg", "p50_max"
        ]

    if title:
        res = ",".join(columns) + "\n"
    else:
        res = ""
        for col in columns:
            res += f"{agg.get(col, '-')},"
        res += "\n"
    return res

def generate_rds_metrics_graphics(last_n_days=30, clusters=None, service_tags=None):
    # Set up AWS credentials and region
    aws_access_key = aws_cred.AWS_ACCESS_KEY_RDS
    aws_secret_key = aws_cred.AWS_SECRET_KEY_RDS
    aws_region = "ap-northeast-1"

    # Initialize boto3 client
    rds_client = boto3.client(
        'rds',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    cloudwatch_client = boto3.client('cloudwatch', 
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region)

    start_time = datetime.now() - timedelta(days=last_n_days)
    end_time = datetime.now()
    is_show = False
    is_save = True


    # Define metrics configurations
    metrics_instance_configurations = [
        {
            "metric_name": "CPUUtilization",
            "period": 60 * 5,
            "statistics": ['Average', 'Maximum'],
            "extended_statistics": ['p90', 'p99'],
            "start_time": start_time,
            "end_time": end_time
        },
        {
            "metric_name": "DatabaseConnections",
            "period": 60 * 5,
            "statistics": ['Average', 'Maximum', 'Sum'],
            "extended_statistics": ['p90', 'p99'],
            "start_time": start_time,
            "end_time": end_time
        },
        {
            "metric_name": "FreeableMemory",
            "period": 60 * 5,
            "statistics": ['Average', 'Minimum'],
            "extended_statistics": ['p90', 'p99'],
            "start_time": start_time,
            "end_time": end_time
        },
        {
            "metric_name": "Queries",
            "period": 60 * 5,
            "statistics": ['Average', 'Maximum', 'Sum'],
            "extended_statistics": ['p90', 'p99'],
            "start_time": start_time,
            "end_time": end_time
        },
        {
            "metric_name": "ReadIOPS",
            "period": 60 * 60 * 24,
            "statistics": ['Average', 'Maximum', 'Sum'],
            "extended_statistics": ['p90', 'p99'],
            "start_time": start_time,
            "end_time": end_time
        },
        {
            "metric_name": "WriteIOPS",
            "period": 60 * 60 * 24,
            "statistics": ['Average', 'Maximum', 'Sum'],
            "extended_statistics": ['p90', 'p99'],
            "start_time": start_time,
            "end_time": end_time
        }
        # Add more configurations as needed
    ]

    metrics_cluster_configurations = [
        {
            "metric_name": "VolumeBytesUsed",
            "period": 60 * 60,
            "statistics": ['Average', 'Maximum'],
            "extended_statistics": ['p90', 'p99'],
            "start_time": start_time,
            "end_time": end_time
        }
    ]

    rds_clusters = function_rds.get_rds_clusters(rds_client, clusters=clusters, service_tags=service_tags)
    if not rds_clusters:
        print("No RDS clusters found.")
        return
    for cluster_info in rds_clusters:
        print("RDS Cluster:", cluster_info)
        cluster = cluster_info['ClusterId']
        service_tag = cluster_info['ServiceTag']
        instances = function_rds.get_instances_in_cluster(rds_client, cluster)

        ## generate basic infor for this cluster
        os.makedirs(f"metrics/{cluster}", exist_ok=True)
        with open(f"metrics/{cluster}/{cluster}.txt", "w") as file:
            file.write(f"Cluster ID: {cluster}\n")
            cluster_info = rds_client.describe_db_clusters(DBClusterIdentifier=cluster)['DBClusters'][0]
            cluster_engine = cluster_info['Engine']
            file.write(f"DB Engine: {cluster_engine}\n")
            file.write(f"Region: {aws_region}\n")
            file.write(f"Number of Instances: {len(instances)}\n")
            file.write("Instances:\n")
            for instance_id in instances:
                instance_info = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)['DBInstances'][0]
                matching_member = next(
                    (member for member in cluster_info["DBClusterMembers"] if member["DBInstanceIdentifier"] == instance_id), 
                    None
                )
                role = "Writer" if matching_member and matching_member["IsClusterWriter"] else "Reader"
                instance_type = instance_info['DBInstanceClass']
                az = instance_info['AvailabilityZone']
                file.write(f"{instance_id}, {role}, {instance_type}, {az}\n")
        

        agg_file = f"metrics/{cluster}/{cluster}-aggregated-metrics.csv"
        # if(os.path.isfile(agg_file)):
        #     os.remove(agg_file)
        with open(agg_file, "w") as file:
            file.write(output_line_agg_result(None, title=True))

        for config in metrics_instance_configurations:
            agg_result = function_rds.generate_instance_level_metrics_graphics_for_cluster(
                cluster, instances, cloudwatch_client,
                metric_name=config["metric_name"],
                period=config["period"],
                statistics=config["statistics"],
                extended_statistics=config["extended_statistics"],
                start_time=config["start_time"],
                end_time=config["end_time"],
                is_show=is_show,
                is_save=is_save,
                service_tag=service_tag
            )
            # Write agg_result to file
            with open(agg_file, "a") as file:
                for key in agg_result:
                    file.write(output_line_agg_result(agg_result[key], title=False))
        
        for config in metrics_cluster_configurations:
            agg_result = function_rds.generate_cluster_level_metrics_graphics_for_cluster(
                cluster, cloudwatch_client,
                metric_name=config["metric_name"],
                period=config["period"],
                statistics=config["statistics"],
                extended_statistics=config["extended_statistics"],
                start_time=config["start_time"],
                end_time=config["end_time"],
                is_show=is_show,
                is_save=is_save,
                service_tag=service_tag
            )
            # Write agg_result to file
            with open(agg_file, "a") as file:
                for key in agg_result:
                    file.write(output_line_agg_result(agg_result[key], title=False))


