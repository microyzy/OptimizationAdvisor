import app_rds_metrics
import app_llm
import argparse
import sys

if __name__ == "__main__":
    
    # Retrieve parameters from command-line arguments
    parser = argparse.ArgumentParser(description="Retrieve parameters for RDS metrics and LLM advisor.")
    parser.add_argument("--LastDays", type=int, default=30, help="Number of days to look back for metrics.")
    parser.add_argument("--Clusters", type=str, help="Comma-separated list of cluster id.")
    args = parser.parse_args()

    last_n_days = args.LastDays
    clusters = args.Clusters.split(",") if args.Clusters else None

    # Print parameter values
    print(f"LastDays: {last_n_days}")
    print(f"Clusters: {clusters}")

    # sys.exit()

    app_rds_metrics.generate_rds_metrics_graphics(last_n_days=last_n_days, clusters=clusters)

    # app_llm.ask_llm_advisor()
