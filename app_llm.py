import boto3
from datetime import datetime, timedelta
import json
import base64
import aws_cred

def ask_llm_advisor():
    # Set up AWS credentials and region
    aws_access_key = aws_cred.AWS_ACCESS_KEY_BEDROCK
    aws_secret_key = aws_cred.AWS_SECRET_KEY_BEDROCK
    aws_region = "us-west-2"

    bedrock_agent_client = boto3.client(service_name='bedrock-agent-runtime', region_name=aws_region,
                                        aws_access_key_id=aws_access_key,
                                        aws_secret_access_key=aws_secret_key)
    bedrock_runtime_client = boto3.client(service_name='bedrock-runtime', region_name=aws_region,
                                          aws_access_key_id=aws_access_key,
                                            aws_secret_access_key=aws_secret_key)

    modelIdMain = "anthropic.claude-3-5-sonnet-20240620-v1:0"

    metrics_graphic_file = "metrics/aurora-test-1/aurora-test-1-CPUUtilization-plotly.png"
    promptTemplate = f"""
    You are an expert in analyzing AWS RDS metrics and your duty is monitoring the RDS and suggest possible optimization for the RDS cluster.
    You have been provided with a graph of RDS metrics for a specific cluster.
    Please analyze the attached graph and provide insights about the cluster's performance. 

    Step.1 give a short summary on given metrics. also give the number or range of the metrics.
    Step.2 based on the given metrics, by follwoing below target or policy list, also referring to the optimizations we done before, 
    identify any potential issues or areas for improvement, and suggest possible optimizations.

    <analyze target and policy list>
    - is the usage too low(<30%) or too hight(>70)?

    <optimizations we done before>
    - down/up grade instance type, instance type must be >= large
    - add/remove reader instance
    - change instance type to serverless

    give a short summary about how the metrics will change if we done the optimization you suggest.
    please report shortly only with necessary information.
    """

    # Read the image file
    with open(metrics_graphic_file, "rb") as file:
        # image_data = file.read()
        image_data = base64.b64encode(file.read()).decode('utf-8')

    # Prepare the payload for the LLM
    payload = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": promptTemplate},
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": image_data
                        }
                    }
                ]
            }
        ],
        "max_tokens": 2048,
        "anthropic_version": "bedrock-2023-05-31"
    }

    # Call the LLM
    response = bedrock_runtime_client.invoke_model(
        modelId=modelIdMain,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload)
    )

    # Parse the response
    response_body = json.loads(response.get('body').read())
    # print(response_body)
    analysis_result = response_body["content"][0]["text"]
    input_tokens = response_body["usage"]["input_tokens"]
    output_tokens = response_body["usage"]["output_tokens"]

    # Print the results
    print("LLM Analysis Result:")
    print(analysis_result)

    ex_rate = 150
    input_cost = input_tokens / 1000 * 0.003
    output_cost = output_tokens / 1000 * 0.015
    total_cost = input_cost + output_cost

    print("LLM Cost."
        f"\nInput: {input_tokens} tokens ({input_cost} USD | {input_cost * ex_rate} JPY) - 0.003 USD per 1K tokens) ",
        f"\nOutput: {output_tokens} tokens ({output_cost} USD | {output_cost * ex_rate} JPY) - 0.015 USD per 1K tokens) ",
        f"\nTotal: {total_cost} USD | {total_cost * ex_rate} JPY)",
    )


