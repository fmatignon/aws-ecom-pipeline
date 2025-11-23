"""
CloudFormation Custom Resource response helper.

This module provides utilities for sending responses back to CloudFormation
from Lambda functions used as Custom Resources.
"""

import json
import urllib.request
import urllib.error

SUCCESS = "SUCCESS"
FAILED = "FAILED"


def send(
    event,
    context,
    response_status,
    response_data,
    physical_resource_id=None,
    reason=None,
):
    """
    Send a response to CloudFormation for a Custom Resource.

    Args:
        event: The Lambda event
        context: The Lambda context
        response_status: SUCCESS or FAILED
        response_data: Dictionary of response data
        physical_resource_id: The physical resource ID
        reason: Reason for failure (if status is FAILED)
    """
    response_url = event["ResponseURL"]

    response_body = {
        "Status": response_status,
        "Reason": reason
        or f"See the details in CloudWatch Log Stream: {context.log_stream_name}",
        "PhysicalResourceId": physical_resource_id or context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": response_data,
    }

    json_response_body = json.dumps(response_body).encode("utf-8")

    headers = {"content-type": "", "content-length": str(len(json_response_body))}

    try:
        req = urllib.request.Request(response_url, data=json_response_body, headers=headers, method="PUT")
        with urllib.request.urlopen(req) as response:
            print(f"Status code: {response.getcode()}")
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
        raise
    except Exception as e:
        print(f"Failed to send response: {e}")
        raise

