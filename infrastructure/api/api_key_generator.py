"""
Lambda function for generating and syncing API keys between Secrets Manager and API Gateway.

This function is used as a CloudFormation Custom Resource to:
- Generate a secure 48-character API key
- Store it in Secrets Manager with structure {"api_key": "value"}
- Create/update the API Gateway API key with the same value
- Associate the API key with the usage plan
"""

import boto3
import json
import secrets
import string
import cfnresponse


def generate_api_key(length=48):
    """
    Generate a secure alphanumeric API key.

    Args:
        length: Length of the API key to generate (default: 48)

    Returns:
        str: A secure random alphanumeric string
    """
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def handler(event, context):
    """
    CloudFormation Custom Resource handler for API key synchronization.

    Args:
        event: CloudFormation Custom Resource event
        context: Lambda context object

    The event contains:
        - RequestType: 'Create', 'Update', or 'Delete'
        - ResourceProperties:
            - SecretArn: ARN of the Secrets Manager secret (already generated)
            - ApiKeyName: Name for the API Gateway API key
            - UsagePlanId: ID of the API Gateway usage plan
        - PhysicalResourceId: Existing API key ID (for updates/deletes)
    """
    try:
        secrets_client = boto3.client("secretsmanager")
        apigateway_client = boto3.client("apigateway")

        secret_arn = event["ResourceProperties"]["SecretArn"]
        api_key_name = event["ResourceProperties"]["ApiKeyName"]
        usage_plan_id = event["ResourceProperties"].get("UsagePlanId")
        api_key_id = event.get("PhysicalResourceId")  # Existing API key ID if updating

        if event["RequestType"] == "Delete":
            # On delete, just respond success (API Gateway key will be deleted by CDK)
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
            return

        # Check if secret already has a value (from previous deployment)
        api_key_value = None
        try:
            secret_response = secrets_client.get_secret_value(SecretId=secret_arn)
            secret_data = json.loads(secret_response["SecretString"])
            if "api_key" in secret_data and secret_data["api_key"]:
                # Use existing API key value
                api_key_value = secret_data["api_key"]
                print(
                    f"Using existing API key from secret (length: {len(api_key_value)})"
                )
        except secrets_client.exceptions.ResourceNotFoundException:
            # Secret doesn't exist yet, will generate new key below
            print("Secret not found, will generate new key")
            api_key_value = None
        except json.JSONDecodeError:
            # Secret has invalid JSON, will regenerate below
            print("Secret has invalid JSON, will regenerate")
            api_key_value = None
        except Exception as e:
            # If we can't read existing, generate new
            print(f"Could not read existing secret: {e}, generating new key")
            api_key_value = None

        # Generate new API key if we don't have one
        if not api_key_value:
            api_key_value = generate_api_key(length=48)
            secret_string = json.dumps({"api_key": api_key_value})
            secrets_client.put_secret_value(
                SecretId=secret_arn, SecretString=secret_string
            )
            print("Generated and stored new 48-character API key")

        # Create or update API Gateway API key
        if api_key_id and event["RequestType"] == "Update":
            # Update existing key with new value from secret
            apigateway_client.update_api_key(
                apiKey=api_key_id,
                patchOperations=[
                    {"op": "replace", "path": "/value", "value": api_key_value}
                ],
            )
            response_data = {"ApiKeyId": api_key_id, "ApiKeyValue": api_key_value}
            print(f"Updated existing API Gateway key {api_key_id}")
        else:
            # Create new API Gateway key with value from secret
            response = apigateway_client.create_api_key(
                name=api_key_name,
                description="API key for payments and shipments APIs",
                enabled=True,
                value=api_key_value,
            )
            api_key_id = response["id"]
            response_data = {"ApiKeyId": api_key_id, "ApiKeyValue": api_key_value}
            print(f"Created new API Gateway key {api_key_id}")

            # Associate API key with usage plan
            if usage_plan_id:
                try:
                    apigateway_client.create_usage_plan_key(
                        usagePlanId=usage_plan_id, keyId=api_key_id, keyType="API_KEY"
                    )
                    print(f"Associated API key with usage plan {usage_plan_id}")
                except apigateway_client.exceptions.ConflictException:
                    # Already associated, ignore
                    print("API key already associated with usage plan")
                    pass

        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, api_key_id)

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()
        cfnresponse.send(
            event,
            context,
            cfnresponse.FAILED,
            {},
            physical_resource_id=api_key_id if "api_key_id" in locals() else None,
        )
