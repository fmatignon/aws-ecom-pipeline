#!/usr/bin/env python3
"""
ECS Operations Deployment Script

This script handles the complete process of:
1. Building the data-generation Docker image
2. Pushing it to ECR
3. Verifying the deployment
4. Optionally testing the execution

Environment Variables:
- Loads from .env file at project root if available
- Supports AWS credentials and configuration via .env

Usage:
    python source-systems/ecs/deploy_operations.py [--test] [--no-push] [--test-only]

Arguments:
    --test: Trigger a test execution after deployment
    --no-push: Build image but don't push to ECR
    --test-only: Trigger test execution without building/pushing image
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


def _current_timestamp() -> str:
    """
    Generate a UTC timestamp for console logging.

    Returns:
        str: Current UTC time formatted as YYYY-MM-DD HH:MM:SS.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def log_section_start(section: str, details: str | None = None) -> None:
    """
    Print a timestamped start log for an operation section.

    Args:
        section (str): Description of the section.
        details (str | None): Optional extra context.
    """
    suffix = f" - {details}" if details else ""
    print(f"[{_current_timestamp()}] Starting: {section}{suffix}")


def log_section_complete(section: str, details: str | None = None) -> None:
    """
    Print a timestamped completion log for an operation section.

    Args:
        section (str): Description of the section.
        details (str | None): Optional extra context.
    """
    suffix = f" - {details}" if details else ""
    print(f"[{_current_timestamp()}] Completed: {section}{suffix}")


def log_progress(section: str, message: str) -> None:
    """
    Print a timestamped progress message for a running section.

    Args:
        section (str): Description of the section.
        message (str): Progress detail.
    """
    print(f"[{_current_timestamp()}] {section}: {message}")


def log_error(section: str, error: Exception) -> None:
    """
    Print a timestamped error log.

    Args:
        section (str): Section name where the error occurred.
        error (Exception): Error instance to report.
    """
    print(f"[{_current_timestamp()}] Error in {section}: {error}")


class OperationsDeployer:
    """
    Helper that orchestrates building, pushing, verifying, and testing ECS deployments.
    """

    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent

        # Load environment variables from .env file at project root
        env_path = self.project_root / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=True)
            self._log_section_start("Environment", f"Loading {env_path}")
            self._log_section_complete(
                "Environment", f"Loaded variables from {env_path}"
            )
        else:
            self._log_section_start("Environment", f"Checking {env_path}")
            self._log_section_complete(
                "Environment", f"No .env file found at {env_path}"
            )

        self.data_generation_dir = (
            self.project_root / "source-systems" / "data-generation"
        )
        self.infrastructure_dir = self.project_root / "infrastructure"

        # Get AWS region from CDK context or default
        self.region = self._get_aws_region()

        # Initialize AWS clients
        self.ecr_client = boto3.client("ecr", region_name=self.region)
        self.stepfunctions_client = boto3.client(
            "stepfunctions", region_name=self.region
        )

        # Get deployment outputs
        self.outputs = self._get_deployment_outputs()

    def _timestamp(self) -> str:
        """
        Provide the current UTC timestamp for logging.

        Returns:
            str: Timestamp formatted as YYYY-MM-DD HH:MM:SS in UTC.
        """
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def _log_section_start(self, section: str, details: str | None = None) -> None:
        """
        Log the start of an operation section.

        Args:
            section (str): Description of the section.
            details (str | None): Optional extra details to include.
        """
        suffix = f" - {details}" if details else ""
        print(f"[{self._timestamp()}] Starting: {section}{suffix}")

    def _log_progress(self, section: str, message: str) -> None:
        """
        Log intermediate progress within a section.

        Args:
            section (str): Description of the section.
            message (str): Progress detail.
        """
        print(f"[{self._timestamp()}] {section}: {message}")

    def _log_section_complete(self, section: str, details: str | None = None) -> None:
        """
        Log the completion of an operation section.

        Args:
            section (str): Description of the section.
            details (str | None): Optional extra details to include.
        """
        suffix = f" - {details}" if details else ""
        print(f"[{self._timestamp()}] Completed: {section}{suffix}")

    def _log_error(self, section: str, error: Exception) -> None:
        """
        Log an error associated with a section.

        Args:
            section (str): Description of the section where the error occurred.
            error (Exception): Exception instance.
        """
        print(f"[{self._timestamp()}] Error in {section}: {error}")

    def _get_aws_region(self):
        """Get AWS region from CDK context or environment."""
        cdk_context = self.infrastructure_dir / "cdk.context.json"
        if cdk_context.exists():
            try:
                with open(cdk_context) as f:
                    context = json.load(f)
                    return (
                        context.get("availability-zones", ["us-east-1"])[0].split("-")[
                            0
                        ]
                        + "-east-1"
                    )
            except Exception as e:
                log_error("AWS Region Detection", e)

        # Fallback to environment or default
        import os

        return os.getenv("AWS_DEFAULT_REGION")

    def _get_deployment_outputs(self):
        """Get outputs from CDK deployment"""
        outputs_file = (
            self.infrastructure_dir / "cdk.out" / "EcomOperationsStack.outputs.json"
        )
        if outputs_file.exists():
            with open(outputs_file) as f:
                return json.load(f)
        return {}

    def _run_command(self, cmd, cwd=None, capture_output=False, input=None):
        """Run a shell command with proper error handling."""
        self._log_section_start("Command Execution", f"{' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                cwd=cwd,
                capture_output=capture_output,
                text=True,
                check=True,
                input=input,
            )
            if capture_output:
                return result.stdout.strip()
            return True
        except subprocess.CalledProcessError as e:
            self._log_error("Command Execution", e)
            if e.stdout:
                self._log_progress("Command Execution", f"stdout: {e.stdout}")
            if e.stderr:
                self._log_progress("Command Execution", f"stderr: {e.stderr}")
            raise
        finally:
            self._log_section_complete("Command Execution", "Command finished")

    def build_docker_image(self):
        """Build the Docker image."""
        self._log_section_start("Build Docker Image", "Preparing build")

        # Ensure we're in the data-generation directory
        if not (self.data_generation_dir / "Dockerfile").exists():
            raise FileNotFoundError(
                f"Dockerfile not found in {self.data_generation_dir}"
            )

        # Build for linux/amd64 platform (required for ECS Fargate)
        self._run_command(
            [
                "docker",
                "build",
                "--platform",
                "linux/amd64",
                "-t",
                "ecom-operations:latest",
                ".",
            ],
            cwd=self.data_generation_dir,
        )
        self._log_section_complete(
            "Build Docker Image", "Docker image built successfully"
        )

    def login_to_ecr(self):
        """Login to ECR."""
        self._log_section_start("ECR Login", "Preparing credentials")

        # Get ECR login password
        login_password = self._run_command(
            ["aws", "ecr", "get-login-password", "--region", self.region],
            capture_output=True,
        )

        account_id = os.getenv("AWS_ACCOUNT_ID")
        ecr_uri = self.outputs.get(
            "EcrRepositoryUri",
            f"{account_id}.dkr.ecr.{self.region}.amazonaws.com/ecom-operations",
        )
        registry_uri = ecr_uri.split("/")[0]

        # Login to Docker
        self._run_command(
            ["docker", "login", "--username", "AWS", "--password-stdin", registry_uri],
            input=login_password,
        )

        self._log_section_complete("ECR Login", "Logged into ECR successfully")

    def tag_and_push_image(self):
        """Tag and push the Docker image to ECR."""
        self._log_section_start("Tag and Push Image", "Tagging image")

        account_id = os.getenv("AWS_ACCOUNT_ID")
        ecr_uri = self.outputs.get(
            "EcrRepositoryUri",
            f"{account_id}.dkr.ecr.{self.region}.amazonaws.com/ecom-operations",
        )

        # Tag the image
        self._run_command(
            ["docker", "tag", "ecom-operations:latest", f"{ecr_uri}:latest"]
        )

        # Push the image
        self._run_command(["docker", "push", f"{ecr_uri}:latest"])

        self._log_section_complete(
            "Tag and Push Image", "Image pushed to ECR successfully"
        )

    def verify_ecr_image(self):
        """Verify the image exists in ECR."""
        self._log_section_start("ECR Verification", "Checking repository")

        try:
            response = self.ecr_client.describe_images(
                repositoryName="ecom-operations", imageIds=[{"imageTag": "latest"}]
            )

            if response["imageDetails"]:
                image = response["imageDetails"][0]
                size_mb = image["imageSizeInBytes"] / (1024 * 1024)
                pushed_at = image["imagePushedAt"].strftime("%Y-%m-%d %H:%M:%S")
                self._log_section_complete(
                    "ECR Verification",
                    f"Image verified: {size_mb:.1f}MB, pushed at {pushed_at}",
                )
                return True
            else:
                self._log_section_complete("ECR Verification", "Image not found in ECR")
                return False

        except ClientError as e:
            self._log_error("ECR Verification", e)
            return False

    def test_execution(self):
        """Trigger a test execution of the Step Functions state machine."""
        self._log_section_start("Test Execution", "Starting a test run")

        state_machine_arn = self.outputs.get(
            "StateMachineArn", os.getenv("STATE_MACHINE_ARN")
        )

        execution_name = f"test-{int(time.time())}"

        try:
            response = self.stepfunctions_client.start_execution(
                stateMachineArn=state_machine_arn, name=execution_name
            )

            execution_arn = response["executionArn"]
            self._log_section_complete(
                "Test Execution", f"Test execution started: {execution_arn}"
            )

            execution = self.stepfunctions_client.describe_execution(
                executionArn=execution_arn
            )
            log_progress("Test Execution", f"Status: {execution['status']}")
            log_progress("Test Execution", f"Started: {execution['startDate']}")

            return execution_arn

        except ClientError as e:
            self._log_error("Test Execution", e)
            return None

    def monitor_logs(self, execution_arn=None):
        """Monitor CloudWatch logs for the execution."""
        self._log_section_start("Log Monitoring", "Tailing CloudWatch logs")
        log_progress("Log Monitoring", "Press Ctrl+C to stop monitoring")
        log_progress(
            "Log Monitoring",
            f"Check: https://{self.region}.console.aws.amazon.com/cloudwatch/home -> Logs -> Log groups -> /ecs/ecom-operations",
        )

        try:
            # Simple log tailing using AWS CLI
            cmd = [
                "aws",
                "logs",
                "tail",
                "/ecs/ecom-operations",
                "--follow",
                "--region",
                self.region,
            ]
            if execution_arn:
                # Filter for specific execution if possible
                pass

            self._run_command(cmd)

        except KeyboardInterrupt:
            log_progress("Log Monitoring", "Stopped monitoring logs")
        except Exception as e:
            log_error("Log Monitoring", e)
            log_progress("Log Monitoring", "Try checking CloudWatch console directly")
        finally:
            self._log_section_complete("Log Monitoring", "Log monitoring finished")

    def deploy(self, push=True, test=False, test_only=False):
        """Main deployment process"""
        if test_only:
            self._log_section_start("Test Only", "Starting test execution only")
        else:
            self._log_section_start("Deployment", "Starting ECS Operations Deployment")

        try:
            if not test_only:
                # Step 1: Build Docker image
                self.build_docker_image()

                if push:
                    # Step 2: Login to ECR
                    self.login_to_ecr()

                    # Step 3: Push image
                    self.tag_and_push_image()

                    # Step 4: Verify image
                    if not self.verify_ecr_image():
                        raise Exception("Image verification failed")

                self._log_section_complete(
                    "Deployment", "Docker deployment completed successfully"
                )

            if test or test_only:
                if test_only:
                    log_progress("Test Only", "Starting test execution")
                else:
                    log_progress("Deployment", "Testing deployment")

                # Step 5: Trigger test execution
                execution_arn = self.test_execution()

                if execution_arn:
                    log_progress(
                        "Test Only" if test_only else "Deployment", "Monitoring logs"
                    )
                    log_progress(
                        "Test Only" if test_only else "Deployment",
                        f"Execution ARN: {execution_arn}",
                    )
                    log_progress(
                        "Test Only" if test_only else "Deployment",
                        "Also monitor at: https://console.aws.amazon.com/states/home",
                    )

                    # Step 6: Monitor logs
                    self.monitor_logs(execution_arn)

        except Exception as e:
            self._log_error("Test Only" if test_only else "Deployment", e)
            sys.exit(1)

    def show_status(self):
        """Show current deployment status."""
        self._log_section_start("Status Check", "Reporting current deployment status")

        # Check ECR
        try:
            images = self.ecr_client.describe_images(repositoryName="ecom-operations")
            if images["imageDetails"]:
                latest = max(images["imageDetails"], key=lambda x: x["imagePushedAt"])
                log_progress(
                    "Status Check",
                    f"ECR: Image exists ({latest['imagePushedAt'].strftime('%Y-%m-%d %H:%M:%S')})",
                )
            else:
                log_progress("Status Check", "ECR: No images found")
        except Exception as e:
            log_error("Status Check", e)

        # Check State Machine
        try:
            state_machine_arn = self.outputs.get("StateMachineArn")
            if state_machine_arn:
                executions = self.stepfunctions_client.list_executions(
                    stateMachineArn=state_machine_arn, maxResults=5
                )
                if executions["executionList"]:
                    latest = executions["executionList"][0]
                    log_progress(
                        "Status Check",
                        f"Step Functions: {len(executions['executionList'])} executions, latest: {latest['status']}",
                    )
                else:
                    log_progress("Status Check", "Step Functions: No executions yet")
            else:
                log_progress("Status Check", "Step Functions: ARN not found")
        except Exception as e:
            log_error("Status Check", e)
        finally:
            self._log_section_complete("Status Check", "Status report finished")


def main():
    """
    Parse CLI arguments and orchestrate ECS deployment workflow.
    """
    parser = argparse.ArgumentParser(description="Deploy ECS Operations")
    parser.add_argument(
        "--test", action="store_true", help="Trigger test execution after deployment"
    )
    parser.add_argument(
        "--no-push", action="store_true", help="Build image but don't push to ECR"
    )
    parser.add_argument(
        "--status", action="store_true", help="Show current deployment status"
    )
    parser.add_argument(
        "--test-only",
        action="store_true",
        help="Trigger test execution without building/pushing image",
    )

    args = parser.parse_args()

    deployer = OperationsDeployer()

    if args.status:
        deployer.show_status()
        return

    if args.test_only:
        log_section_start("Test Only Script", "Preparing to run test execution")
        log_progress("Test Only Script", "This script will:")
        log_progress(
            "Test Only Script",
            "1. Trigger a test execution of the Step Functions state machine",
        )
        log_progress("Test Only Script", "2. Monitor the execution logs")
        log_section_complete("Test Only Script", "Ready for confirmation")
        confirm = input("Continue? (y/N): ").lower().strip()
        if confirm not in ["y", "yes"]:
            log_progress("Test Only Script", "Cancelled by user")
            return
        deployer.deploy(push=False, test=False, test_only=True)
        return

    log_section_start("Deployment Script", "Preparing to deploy")
    log_progress("Deployment Script", "This script will:")
    log_progress("Deployment Script", "1. Build the data-generation Docker image")
    log_progress("Deployment Script", "2. Tag and push it to ECR")
    log_progress("Deployment Script", "3. Verify the image exists in ECR")

    if args.test:
        log_progress(
            "Deployment Script",
            "4. Trigger a test execution of the Step Functions state machine",
        )
        log_progress("Deployment Script", "5. Monitor the execution logs")

    if args.no_push:
        log_progress(
            "Deployment Script", "--no-push specified: Image will NOT be pushed to ECR"
        )

    log_section_complete("Deployment Script", "Ready for confirmation")
    confirm = input("Continue? (y/N): ").lower().strip()
    if confirm not in ["y", "yes"]:
        log_progress("Deployment Script", "Cancelled by user")
        return

    deployer.deploy(push=not args.no_push, test=args.test)


if __name__ == "__main__":
    main()
