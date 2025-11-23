# AWS E-commerce Data Pipeline

A comprehensive AWS-based data pipeline for simulating and analyzing e-commerce operations, featuring automated data generation, real-time processing, and analytics.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                            │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐         ┌──────────────┐                      │
│  │  RDS (Postgres)       │  S3 (Data Lake)                      │
│  │  - Customers          │  - Payments (Parquet)                │
│  │  - Products           │  - Shipments (Parquet)               │
│  │  - Orders             │  - Execution Logs                    │
│  │  - Order Items        │                                      │
│  └──────────────┘         └──────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │
┌─────────────────────────────────────────────────────────────────┐
│                    OPERATIONS AUTOMATION                         │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  ECS Fargate (Daily Data Generation)                     │   │
│  │  ┌────────────────────────────────────────────────────┐  │   │
│  │  │  • Generate new customers & products               │  │   │
│  │  │  • Create orders with realistic patterns           │  │   │
│  │  │  • Update order statuses (shipped, delivered, etc) │  │   │
│  │  │  • Generate payments & shipments                   │  │   │
│  │  │  • Calculate customer segments                     │  │   │
│  │  └────────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                    ▲                             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Step Functions (Orchestration + Retry)                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                    ▲                             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  EventBridge (Daily Schedule - 2 AM UTC)                 │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DATA INGESTION                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  AWS Glue (ETL Jobs)                                     │   │
│  │  • Extract from RDS & S3                                 │   │
│  │  • Transform to analytics format                         │   │
│  │  • Load to Data Warehouse                                │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS & BI                                │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  dbt (Data Transformation)                               │   │
│  │  • Data modeling & quality checks                        │   │
│  │  • Business metrics calculation                          │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## 📂 Project Structure

```
aws-ecom/
├── infrastructure/           # AWS CDK infrastructure as code
│   ├── rds/                  # RDS stack (Postgres database)
│   ├── s3/                   # S3 stack (data lake)
│   ├── api/                  # API Gateway + Lambda functions
│   ├── operations/           # ECS Fargate + Step Functions + EventBridge
│   ├── source_infra.py       # Main CDK app
│   └── requirements.txt      # CDK dependencies
│
├── source-systems/           # Source system utilities
│   ├── data-generation/      # ECS data generation package
│   │   ├── generators/       # Data generators (customers, orders, etc.)
│   │   ├── rds/              # RDS loader utilities
│   │   ├── s3/               # S3 Parquet manager
│   │   ├── config/           # Configuration & settings
│   │   ├── main.py           # Main entry point
│   │   ├── utils/            # Shared utilities (state, logging, cleanup)
│   │   ├── Dockerfile        # Container image
│   │   └── requirements.txt  # Python dependencies
│   ├── ecs/                  # ECS deployment helpers
│   ├── lambda/               # Lambda function code                 # S3 utilities
│
├── glue/                     # AWS Glue ETL jobs
├── dbt/                      # dbt transformation models
├── step-functions/           # Step Functions definitions
│
├── DEPLOYMENT_GUIDE.md       # Detailed deployment instructions
├── DEPLOYMENT_QUICK_REFERENCE.md  # Quick command reference
├── ENV_EXAMPLE.md            # Environment variables template
└── README.md                 # This file
```

## 🚀 Quick Start

### Prerequisites
- AWS Account with appropriate permissions
- AWS CLI configured (`aws configure`)
- Docker installed
- AWS CDK CLI installed (`npm install -g aws-cdk`)
- Python 3.11+

### 1. Clone & Setup
```bash
git clone <repository-url>
cd aws-ecom

# Create .env file (see ENV_EXAMPLE.md)
cp ENV_EXAMPLE.md .env
# Edit .env with your AWS credentials
```

### 2. Deploy Infrastructure
```bash
cd infrastructure

# Install CDK dependencies
pip install -r requirements.txt

# Deploy all stacks
cdk deploy --all

# Or deploy individually:
cdk deploy EcomS3Stack
cdk deploy EcomRDSStack
cdk deploy EcomAPIStack
cdk deploy EcomOperationsStack
```

### 3. Build & Deploy Docker Image
```bash
cd ../source-systems/data-generation

# Get ECR URI from CDK outputs
export ECR_URI="<your-ecr-uri>"

# Build and push
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin $ECR_URI
docker build -t ecom-operations:latest .
docker tag ecom-operations:latest $ECR_URI:latest
docker push $ECR_URI:latest
```

### 4. Run Initial Data Generation (Optional - for testing)
```bash
cd source-systems/data-generation

# Set environment variables (if not using CDK/ECS)
export S3_BUCKET_NAME="aws-ecom-pipeline"
export RDS_ENDPOINT="<your-rds-endpoint>"
export RDS_USERNAME="admin"
export RDS_PASSWORD="<your-password>"
export START_DATE="2024-01-01"

# Run locally
python main.py
```

### 5. Verify Deployment
```bash
# Run verification script
cd ..
./verify_deployment_ready.sh

# Check CloudWatch logs
aws logs tail /ecs/ecom-operations --follow
```

## 📋 Features

### Data Generation
- **Realistic Customer Data**: Demographics, segments (VIP, Regular, New), purchase patterns
- **Product Catalog**: 50+ categories, seasonal variations, inventory tracking
- **Order Simulation**: Realistic order patterns based on customer segments and product popularity
- **Payment Processing**: Multiple payment methods, transaction tracking
- **Shipment Management**: Realistic delivery times, carrier tracking
- **Status Updates**: Automatic order progression (pending → shipped → delivered)

### Operational Modes
1. **Initial Run**: Generate full historical dataset (18 months) using date range from `config/settings.py` (triggered when no data exists in RDS or S3 logs)
2. **Ongoing Operations**: Daily incremental updates from last run date to yesterday
3. **Bulk Reload**: Efficient full database refresh using Parquet snapshots

### Storage Strategy
- **RDS (Postgres)**: Transactional data (customers, products, orders, order items)
- **S3 (Parquet)**: Analytical data (payments, shipments) with date partitioning
- **State Management**: S3 logs track execution history and resume points

### Automation
- **Daily Execution**: EventBridge triggers at 2 AM UTC
- **Orchestration**: Step Functions manages workflow with retry logic
- **Monitoring**: CloudWatch logs for execution tracking
- **Error Handling**: Automatic retries on failures (2 attempts, exponential backoff)

## 🔧 Configuration

### Environment Variables

#### Required for ECS Deployment
- `S3_BUCKET_NAME`: S3 bucket for data storage
- `RDS_ENDPOINT`: RDS database endpoint
- `RDS_DATABASE_NAME`: Database name (default: ecommerce)
- `RDS_SECRET_ARN`: Secrets Manager ARN for RDS credentials
- `AWS_DEFAULT_REGION`: AWS region

#### Optional
- `START_DATE`: Initial run date (YYYY-MM-DD) - only for first execution
- `RDS_USERNAME`: Direct RDS username (if not using Secrets Manager)
- `RDS_PASSWORD`: Direct RDS password (if not using Secrets Manager)

See `ENV_EXAMPLE.md` for complete configuration guide.

### Environment Variables

#### Adjust Data Generation Settings
Edit `source-systems/data-generation/config/settings.py`:
```python
# Customer generation
INITIAL_CUSTOMERS = 100000
DAILY_NEW_CUSTOMERS = 400

# Order generation
DAILY_ORDER_RATE = 0.04  # 4% of customers order per day

# Customer segments
VIP_SPENDING_THRESHOLD = 10000  # $10,000
VIP_SUBSCRIPTION_RATE = 0.3  # 30% have subscription
```

#### Change Execution Schedule
Edit `infrastructure/operations/operations_stack.py`:
```python
schedule=events.Schedule.cron(
    minute="0",
    hour="2",  # Change to different UTC hour
    day="*",
    month="*",
    year="*"
)
```

#### Adjust Task Resources
Edit `infrastructure/operations/operations_stack.py`:
```python
task_definition = ecs.FargateTaskDefinition(
    self,
    "OperationsTaskDefinition",
    memory_limit_mib=4096,  # Increase if needed
    cpu=2048,                # Increase if needed
)
```

## 📊 Data Schema

### RDS Tables

#### customers
| Column | Type | Description |
|--------|------|-------------|
| customer_id | INT | Primary key |
| first_name | VARCHAR | Customer first name |
| last_name | VARCHAR | Customer last name |
| email | VARCHAR | Email address |
| phone | VARCHAR | Phone number |
| address | TEXT | Full address |
| city | VARCHAR | City |
| state | VARCHAR | State/Province |
| zip_code | VARCHAR | Postal code |
| country | VARCHAR | Country |
| registration_date | DATE | Account creation date |
| segment | VARCHAR | Customer segment (VIP/Regular/New) |
| lifetime_value | DECIMAL | Total spending |
| created_at | TIMESTAMP | Record creation timestamp |
| updated_at | TIMESTAMP | Last update timestamp |

#### products
| Column | Type | Description |
|--------|------|-------------|
| product_id | INT | Primary key |
| product_name | VARCHAR | Product name |
| category | VARCHAR | Product category |
| price | DECIMAL | Current price |
| cost | DECIMAL | Cost of goods sold |
| stock_quantity | INT | Available inventory |
| created_at | TIMESTAMP | Product creation date |
| updated_at | TIMESTAMP | Last update timestamp |

#### orders
| Column | Type | Description |
|--------|------|-------------|
| order_id | INT | Primary key |
| customer_id | INT | Foreign key → customers |
| order_date | DATE | Order creation date |
| order_datetime | TIMESTAMP | Exact order time |
| status | VARCHAR | Order status |
| total_amount | DECIMAL | Total order value |
| created_at | TIMESTAMP | Record creation timestamp |
| updated_at | TIMESTAMP | Last update timestamp |

#### order_items
| Column | Type | Description |
|--------|------|-------------|
| order_item_id | INT | Primary key |
| order_id | INT | Foreign key → orders |
| product_id | INT | Foreign key → products |
| quantity | INT | Quantity ordered |
| unit_price | DECIMAL | Price per unit |
| subtotal | DECIMAL | Line item total |
| created_at | TIMESTAMP | Record creation timestamp |

### S3 Parquet Files

#### payments/ (date partitioned)
- `payment_id`: Unique payment identifier
- `order_id`: Associated order
- `payment_date`: Payment date
- `payment_datetime`: Exact payment time
- `payment_method`: Payment method used
- `amount`: Payment amount
- `transaction_id`: External transaction reference

#### shipments/ (date partitioned)
- `shipment_id`: Unique shipment identifier
- `order_id`: Associated order
- `shipment_date`: Shipment date
- `shipment_datetime`: Exact shipment time
- `carrier`: Shipping carrier
- `tracking_number`: Tracking reference
- `estimated_delivery`: Expected delivery date
- `actual_delivery`: Actual delivery date (if delivered)
- `shipment_status`: Current shipment status

## 🧪 Testing

### Local Testing
```bash
cd source-systems/data-generation
python main.py
```

### Docker Testing
```bash
cd source-systems/data-generation
docker build -t ecom-operations:test .
docker run --env-file ../.env ecom-operations:test
```

### Manual ECS Execution
```bash
export STATE_MACHINE_ARN="<your-state-machine-arn>"
aws stepfunctions start-execution \
  --state-machine-arn $STATE_MACHINE_ARN \
  --name "manual-test-$(date +%s)"
```

## 📈 Monitoring & Observability

### CloudWatch Logs
```bash
# Tail logs in real-time
aws logs tail /ecs/ecom-operations --follow

# Filter for errors
aws logs filter-log-events \
  --log-group-name /ecs/ecom-operations \
  --filter-pattern "ERROR"

# View specific time range
aws logs filter-log-events \
  --log-group-name /ecs/ecom-operations \
  --start-time $(date -u -d '1 hour ago' +%s)000
```

### Step Functions Console
1. AWS Console → Step Functions
2. Find: `EcomOperationsStack-OperationsStateMachine...`
3. View execution history and details

### S3 Execution Logs
```bash
aws s3 ls s3://aws-ecom-pipeline/logs/operations/ --recursive
aws s3 cp s3://aws-ecom-pipeline/logs/operations/run_2025-11-14_135928.json -
```

## 💰 Cost Estimate

| Service | Configuration | Monthly Cost |
|---------|--------------|--------------|
| RDS (Postgres) | db.t3.micro | ~$15 |
| S3 Storage | ~10GB | ~$0.23 |
| ECS Fargate | 4GB/2vCPU, 10min/day | ~$0.60 |
| ECR Storage | 500MB | ~$0.05 |
| CloudWatch Logs | 100MB/month | ~$0.05 |
| Lambda (if used) | Minimal usage | ~$0.10 |
| **Total** | | **~$16/month** |

*Prices based on us-east-1 region. Actual costs may vary.*

## 🐛 Troubleshooting

### Common Issues

#### Docker build fails
- Check Python version in Dockerfile matches local version
- Ensure all dependencies are in requirements.txt
- Try building without cache: `docker build --no-cache`

#### RDS connection timeout
- Verify security group allows inbound from ECS tasks
- Check VPC configuration in operations_stack.py
- Ensure RDS is in same VPC as ECS tasks

#### Out of Memory (OOM) errors
- Increase task memory in operations_stack.py
- Optimize data processing (batch sizes, chunking)
- Monitor memory usage in CloudWatch

#### Task execution timeout
- Increase timeout in operations_stack.py
- Check for slow queries or large data volumes
- Review CloudWatch logs for bottlenecks

See `DEPLOYMENT_GUIDE.md` for detailed troubleshooting steps.

## 📚 Documentation

- **[DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)**: Comprehensive deployment instructions
- **[DEPLOYMENT_QUICK_REFERENCE.md](./DEPLOYMENT_QUICK_REFERENCE.md)**: Quick command reference
- **[ENV_EXAMPLE.md](./ENV_EXAMPLE.md)**: Environment variables configuration

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Faker library for realistic data generation
- AWS CDK for infrastructure as code
- pandas & pyarrow for efficient data processing

## 📞 Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Check CloudWatch logs for execution details
- Review Step Functions execution history
- Consult AWS documentation for service-specific issues

---

**Built with ❤️ for modern data engineering**

