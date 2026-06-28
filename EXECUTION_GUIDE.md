# Local & Cloud Execution Guide

## Table of Contents
1. [Local Execution](#local-execution)
2. [Azure Cloud Deployment](#azure-cloud-deployment)
3. [Troubleshooting](#troubleshooting)

---

## Local Execution

### Prerequisites
- Python 3.8+
- Java 8 or later (required by Spark)
- Git
- pip (Python package manager)

### Step 1: Clone the Repository

```bash
git clone https://github.com/Kishore2488/azure-data.git
cd azure-data
```

### Step 2: Create Virtual Environment

**On Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**On Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 4: Verify Installation

```bash
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
```

### Step 5: Run the Application

```bash
python main.py
```

**Expected Output:**
- Display of Customers, Products, and Orders tables
- Join results combining all three tables
- Aggregation statistics (total sales, average order value, etc.)
- High-value order filtering
- Summary statistics
- Files saved to `output/` directory

### Step 6: View Results

```bash
# View Parquet results (requires pandas)
python -c "import pandas as pd; df = pd.read_parquet('output/customer_orders'); print(df.head())"

# View CSV results
cat output/sales_by_customer/part-00000-*.csv
```

### Step 7: Stop Virtual Environment

```bash
deactivate
```

---

## Azure Cloud Deployment

### Architecture Overview

```
Local Development
    ↓
    ├── GitHub Repository
    ↓
    ├── Azure DevOps / GitHub Actions
    ↓
    ├── Azure Synapse Analytics / Azure Databricks
    ↓
    ├── Data Lake Storage (ADLS)
    ↓
    └── Results & Dashboards
```

### Prerequisites for Azure

- Azure subscription
- Azure CLI installed
- Azure Storage account
- Azure Databricks workspace (or Synapse Analytics)
- Service Principal for authentication

---

### Option A: Deploy to Azure Databricks

#### Step 1: Set Up Azure Databricks Workspace

```bash
# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Login to Azure
az login

# Create resource group
az group create --name spark-sql-rg --location eastus

# Create Databricks workspace
az databricks workspace create \
  --resource-group spark-sql-rg \
  --name spark-sql-workspace \
  --location eastus \
  --sku premium
```

#### Step 2: Configure Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure CLI (get token from workspace)
databricks configure --token
# Enter host: https://<region>.azuredatabricks.net
# Enter token: <your-databricks-token>
```

#### Step 3: Upload Files to Databricks

```bash
# Create directory in DBFS
databricks fs mkdirs dbfs:/spark-sql-demo

# Upload data files
databricks fs cp data/customers.csv dbfs:/spark-sql-demo/customers.csv --overwrite
databricks fs cp data/products.csv dbfs:/spark-sql-demo/products.csv --overwrite
databricks fs cp data/orders.csv dbfs:/spark-sql-demo/orders.csv --overwrite

# Upload main script
databricks fs cp main.py dbfs:/spark-sql-demo/main.py --overwrite
```

#### Step 4: Create Databricks Notebook

Create a notebook in Databricks with the following content:

```python
# Databricks notebook source
# Mount Azure Storage
dbutils.fs.mount(
  source = "wasbs://spark-data@<storage-account>.blob.core.windows.net",
  mount_point = "/mnt/spark-data",
  extra_configs = {"fs.azure.account.key.<storage-account>.blob.core.windows.net":"<storage-key>"}
)

# Read data from DBFS
customers_df = spark.read.csv("dbfs:/spark-sql-demo/customers.csv", header=True, inferSchema=True)
products_df = spark.read.csv("dbfs:/spark-sql-demo/products.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("dbfs:/spark-sql-demo/orders.csv", header=True, inferSchema=True)

# Register views
customers_df.createOrReplaceTempView("customers")
products_df.createOrReplaceTempView("products")
orders_df.createOrReplaceTempView("orders")

# Execute queries
query = """
SELECT 
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS total_spent
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent DESC
"""

result_df = spark.sql(query)
result_df.show()

# Save results
result_df.write.mode("overwrite").parquet("/mnt/spark-data/output/sales_by_customer")
```

#### Step 5: Create and Run a Job

```bash
# Create job from notebook
databricks jobs create --json '{
  "name": "spark-sql-demo-job",
  "new_cluster": {
    "spark_version": "11.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2
  },
  "notebook_task": {
    "notebook_path": "/spark-sql-demo/main"
  }
}'

# Run the job
databricks jobs run-now --job-id <job-id>

# Check job status
databricks jobs get-run --run-id <run-id>
```

---

### Option B: Deploy to Azure Synapse Analytics

#### Step 1: Create Synapse Workspace

```bash
# Create storage account for Synapse
az storage account create \
  --name sparksynapsestr \
  --resource-group spark-sql-rg \
  --location eastus

# Create Synapse workspace
az synapse workspace create \
  --name spark-sql-synapse \
  --resource-group spark-sql-rg \
  --storage-account sparksynapsestr \
  --file-system synapsefs \
  --sql-admin-login-user sqladmin \
  --sql-admin-login-password P@ssw0rd123!
```

#### Step 2: Upload Data to Data Lake

```bash
# Get storage account key
STORAGE_KEY=$(az storage account keys list \
  --resource-group spark-sql-rg \
  --account-name sparksynapsestr \
  --query [0].value -o tsv)

# Upload files to blob storage
az storage blob upload-batch \
  --destination synapsefs \
  --source data/ \
  --account-name sparksynapsestr \
  --account-key $STORAGE_KEY \
  --destination-path spark-sql-demo
```

#### Step 3: Create Synapse Spark Pool

```bash
az synapse spark pool create \
  --name sparksql-pool \
  --workspace-name spark-sql-synapse \
  --resource-group spark-sql-rg \
  --spark-version 3.1 \
  --node-count 3
```

#### Step 4: Create Synapse Notebook

```bash
# Create notebook script
cat > synapse_notebook.py << 'EOF'
# Read from Data Lake
customers_df = spark.read.csv("abfss://synapsefs@sparksynapsestr.dfs.core.windows.net/spark-sql-demo/customers.csv", header=True, inferSchema=True)
products_df = spark.read.csv("abfss://synapsefs@sparksynapsestr.dfs.core.windows.net/spark-sql-demo/products.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("abfss://synapsefs@sparksynapsestr.dfs.core.windows.net/spark-sql-demo/orders.csv", header=True, inferSchema=True)

# Register views
customers_df.createOrReplaceTempView("customers")
products_df.createOrReplaceTempView("products")
orders_df.createOrReplaceTempView("orders")

# Execute and display
query = "SELECT c.customer_id, c.customer_name, COUNT(o.order_id) AS orders, SUM(o.total_amount) AS revenue FROM orders o JOIN customers c ON o.customer_id = c.customer_id GROUP BY c.customer_id, c.customer_name ORDER BY revenue DESC"
spark.sql(query).show()

# Save results
spark.sql(query).write.mode("overwrite").parquet("abfss://synapsefs@sparksynapsestr.dfs.core.windows.net/spark-sql-demo/output")
EOF
```

---

### Option C: Deploy via Docker Container

#### Step 1: Create Dockerfile

Create a `Dockerfile` in the project root:

```dockerfile
FROM python:3.9-slim

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk-headless

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy project files
COPY requirements.txt .
COPY main.py .
COPY data/ data/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create output directory
RUN mkdir -p output

# Run the application
CMD ["python", "main.py"]
```

#### Step 2: Build Docker Image

```bash
docker build -t spark-sql-demo:latest .
```

#### Step 3: Run Locally

```bash
docker run -v $(pwd)/output:/app/output spark-sql-demo:latest
```

#### Step 4: Push to Azure Container Registry

```bash
# Create container registry
az acr create \
  --resource-group spark-sql-rg \
  --name sparkcontainerreg \
  --sku Basic

# Login to registry
az acr login --name sparkcontainerreg

# Tag image
docker tag spark-sql-demo:latest sparkcontainerreg.azurecr.io/spark-sql-demo:latest

# Push to registry
docker push sparkcontainerreg.azurecr.io/spark-sql-demo:latest
```

#### Step 5: Deploy to Azure Container Instances

```bash
az container create \
  --resource-group spark-sql-rg \
  --name spark-sql-container \
  --image sparkcontainerreg.azurecr.io/spark-sql-demo:latest \
  --registry-login-server sparkcontainerreg.azurecr.io \
  --registry-username <username> \
  --registry-password <password>
```

---

## GitHub Actions - Automated Deployment

### Step 1: Create GitHub Actions Workflow

Create `.github/workflows/deploy-to-azure.yml`:

```yaml
name: Deploy to Azure

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run tests
      run: python main.py
    
    - name: Login to Azure
      uses: azure/login@v1
      with:
        creds: ${{ secrets.AZURE_CREDENTIALS }}
    
    - name: Upload to Azure Storage
      run: |
        az storage blob upload-batch \
          --destination spark-sql-demo \
          --source . \
          --account-name sparksynapsestr
```

### Step 2: Add GitHub Secrets

```bash
# Generate Azure credentials
az ad sp create-for-rbac --name github-actions --role contributor

# Add to GitHub Secrets:
# AZURE_CREDENTIALS: (output from above command)
```

---

## Monitoring & Troubleshooting

### Check Spark Job Status

```bash
# For Databricks
databricks jobs list
databricks runs list

# For Azure Synapse
az synapse spark job list --workspace-name spark-sql-synapse --on-demand-pool sparksql-pool
```

### View Logs

```bash
# Docker logs
docker logs <container-id>

# Azure Container Instances
az container logs --resource-group spark-sql-rg --name spark-sql-container
```

### Performance Tuning

Add to `main.py` before creating SparkSession:

```python
spark = SparkSession.builder \
    .appName("SparkSQL_Demo") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

---

## Cost Optimization for Azure

1. **Use spot instances** for Databricks clusters
2. **Auto-terminate clusters** after idle time
3. **Schedule jobs** during off-peak hours
4. **Use Synapse dedicated SQL pools** for frequent queries
5. **Enable caching** in Spark for repeated operations

---

## Cleanup

### Local

```bash
deactivate
rm -rf venv/
rm -rf output/
```

### Azure Resources

```bash
# Delete entire resource group (WARNING: deletes all resources)
az group delete --name spark-sql-rg --yes

# Or delete specific resources
az container delete --name spark-sql-container --resource-group spark-sql-rg --yes
az databricks workspace delete --name spark-sql-workspace --resource-group spark-sql-rg
```

---

## Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Azure Databricks Guide](https://docs.microsoft.com/en-us/azure/databricks/)
- [Azure Synapse Analytics](https://docs.microsoft.com/en-us/azure/synapse-analytics/)
- [Docker Documentation](https://docs.docker.com/)
- [GitHub Actions for Azure](https://github.com/Azure/actions)

