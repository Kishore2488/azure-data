# Local & Cloud Execution Guide

## Table of Contents
1. [Local Execution](#local-execution)
2. [Azure Databricks Deployment](#azure-databricks-deployment)
3. [Azure Synapse Analytics Deployment](#azure-synapse-analytics-deployment)
4. [Azure Container Instances Deployment](#azure-container-instances-deployment)
5. [Microsoft Fabric Deployment](#microsoft-fabric-deployment)
6. [GitHub Actions CI/CD](#github-actions-cicd)
7. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
8. [Cost Optimization](#cost-optimization)

---

## Local Execution

### Prerequisites
- Python 3.8+
- Java 8 or later (required by Spark)
- Git
- pip (Python package manager)

### Step 1: Clone Repository

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
java -version
```

### Step 5: Run the Application

```bash
python main.py
```

### Expected Output

The script will display:
- ✓ Customers table (8 rows)
- ✓ Products table (8 rows)
- ✓ Orders table (12 rows)
- ✓ Customer Orders JOIN (12 rows with product details)
- ✓ Aggregated sales by customer
- ✓ High-value orders (>$300)
- ✓ Sales by product category
- ✓ Top 5 customers by revenue
- ✓ Summary statistics
- ✓ Output files saved to `output/` directory

### Step 6: View Results

```bash
# View directory structure
ls -la output/

# View Parquet results
python -c "import pandas as pd; df = pd.read_parquet('output/customer_orders'); print(df.head())"

# View CSV results
cat output/sales_by_customer/part-*.csv
```

### Step 7: Stop Virtual Environment

```bash
deactivate
```

---

## Azure Databricks Deployment

### Prerequisites
- Azure subscription
- Azure CLI installed
- `az login` completed

### Step 1: Create Resource Group

```bash
az group create --name spark-sql-rg --location eastus
```

### Step 2: Create Databricks Workspace

```bash
az databricks workspace create \
  --resource-group spark-sql-rg \
  --name spark-sql-workspace \
  --location eastus \
  --sku premium
```

### Step 3: Install and Configure Databricks CLI

```bash
pip install databricks-cli

databricks configure --token
# Enter host: https://<region>.azuredatabricks.net
# Enter token: <your-databricks-personal-token>
```

### Step 4: Create DBFS Directory and Upload Files

```bash
# Create directory
databricks fs mkdirs dbfs:/spark-sql-demo

# Upload data files
databricks fs cp data/customers.csv dbfs:/spark-sql-demo/customers.csv --overwrite
databricks fs cp data/products.csv dbfs:/spark-sql-demo/products.csv --overwrite
databricks fs cp data/orders.csv dbfs:/spark-sql-demo/orders.csv --overwrite

# Upload main script
databricks fs cp main.py dbfs:/spark-sql-demo/main.py --overwrite
```

### Step 5: Create Databricks Notebook

1. Log in to Databricks workspace
2. Click "Create" → "Notebook"
3. Name: `spark-sql-demo`
4. Language: Python
5. Cluster: Create or select existing

### Step 6: Add Code to Notebook

```python
# Databricks notebook source
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
    SUM(o.total_amount) AS total_spent,
    AVG(o.total_amount) AS avg_order
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent DESC
"""

result_df = spark.sql(query)
result_df.show()

# Save results
result_df.write.mode("overwrite").parquet("dbfs:/spark-sql-demo/output/sales_by_customer")
```

### Step 7: Run Notebook

1. Click "Run" → "Run All"
2. Monitor execution
3. Check results in Spark UI

### Step 8: View Results

```bash
# Download results
databricks fs cp dbfs:/spark-sql-demo/output/sales_by_customer -r output/ --overwrite
```

---

## Azure Synapse Analytics Deployment

### Prerequisites
- Azure subscription
- Azure CLI installed

### Step 1: Create Storage Account

```bash
az storage account create \
  --name synapsestorage123 \
  --resource-group spark-sql-rg \
  --location eastus \
  --sku Standard_LRS
```

### Step 2: Create Synapse Workspace

```bash
az synapse workspace create \
  --name spark-sql-synapse \
  --resource-group spark-sql-rg \
  --storage-account synapsestorage123 \
  --file-system synapsefs \
  --sql-admin-login-user sqladmin \
  --sql-admin-login-password P@ssw0rd123!
```

### Step 3: Create Spark Pool

```bash
az synapse spark pool create \
  --name sparksql-pool \
  --workspace-name spark-sql-synapse \
  --resource-group spark-sql-rg \
  --spark-version 3.1 \
  --node-count 3 \
  --node-size Small
```

### Step 4: Upload Data to Storage

```bash
# Get storage key
STORAGE_KEY=$(az storage account keys list \
  --resource-group spark-sql-rg \
  --account-name synapsestorage123 \
  --query [0].value -o tsv)

# Upload files
az storage blob upload-batch \
  --destination synapsefs \
  --source data/ \
  --account-name synapsestorage123 \
  --account-key $STORAGE_KEY \
  --destination-path spark-sql-demo
```

### Step 5: Create Synapse Notebook

1. Go to https://web.azuresynapse.net/
2. Select workspace: `spark-sql-synapse`
3. Click "Create" → "Notebook"
4. Attach to pool: `sparksql-pool`

### Step 6: Add Code to Notebook

```python
# Read from Data Lake
customers_df = spark.read.csv(
    "abfss://synapsefs@synapsestorage123.dfs.core.windows.net/spark-sql-demo/customers.csv",
    header=True, inferSchema=True)
products_df = spark.read.csv(
    "abfss://synapsefs@synapsestorage123.dfs.core.windows.net/spark-sql-demo/products.csv",
    header=True, inferSchema=True)
orders_df = spark.read.csv(
    "abfss://synapsefs@synapsestorage123.dfs.core.windows.net/spark-sql-demo/orders.csv",
    header=True, inferSchema=True)

# Register views
customers_df.createOrReplaceTempView("customers")
products_df.createOrReplaceTempView("products")
orders_df.createOrReplaceTempView("orders")

# Execute queries
query = """
SELECT c.customer_id, c.customer_name, COUNT(o.order_id) AS orders, SUM(o.total_amount) AS revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY revenue DESC
"""

spark.sql(query).show()

# Save results
spark.sql(query).write.mode("overwrite").parquet(
    "abfss://synapsefs@synapsestorage123.dfs.core.windows.net/spark-sql-demo/output")
```

### Step 7: Run Notebook

1. Click "Run All"
2. Monitor in Spark History
3. Check output in Data Lake

---

## Azure Container Instances Deployment

### Prerequisites
- Docker installed locally
- Azure CLI installed

### Step 1: Create Container Registry

```bash
az acr create \
  --resource-group spark-sql-rg \
  --name sparkcontainerreg \
  --sku Basic
```

### Step 2: Build Docker Image

```bash
docker build -t spark-sql-demo:latest .
```

### Step 3: Tag Image

```bash
docker tag spark-sql-demo:latest sparkcontainerreg.azurecr.io/spark-sql-demo:latest
```

### Step 4: Login to Registry

```bash
az acr login --name sparkcontainerreg
```

### Step 5: Push to Registry

```bash
docker push sparkcontainerreg.azurecr.io/spark-sql-demo:latest
```

### Step 6: Deploy to Container Instances

```bash
az container create \
  --resource-group spark-sql-rg \
  --name spark-sql-container \
  --image sparkcontainerreg.azurecr.io/spark-sql-demo:latest \
  --cpu 2 \
  --memory 4
```

### Step 7: Monitor Container

```bash
# Get container status
az container show \
  --resource-group spark-sql-rg \
  --name spark-sql-container \
  --query "[ipAddress.ip, containers[0].instanceView.currentState.state]"

# View logs
az container logs \
  --resource-group spark-sql-rg \
  --name spark-sql-container
```

### Step 8: Download Results

```bash
# Connect to container and download outputs
az container exec \
  --resource-group spark-sql-rg \
  --name spark-sql-container \
  --exec-command "/bin/bash"
```

---

## Microsoft Fabric Deployment

### Prerequisites
- Microsoft Fabric capacity (F2 or higher)
- Power BI subscription
- Access to https://app.powerbi.com/

### Step 1: Create Workspace

1. Go to https://app.powerbi.com/
2. Click "Workspaces" in left sidebar
3. Click "+ New workspace"
4. Enter name: `spark-sql-demo`
5. Select capacity: F2 (Trial) or higher
6. Click "Apply"

### Step 2: Create Lakehouse

1. In workspace, click "+ New"
2. Select "Lakehouse"
3. Enter name: `spark_sql_data`
4. Click "Create"

### Step 3: Create Spark Notebook

1. Click "+ New" → "Notebook"
2. Name: `spark-sql-demo`
3. Paste code from `fabric-notebook.py`

### Step 4: Run Notebook Cells

Run each cell sequentially:
- Cell 1: Create sample data
- Cell 2: Create Delta tables
- Cell 3: Display tables
- Cell 4: JOIN query
- Cell 5: AGGREGATION query
- Cell 6: CATEGORY analysis
- Cell 7: TOP customers
- Cell 8: Summary statistics

### Step 5: Verify Delta Tables

1. In Explorer panel, expand Lakehouse
2. Verify tables created:
   - `customers`
   - `products`
   - `orders`
   - `customer_orders_details`
   - `customer_sales_summary`

### Step 6: Create SQL Query

1. Click "+ New" → "SQL Query"
2. Write query:

```sql
SELECT 
    c.customer_name,
    COUNT(o.order_id) AS orders,
    SUM(o.total_amount) AS revenue
FROM customer_sales_summary c
GROUP BY c.customer_name
ORDER BY revenue DESC
```

3. Click "Run"

### Step 7: Create Power BI Report

1. Click "+ New" → "Report"
2. Select Lakehouse as data source
3. Add visualizations:
   - Bar chart: Revenue by customer
   - Table: Customer details
   - KPI: Total revenue

### Step 8: Schedule Pipeline (Optional)

1. Click "+ New" → "Data Pipeline"
2. Add notebook activity
3. Configure schedule
4. Set frequency: Daily/Weekly

---

## GitHub Actions CI/CD

### Step 1: Add Test Workflow

Create `.github/workflows/test.yml`:

```yaml
name: Test & Validate

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run Spark application
      run: python main.py
    
    - name: Upload results
      uses: actions/upload-artifact@v3
      with:
        name: spark-output
        path: output/
```

### Step 2: Add Databricks Deployment

Create `.github/workflows/deploy-databricks.yml`:

```yaml
name: Deploy to Databricks

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Install Databricks CLI
      run: pip install databricks-cli
    
    - name: Configure Databricks
      env:
        DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
        DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      run: |
        mkdir -p ~/.databricks
        echo "host = $DATABRICKS_HOST" > ~/.databricks/config
        echo "token = $DATABRICKS_TOKEN" >> ~/.databricks/config
    
    - name: Upload files
      run: |
        databricks fs cp data/customers.csv dbfs:/spark-sql-demo/customers.csv --overwrite
        databricks fs cp data/products.csv dbfs:/spark-sql-demo/products.csv --overwrite
        databricks fs cp data/orders.csv dbfs:/spark-sql-demo/orders.csv --overwrite
```

### Step 3: Add Synapse Deployment

Create `.github/workflows/deploy-synapse.yml`:

```yaml
name: Deploy to Synapse

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Login to Azure
      uses: azure/login@v1
      with:
        creds: ${{ secrets.AZURE_CREDENTIALS }}
    
    - name: Upload to Storage
      run: |
        az storage blob upload-batch \
          --destination synapsefs \
          --source data/ \
          --account-name ${{ secrets.SYNAPSE_STORAGE_ACCOUNT }} \
          --account-key ${{ secrets.SYNAPSE_STORAGE_KEY }} \
          --destination-path spark-sql-demo
```

### Step 4: Add Container Deployment

Create `.github/workflows/deploy-container.yml`:

```yaml
name: Deploy to Container Registry

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Login to Azure
      uses: azure/login@v1
      with:
        creds: ${{ secrets.AZURE_CREDENTIALS }}
    
    - name: Build and push image
      run: |
        az acr build \
          --registry ${{ secrets.AZURE_REGISTRY_NAME }} \
          --image spark-sql-demo:latest .
```

### Step 5: Configure Secrets

1. Go to GitHub repository
2. Settings → Secrets → New repository secret
3. Add secrets:
   - `AZURE_CREDENTIALS`
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`
   - `SYNAPSE_STORAGE_ACCOUNT`
   - `SYNAPSE_STORAGE_KEY`
   - `AZURE_REGISTRY_NAME`

---

## Monitoring & Troubleshooting

### Common Issues

**1. PySpark not found**
```bash
# Install Java first
java -version

# Reinstall PySpark
pip install --force-reinstall pyspark==3.5.0
```

**2. JAVA_HOME not set**
```bash
# Linux/Mac
export JAVA_HOME=/path/to/java
export PATH=$JAVA_HOME/bin:$PATH

# Windows - Set in Environment Variables
```

**3. Permission denied on DBFS**
- Check Databricks token validity
- Verify credentials in `.databricks/config`

**4. Container won't start**
```bash
# Check logs
docker logs <container-id>

# Increase resources
az container delete --name spark-sql-container --resource-group spark-sql-rg
# Then recreate with more memory/CPU
```

### Monitoring Commands

```bash
# Check Databricks jobs
databricks jobs list
databricks runs list

# Check Synapse
az synapse spark session list --workspace-name spark-sql-synapse

# Check containers
az container list --resource-group spark-sql-rg

# View container logs
az container logs --resource-group spark-sql-rg --name spark-sql-container
```

---

## Cost Optimization

### Databricks
- Use spot instances: Save 60-70%
- Auto-terminate clusters after 15 min
- Resize clusters based on workload
- Use standard nodes for non-critical tasks

### Synapse
- Use on-demand (no reserved capacity)
- Pause dedicated pools when not in use
- Optimize query performance
- Use serverless Spark for interactive queries

### Fabric
- Start with F2 capacity
- Scale up only when needed
- Monitor capacity metrics
- Optimize notebook execution

### Container Instances
- Use spot instances for testing
- Set appropriate CPU/memory limits
- Use scheduled scaling
- Clean up containers after use

### Estimated Monthly Costs

| Service | Dev | Prod |
|---------|-----|------|
| Databricks | $100 | $500+ |
| Synapse | $50 | $300+ |
| Fabric | $200 | $500+ |
| Container | $10 | $100+ |
| Local Dev | $0 | $0 |

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
# Delete all resources
az group delete --name spark-sql-rg --yes

# Delete specific resources
az databricks workspace delete --name spark-sql-workspace --resource-group spark-sql-rg
az synapse workspace delete --name spark-sql-synapse --resource-group spark-sql-rg
az container delete --name spark-sql-container --resource-group spark-sql-rg
```

---

## Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Azure Databricks Guide](https://docs.microsoft.com/azure/databricks/)
- [Azure Synapse Analytics](https://docs.microsoft.com/azure/synapse-analytics/)
- [Microsoft Fabric](https://learn.microsoft.com/fabric/)
- [Docker Documentation](https://docs.docker.com/)
- [GitHub Actions](https://github.com/features/actions)

---

## Support & Troubleshooting

For issues:
1. Check the documentation files in the repo
2. Review the logs and error messages
3. Verify credentials and permissions
4. Check Azure quota limits
5. Consult official documentation links above
