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
