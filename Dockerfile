# Dockerfile
FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
# Add --default-timeout to increase the download timeout for pip  ~ about 18 minutes
RUN pip install --no-cache-dir -r requirements.txt --default-timeout=1000

# Copy the rest of your application code
COPY . .

# Expose the port FastAPI will run on (default 8000)
EXPOSE 8000

# Command to run the FastAPI application using Uvicorn (overridden by docker-compose for specific services)
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]