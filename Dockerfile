# Use the official Python image.
FROM python:3.10-slim

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -yq --no-install-recommends \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libcairo2 libx11-xcb1

# Set the working directory.
WORKDIR /app

# Copy local code to the container image.
COPY . .

# Install Python dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install --with-deps

# Run the web service on container startup.
# Use Gunicorn for production
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
