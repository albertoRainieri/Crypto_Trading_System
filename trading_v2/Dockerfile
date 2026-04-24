# Use the official Playwright image — it ships with all Chromium system
# dependencies pre-installed (libglib, libnss, libatk, libcups, etc.).
# This avoids the painful apt-get dependency list required on plain Python images.
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

WORKDIR /app

# Copy dependency manifest first for better layer caching.
COPY requirements.txt .

# Install Python packages.
RUN pip install --no-cache-dir -r requirements.txt

# Playwright browsers are already baked into the base image, but install
# ensures the version matches the pip package if they ever drift.
RUN playwright install chromium --with-deps

# Copy application source.
COPY financialjuice_live.py .

# Create debug directory and make it writable by pwuser before switching user.
RUN mkdir -p /app/debug && chown -R pwuser /app/debug && chmod -R 777 /app/debug

# Chromium requires a non-root user in some kernel configurations.
# The base image already provides a 'pwuser' for this purpose.
USER pwuser

# Run the live news capture script.
CMD ["python3", "-u", "financialjuice_live.py"]
