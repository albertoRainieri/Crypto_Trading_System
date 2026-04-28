# Use the official Playwright image — it ships with all Chromium system
# dependencies pre-installed (libglib, libnss, libatk, libcups, etc.).
# This avoids the painful apt-get dependency list required on plain Python images.
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

WORKDIR /app

# Copy dependency manifest first for better layer caching.
COPY requirements.txt .

# Install Python packages (includes playwright + camoufox).
RUN pip install --no-cache-dir -r requirements.txt

# Chromium is kept for persistent-profile sessions (FJ_PROFILE_DIR path).
RUN playwright install chromium --with-deps

# Install Firefox system dependencies that camoufox's bundled browser needs.
# Camoufox ships its own Firefox binary; this just ensures required system libs
# (libgtk-3, libdbus, etc.) are present on the base image.
RUN playwright install firefox --with-deps

# Copy application source.
COPY financialjuice_live.py .

# Download the camoufox patched Firefox binary (~150 MB).
# camoufox stores the binary inside its own pip package directory, which is
# already world-readable — no chown needed.
RUN python -m camoufox fetch

# Create debug directory and prepare a writable home for pwuser.
RUN mkdir -p /app/debug /home/pwuser \
    && chown -R pwuser /app/debug /home/pwuser \
    && chmod -R 777 /app/debug

# Chromium requires a non-root user in some kernel configurations.
# The base image already provides a 'pwuser' for this purpose.
USER pwuser

# Run the live news capture script.
CMD ["python3", "-u", "financialjuice_live.py"]
