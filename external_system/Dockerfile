FROM python:3.11-slim

WORKDIR /app

# Install Flask
RUN pip install --no-cache-dir flask==2.3.3

# Copy app
COPY app.py .

# Expose port
EXPOSE 5001

# Run app
CMD ["python", "app.py"]