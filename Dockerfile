FROM python:3.12-slim

# --------------------------
# System dependencies
# --------------------------
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --------------------------
# Install Pipenv
# --------------------------
COPY Pipfile Pipfile.lock /app/
RUN pip install pipenv
RUN pipenv install --system --deploy

# --------------------------
# Copy application code
# --------------------------
COPY . /app

# --------------------------
# Run scheduler + Streamlit
# --------------------------
CMD ["bash", "-c", "python main.py & streamlit run streamlit_main.py --server.port=8501 --server.address=0.0.0.0"]
