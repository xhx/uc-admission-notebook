FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install jupyter

COPY . .

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=5000", "--no-browser", "--allow-root"]