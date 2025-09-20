FROM python:latest
RUN pip install -r requirements.txt
COPY src/* ./
CMD ["python", "./src/land_storage.py"]