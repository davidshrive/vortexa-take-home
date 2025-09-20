FROM python:latest
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY src/* ./
CMD ["python", "./land_storage.py"]