FROM python:3.8-slim
WORKDIR /app
COPY module module
COPY kafkaToS3.py kafkaToS3.py
RUN pip install pyyaml
RUN pip install -r module/requirements.txt
#ENTRYPOINT ["python3"]
ENV PYTHONUNBUFFERED=1
CMD ["python3", "./kafkaToS3.py"]

