FROM python:3.7-slim

RUN pip install --no-cache-dir flask pyarrow requests pandas

COPY app.py /api/app.py

EXPOSE 5000

CMD ["python", "/api/app.py"]
