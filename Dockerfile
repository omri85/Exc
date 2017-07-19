FROM python:2.7.13-alpine
ADD lib/. /code
WORKDIR /code
RUN pip install -r requirements.txt
EXPOSE 5000
CMD ["python", "advertima_restapi.py"]