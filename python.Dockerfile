FROM python:3.9-slim
ENV PYTHONUNBUFFERED 1

RUN mkdir /code
VOLUME /code
ADD . /code
WORKDIR /code

RUN pip3 install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 5000

RUN mkdir logs
CMD ["python"]
