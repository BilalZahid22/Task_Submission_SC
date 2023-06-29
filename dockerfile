#Deriving the latest base image
FROM python:latest

WORKDIR /usr/app/src

#to COPY the remote file at working directory in container
COPY . ./

RUN pip install -r requirements.txt

CMD [ "python", "./main.py"]