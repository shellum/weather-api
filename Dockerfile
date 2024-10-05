FROM python:3.9
WORKDIR /
COPY requirements.txt /
COPY weather_api.py /
RUN pip install -r requirements.txt
RUN apt update
RUN apt install -y iputils-ping telnet
CMD ["python", "weather_api.py"]