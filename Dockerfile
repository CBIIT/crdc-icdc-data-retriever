FROM python:3.13-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV CONFIG_FILE=config/icdc.yaml
ENV PYTHONUNBUFFERED=1

CMD ["sh", "-c", "python main.py --config $CONFIG_FILE"]
