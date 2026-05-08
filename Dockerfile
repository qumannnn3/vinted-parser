FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV BOT_ACCESS_FILE=/data/authorized_users.json
ENV BOT_USER_STATE_FILE=/data/user_profiles.json

RUN mkdir -p /data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "vinted_bot.py"]
