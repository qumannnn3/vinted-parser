# Используем легкий образ Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файл с зависимостями
COPY requirements.txt .

# Устанавливаем библиотеки
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код бота
COPY . .

# Команда запуска бота (убедись, что имя файла совпадает)
CMD ["python", "vinted_bot (1).py"]
