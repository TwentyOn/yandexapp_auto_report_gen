import os

import dotenv

dotenv.load_dotenv()

# Файл содержит настройки или перменные среды для использования в проекте

# Yandex App API
YAPP_TOKEN = os.getenv('YAPP_TOKEN')

# Yandex direct API
YANDEX_DIRECT_TOKEN = os.getenv('YANDEX_DIRECT_TOKEN')

# База данных
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Minio S3 хранилище
ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
SECRET_KEY = os.getenv('S3_SECRET_KEY')
# особо важный параметр при развертывании
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
MINIO_SECURE = os.getenv('S3_MINIO_SECURE')
OUTER_ENDPOINT_URL = os.getenv('S3_OUTER_ENDPOINT_URL')