FROM python:3.11-slim-bullseye
EXPOSE 8080
RUN apt-get update && pip install --upgrade pip
RUN pip install aiohttp==3.8.6
RUN pip install asyncio==3.4.3
RUN pip install requests==2.31.0
ADD sub.py ./
CMD ["python", "-u", "./sub1.py"]