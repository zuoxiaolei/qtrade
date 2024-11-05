FROM python:3.12
WORKDIR /workspace/search_service
COPY * /workspace/search_service/
RUN python3 -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
CMD gunicorn search_service:app -b 0.0.0.0:8081 -w 1 --threads 100 --timeout 1000 -k uvicorn.workers.UvicornWorker