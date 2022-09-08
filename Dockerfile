FROM python:3.9-alpine

ENV DIR_PATH='/sheetmaker/'
ENV DB_DIR='/dealcatcher/db/'

RUN mkdir -p /sheetmaker
RUN mkdir -p /dealcatcher/
RUN pip install pygsheets

COPY . /sheetmaker

CMD ["python3", "/sheetmaker/main.py"]