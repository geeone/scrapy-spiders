FROM scrapinghub/scrapinghub-stack-scrapy:2.3-latest

RUN apt-get update -qq
RUN apt-get install -qy wkhtmltopdf

ENV TERM xterm
ENV SCRAPY_SETTINGS_MODULE brobot_bots.settings
RUN mkdir -p /app
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
RUN python setup.py install
RUN chmod -R 777 /usr/local/lib/python3.8/site-packages/autos_prefeitura_sp-1.0-py3.8.egg/