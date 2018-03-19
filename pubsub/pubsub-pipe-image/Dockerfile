FROM python:3

RUN pip install --upgrade pip
RUN pip install tweepy
RUN pip install --upgrade google-api-python-client
RUN pip install python-dateutil
RUN pip install google-auth
RUN pip install pandas-gbq
RUN pip install pandas
RUN pip install nltk

ADD twitter-to-pubsub.py /twitter-to-pubsub.py
ADD pubsub-to-bigquery.py /pubsub-to-bigquery.py
ADD controller.py /controller.py
ADD utils.py /utils.py
ADD gcp_tw_bq_ps_creds.json /

CMD python controller.py
