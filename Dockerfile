FROM python

WORKDIR /patients

COPY . /patients

RUN pip install -r requirements.txt

CMD python ./patients.py

