FROM python:3.6.3


COPY main/src/* /usr/src/

RUN pip install confluent-kafka
RUN pip install netCDF4
RUN pip install xmltodict

WORKDIR /usr/src/CRCL/FireCRisisCLassification/
RUN wget https://www.dropbox.com/s/41usi36d7b7g0s3/20180612_fwishort.tar?dl=0

WORKDIR /usr/src/
RUN wget https://www.dropbox.com/s/7ehd4le7bl2yep4/bus_credentials.json?dl=0 

CMD [ "python", "./mainCRCL.py" ]