FROM python:3.6.3

COPY main/src /usr/src

RUN pip install --upgrade pip
RUN pip install confluent-kafka
RUN pip install netCDF4
RUN pip install xmltodict
RUN pip install requests
RUN pip install pandas
RUN pip install matplotlib
RUN pip install scipy
RUN pip install xlrd 
RUN pip install openpyxl

WORKDIR /usr/src/CRCL/FireCRisisCLassification
RUN wget https://www.dropbox.com/s/41usi36d7b7g0s3/20180612_fwishort.tar

WORKDIR /usr/src
RUN wget https://www.dropbox.com/s/7ehd4le7bl2yep4/bus_credentials.json

CMD [ "python", "./mainCRCL.py" ]
