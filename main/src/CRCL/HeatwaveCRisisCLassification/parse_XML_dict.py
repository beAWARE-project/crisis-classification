from datetime import datetime
import pandas as pd
import urllib.request
import xmltodict, json
from collections import OrderedDict

def parse_XML_dict(resp, colnames):

    # resp : dictionary generated by the gml file
    # colnames: parameters for request forecasting values

    dict_result = {}

    timestamp = resp['wfs:FeatureCollection']['@timeStamp']
    dict_result.update({'timestamp': timestamp})

    beginTime = resp['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:phenomenonTime']['gml:TimePeriod']['gml:beginPosition']
    dict_result.update({'beginTime': beginTime})

    endTime = resp['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:phenomenonTime']['gml:TimePeriod']['gml:endPosition']
    dict_result.update({'endTime': endTime})

    TimeMeas = resp['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:resultTime']['gml:TimeInstant']['gml:timePosition']
    dict_result.update({'TimeMeasurement': TimeMeas})

    http_params = resp['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:observedProperty']['@xlink:href']
    dict_result.update({'http_params': http_params})

    name = resp['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:featureOfInterest']['sams:SF_SpatialSamplingFeature']['sams:shape']['gml:MultiPoint']['gml:pointMembers']['gml:Point']['gml:name']
    dict_result.update({'name': name})

    pos = resp['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:featureOfInterest']['sams:SF_SpatialSamplingFeature']['sams:shape']['gml:MultiPoint']['gml:pointMembers']['gml:Point']['gml:pos']
    dict_result.update({'position': pos})

    # Data.frame of positions and phenomenonTime
    df_pos = resp['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:result']['gmlcov:MultiPointCoverage']['gml:domainSet']['gmlcov:SimpleMultiPoint']['gmlcov:positions']

    df_pos1 = df_pos.replace(" ", ",").strip("")
    df_pos1 = df_pos1.replace(",,,,,,,,,,,,,,,,", "")
    df_pos2 = df_pos1.splitlines()

    data_pos_phenTime = []
    for i in range(0,len(df_pos2)):
        p = df_pos2[i]
        p = p.split(",")
        if len(p) > 3 : p.remove('')

        pos_phenTime = []
        for j in range(0, len(p)):
            if type(p[j]) == str and j < (len(p)-1):
                pos_phenTime.append( float(p[j]) )
            else:
                pDT = datetime.utcfromtimestamp( int(p[2]) ).isoformat()
                pos_phenTime.append(pDT)

        # update the data frame with new entry
        data_pos_phenTime.append(pos_phenTime)

    # create a data frame with lat/long and phenomenonTime
    df_pos_phenTime = pd.DataFrame(data=data_pos_phenTime, columns=['Lat','Long','phenomenonTime'])


    # Forecasting values of each parameter
    df_res = resp['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:result']['gmlcov:MultiPointCoverage']['gml:rangeSet']['gml:DataBlock']['gml:doubleOrNilReasonTupleList']

    df = df_res.replace(" ", ",").strip("")
    df1 = df.replace(",,,,,,,,,,,,,,,,", "")
    df2 = df1.splitlines()

    data_vals = []
    for i in range( len(df2) ):
        t = df2[i].split(",")
        if len(t) > len(colnames) : t.remove('')

        t2 = []
        for j in range(0,len(t)):
            if type(t[j]) == str:
                t2.append( float(t[j]) )

        data_vals.append(t2)

    # Create a dataframe with forecasted values per parameter
    data = pd.DataFrame(data=data_vals, columns=colnames)

    # Concatenate the two data frames
    newdataset = pd.concat([df_pos_phenTime, data], axis=1)

    dict_result.update({'Data': newdataset})

    return dict_result


#----------------------------------------------------------------------
def parse_XML_to_dict_querylist(qrlist, points, directory):

    # data frame to return
    df = pd.DataFrame(columns=['Name', 'Lat', 'Long', 'DateTime', 'Temperature', 'Humidity'])

    for qrit in range(0, len(qrlist)):

        with urllib.request.urlopen(qrlist[qrit]) as fd:
            query = xmltodict.parse(fd.read())

        # write dictionary from xml file to output file
        xmlflname = directory + "/" + 'response_forecast' + "_" + str(qrit) + ".txt"
        with open(xmlflname, 'w') as outfile:
            json.dump(OrderedDict(query), outfile)

        qr = query['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:result']['gmlcov:MultiPointCoverage']['gml:domainSet']['gmlcov:SimpleMultiPoint']['gmlcov:positions']
        qr1 = qr.split("\n")

        st = []
        for i in range(len(qr1)):
            t = qr1[i].lstrip().split(" ")
            t.remove(t[2])

            # transform integer to date/time
            dtm = datetime.utcfromtimestamp(int(t[2])).isoformat()
            t.remove(t[2])
            t.append(dtm)

            st.append(t)

        name = [points[qrit]['name']] * len(st)

        ds = pd.DataFrame()
        ds['Measurement_Number'] = list(range(1,len(st)+1))
        ds['Name'] = name
        ds2 = pd.DataFrame(st, columns=['Lat', 'Long', 'DateTime'])

        # Concatenate the two data frames by column
        ds = pd.concat([ds, ds2], axis=1)

        # Extract data from xml file for the parameters

        qr = query['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:result']['gmlcov:MultiPointCoverage']['gml:rangeSet']['gml:DataBlock']['gml:doubleOrNilReasonTupleList']
        qr2 = qr.split("\n")

        st = []
        for i in range(len(qr2)):
            t = qr2[i].lstrip().split(" ")
            if len(t) == 3:
                t.remove(t[2])

            for j in range(len(t)):
                st.append(float(t[j]))

        Temp = []
        Humid = []
        for it in range(0, len(st), 2):
            Temp.append(st[it])
            Humid.append(st[it + 1])

        # Add two new lists to the dataframe ds
        ds['Temperature'] = Temp
        ds['Humidity'] = Humid

        # Drop NaNs from data frame
        cleaned_ds = ds.dropna()

        df = pd.concat([df, cleaned_ds], ignore_index=True, sort=True)

    return(df)


#----------------------------------------------------------------------
def parse_json_to_dict_querylist(qrlist, points, directory):

    # data frame to return
    df = pd.DataFrame(columns=['Name', 'Lat', 'Long', 'DateTime', 'Temperature', 'Humidity'])

    # write dictionary to output file
    outflname = directory + "/" + "responses_RealTime_Weather_Measurements.txt"
    outfln = open(outflname, 'w')

    for qrit in range(0, len(qrlist)):

        # read from url - execute the query and the response is stored to json obj
        with urllib.request.urlopen(qrlist[qrit]) as url:
            response = json.loads(url.read().decode())

        # Write the response (jsons) to file
        json.dump(OrderedDict(response), outfln)
        outfln.write("\n ------------------------------ \n")

        ds = []
        ds.append( points[qrit]['name'] )
        ds.append( response['latitude'] )
        ds.append( response['longitude'] )

        # transform integer to date/time
        dtm = datetime.utcfromtimestamp(int( response['currently']['time'] )).isoformat()
        ds.append( dtm )

        ds.append( response['currently']['temperature'] )
        ds.append( response['currently']['humidity'] )

        df.loc[len(df)] = ds

    return(df)