import blpapi
from datetime import datetime, timedelta
import json
import re

# Init API session
def start_session():
    options = blpapi.SessionOptions()
    options.setServerHost('localhost')
    options.setServerPort(8194)

    session = blpapi.Session(options)

    if not session.start():
        print('Failed to start session.')

    return session

# Get a list associated with an instrument, a curve or government(?)
def instruments(query, country, maxResults, request_type):
    """instrumentListRequest
    curveListRequest
    govtListRequest
    """

    session = start_session()

    if not session.openService('//blp/instruments'):
        print('Failed to open //blp//instruments')
        return

    # Obtain the historical data
    refDataService = session.getService('//blp/instruments')

    # Create and fill the request for the list: 'curveListRequest'
    request = refDataService.createRequest(request_type)

    request.asElement().setElement('query', query)
    request.asElement().setElement('countryCode', country)
    request.asElement().setElement('maxResults', maxResults)

    session.sendRequest(request)

    msg_list = []
    while(True):
        event = session.nextEvent(500)
        for msg in event:
            msg_list.append(msg)

        if event.eventType() == blpapi.Event.RESPONSE:
            # Response completely received, so we could exit
            break
    
    return msg_list


# Get the curve code for the DI contracts
query= 'DI'
country = 'BR'
maxResults = 100
request_type= 'curveListRequest'

msg_list = instruments(query, country, maxResults, request_type)

# curve = "YCSW0089 Index"
# description = "BM&F Pre x DI Curve"

# curve = "YCMM0119 Index"
# description = "Brazil PRE-DI SWAP Curve"

# curve = "YCSW0388 Index"
# description = "Offsh. Pre x DI Curve"

# curve = "YCSW0090 Index"
# description = "Contributed Pre x DI Curve"


# Get reference data for a ticker 
def refDataRequest(security,fields):

    session = start_session()
    if not session.openService('//blp/refdata'):
        print('Failed to open //blp//refdata')
        return

    refDataService = session.getService('//blp/refdata')
    request = refDataService.createRequest('ReferenceDataRequest')
    request.append('securities',security)
    # Set the fields for which to get data
    for i in fields:
        request.append('fields',i)

    request.set('returnFormattedValue', False)

    session.sendRequest(request)

    # process the response
    msg_list = []
    while(True):
        event = session.nextEvent(500)
        for msg in event:
            msg_list.append(msg)

        if event.eventType() == blpapi.Event.RESPONSE:
            # Response completely received, so we could exit
            break

    return msg_list



# Get the contracts 
s_name = 'YCSW0089 Index'
fields = ['CURVE_TENOR_RATES']

msg_list = refDataRequest(s_name,fields)
list_elements = [i.asElement().hasElement('securityData') for i in msg_list]
index_msg = list_elements.index(True)
data_s = str(msg_list[index_msg])
di_codes = re.findall(r'Tenor Ticker = "(.*?)"', data_s)

def get_messages(request):

    session.sendRequest(request)

    # process the response
    msg_list = []
    while(True):
        event = session.nextEvent(500)
        for msg in event:
            msg_list.append(msg)

        if event.eventType() == blpapi.Event.RESPONSE:
            # Response completely received, so we could exit
            break

    return msg_list



def intradayBarRequest(s_name,field, start_date,end_date,interval):

    session = start_session()

    if not session.openService('//blp/refdata'):
        print('Failed to open //blp//refdata')
        return

    # Obtain the historical data
    refDataService = session.getService('//blp/refdata')

    request = refDataService.createRequest('IntradayBarRequest')
    # Set the security (e.g., stock name)
    request.set('security',s_name)
    request.set('eventType',field)
    request.set('interval',interval)
        # Set the date range
    request.set('startDateTime', start_date)
    request.set('endDateTime', end_date)

    session.sendRequest(request)

    # process the response
    msg_list = []
    while(True):
        event = session.nextEvent(500)
        for msg in event:
            msg_list.append(msg)

        if event.eventType() == blpapi.Event.RESPONSE:
            # Response completely received, so we could exit
            break

    return msg_list



s_name = 'ODF24 Comdty'
field = 'TRADE'
start_date = '2023-09-21T10:00:00'
end_date = '2023-09-22T10:00:00'
interval = 60

msg_list = intradayBarRequest(s_name,field, start_date,end_date,interval)
print(msg_list[3])


# Get Historical Data
def HistoricalDataRequest(s_name,fields, start_date,end_date):

    session = start_session()

    if not session.openService('//blp/refdata'):
        print('Failed to open //blp//refdata')
        return

    # Obtain the historical data
    refDataService = session.getService('//blp/refdata')

    request = refDataService.createRequest('HistoricalDataRequest')
    # Set the date range
    request.set('startDate', start_date)
    request.set('endDate', end_date)
    # Set the security (e.g., stock name)
    request.append('securities',s_name)
    # Set the fields for which to get data
    for i in fields:
        request.append('fields',i)

    session.sendRequest(request)

    # process the response
    msg_list = []
    while(True):
        event = session.nextEvent(500)
        for msg in event:
            msg_list.append(msg)

        if event.eventType() == blpapi.Event.RESPONSE:
            # Response completely received, so we could exit
            break

    return msg_list


s_name = 'NKY Index'
fields = ['PX_LAST']
start_date = '20230921'
end_date = '20230922'

msg_list = HistoricalDataRequest(s_name,fields, start_date,end_date)
print(msg_list[3])
msg_list
list_elements = [i.asElement().hasElement('securityData') for i in msg_list]
data_ticker = list_elements.index(True)
barTickDataArray = msg_list[data_ticker].asElement().getElement('securityData').getElement('fieldData')
print(barTickDataArray)

list_date = [barTickDataArray.getValueAsElement(i).getElementAsDatetime('date') for i in range(barTickDataArray.numValues())]
list_PX_LAST = [barTickDataArray.getValueAsElement(i).getElementAsFloat('PX_LAST') for i in range(barTickDataArray.numValues())]
list_date


# Get Historical Data
def FieldRequest(request_type):
    """FieldInfoRequest, FieldListRequest, FieldSearchRequest, CategorizedFieldSearchRequest"""

    session = start_session()

    if not session.openService('//blp/apiflds'):
        print('Failed to open //blp//apiflds')
        return

    # Obtain the historical data
    refDataService = session.getService('//blp/apiflds')

    request = refDataService.createRequest(request_type)
    request.set('fieldType', 'All')
    request.set('returnFieldDocumentation', True)

    session.sendRequest(request)

    # process the response
    msg_list = []
    while(True):
        event = session.nextEvent(500)
        for msg in event:
            msg_list.append(msg)

        if event.eventType() == blpapi.Event.RESPONSE:
            # Response completely received, so we could exit
            break

    return msg_list

request_type = 'FieldListRequest'
msg_list = FieldRequest(request_type)

dict_fields = {}
count_fields = 0
for msg_i in range(3,len(msg_list)):
    fields = msg_list[msg_i].getElement('fieldData')
    numElements = fields.numValues()

    for i in range(numElements):
        count_fields += 1

        mnemonic = fields.getValueAsElement(i).getElement('fieldInfo').getElement('mnemonic').getValueAsString()
        description = fields.getValueAsElement(i).getElement('fieldInfo').getElement('description').getValueAsString()
        datatype = fields.getValueAsElement(i).getElement('fieldInfo').getElement('datatype').getValueAsString()
        documentation = fields.getValueAsElement(i).getElement('fieldInfo').getElement('documentation').getValueAsString()
        categoryName = fields.getValueAsElement(i).getElement('fieldInfo').getElement('categoryName').getValueAsString()
        ftype = fields.getValueAsElement(i).getElement('fieldInfo').getElement('ftype').getValueAsString()

        inner_dict = {
                'description': description,
                'datatype': datatype,
                'documentation': documentation,
                'categoryName': categoryName,
                'ftype': ftype
                }

        dict_fields[mnemonic] = inner_dict

    
count_fields
len(dict_fields.keys())
dict_fields['BN_SURVEY_AVERAGE']
with open('bbg_fields.json','w') as f:
    f.write(json.dumps(dict_fields))
