from requests import request, Session
from datetime import datetime, timedelta
from typing import List
from uuid import uuid4
from xml.dom import minidom
from marshmallow_dataclass import dataclass
from class_mssql import MSSQLConnection
from class_settings import Settings

@dataclass
class Banknotes:
    nominal: int
    count: int

@dataclass
class Operation:
    date: datetime
    operationType : int
    address : str
    tid: str
    incomeSum: str
    responseCode: int
    receipt: str
    rrn: str
    currencyCode: int
    transactionStatus: str
    banknotes: List[Banknotes]
    pan: str
    customerName: str

@dataclass
class Devices:
    admSN: str
    events: List[Operation]

@dataclass
class Main:
    requestId: str
    fullSize: int
    offset: int
    devices: List[Devices]


def write_data_to_xml(data, filename, header="root"):
    """
    :param data: Объект - список или генератор. Можно передать словари внутри списков, или списки списков.
    :param filename: Имя файла в который требуется записать данные.
    :param header: Имя корневого элемента xml
    :return: Вернет истину, если все отработает корректно.
    """
    g = (n for n in [0, 1])
    try:
        # Умеем сохранять списки или генераторы.
        if type(data) in (list, type(g)):
            with open(filename, "w", encoding="UTF8") as f:
                root = minidom.Document()
                xml = root.createElement(header)
                root.appendChild(xml)
                for i, dd in enumerate(data):
                    row = root.createElement("row")
                    row.setAttribute("id", str(i + 1))
                    if type(dd) == list:
                        for z, vl in enumerate(list(dd)):
                            val = root.createElement("row")
                            val.setAttribute("id", str(z + 1))
                            val.setAttribute("data", str(vl))
                            row.appendChild(val)
                        xml.appendChild(row)
                    elif type(dd) == dict:
                        for x in dd.keys():
                            row.setAttribute(x, str(dd.get(x)))
                        xml.appendChild(row)
                    else:
                        row.setAttribute("data", str(dd))
                        xml.appendChild(row)
                xml_str = root.toprettyxml(indent="\t")
                f.write(xml_str)
                return True
        else:
            print("Записывать в xml данные можно только генераторы или списки!")
            return False
    except Exception as E:
        print(f"Исключительная ситуация при преобразовании в XML: {E}.")
        return False


s = Settings('settings.json')
sql = MSSQLConnection(s)
req = Session()
req.headers.update()
req.trust_env = True

reply = req.get(s.param('ws_addr'),
                 params={
                     'requestID': uuid4(),
                     'dateFrom': datetime.strftime(datetime.now()-timedelta(14), '%Y-%m-%d'),
                     'dateTo': datetime.strftime(datetime.now()+timedelta(1), '%Y-%m-%d'),
                     'count': 5000,
                     'offset': 0,
                     'token': s.param('ws_tokn')
                 },
                 verify=False)
fm = Main.Schema().loads(reply.text)
d = []
dump = Settings.random_file_name_local()

for x in fm.devices:
    for z in x.events:
        d.append({'date': z.date, 'operationType': z.operationType, 'incomeSum': str(z.incomeSum).replace(',', '.'), 'tid': z.tid, 'admSN': x.admSN, 'receipt': z.receipt, 'rrn': z.rrn})

if len(d) > 0:
    if write_data_to_xml(d, dump):
        sql.execute('exec [import_adm_data] %s', (sql.file_to_binary_data(dump)))




