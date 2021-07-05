# from kafka import KafkaConsumer
# consumer = KafkaConsumer('fleetcom-teste',  bootstrap_servers='172.30.16.1:9092')
# for msg in consumer:
#     print (msg)

import argparse

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from pymaxdb import conexao_dbmaker
from pymaxutil import log
import pandas as pd
import sys
import datetime

class Tabela(object):
    """
    User record
    Args:
        name (str): User's name
        favorite_number (int): User's favorite number
        favorite_color (str): User's favorite color
    """
    def __init__(self, LICENCA=None,ITEM_RATEIO=None,PGMOVMOV_CGC=None,PGMOVMOV_NDUPL=None,PGMOVMOV_TPLANC=None,PGMOVMOV_DTE=None,PGMOVMOV_DTEM=None,PGMOVMOV_DTV=None,PGMOVMOV_DTN=None,PGMOVMOV_VALOR=None,PGMOVMOV_VALORI=None,PGMOVMOV_PROJ=None,PGMOVMOV_IND=None,PGMOVMOV_VALIND=None,PGMOVMOV_VALORP=None,PGMOVMOV_VALORN=None,PGMOVMOV_TXJURD=None,PGMOVMOV_VLJURD=None,PGMOVMOV_MORA=None,PGMOVMOV_VLMORA=None,PGMOVMOV_TXDESC=None,PGMOVMOV_VLDESC=None,PGMOVMOV_DIASD=None,PGMOVMOV_BCOE=None,PGMOVMOV_AGE=None,PGMOVMOV_BCOC=None,PGMOVMOV_AGC=None,PGMOVMOV_NUMCH=None,PGMOVMOV_TARJA=None,PGMOVMOV_TARJAN=None,PGMOVMOV_INT_CONT=None,PGMOVMOV_INT_BAIXA=None,PGMOVMOV_NNRO=None,PGMOVMOV_NREF=None,PGMOVMOV_TPMOV_CNAB=None,PGMOVMOV_OBS=None,PGMOVMOV_TPFOR=None,PGMOVMOV_SITPG=None,PGMOVMOV_TPOP=None,PGMOVMOV_MOD=None,PGMOVMOV_RC=None,PGMOVMOV_FLPRO=None,PGMOVMOV_CLASSE_VALOR=None,PGMOVMOV_PARF=None,PGMOVMOV_CPAR=None,PGMOVMOV_CCUSTO=None,PGMOVMOV_TPOD=None,PGMOVMOV_VLTAB=None,PGMOVMOV_USU=None,PGMOVMOV_TIME=None,PGMOVMOV_DTS=None):
        self.LICENCA=LICENCA
        self.ITEM_RATEIO=ITEM_RATEIO
        self.PGMOVMOV_CGC=PGMOVMOV_CGC
        self.PGMOVMOV_NDUPL=PGMOVMOV_NDUPL
        self.PGMOVMOV_TPLANC=PGMOVMOV_TPLANC
        self.PGMOVMOV_DTE=PGMOVMOV_DTE
        self.PGMOVMOV_DTEM=PGMOVMOV_DTEM
        self.PGMOVMOV_DTV=PGMOVMOV_DTV
        self.PGMOVMOV_DTN=PGMOVMOV_DTN
        self.PGMOVMOV_VALOR=PGMOVMOV_VALOR
        self.PGMOVMOV_VALORI=PGMOVMOV_VALORI
        self.PGMOVMOV_PROJ=PGMOVMOV_PROJ
        self.PGMOVMOV_IND=PGMOVMOV_IND
        self.PGMOVMOV_VALIND=PGMOVMOV_VALIND
        self.PGMOVMOV_VALORP=PGMOVMOV_VALORP
        self.PGMOVMOV_VALORN=PGMOVMOV_VALORN
        self.PGMOVMOV_TXJURD=PGMOVMOV_TXJURD
        self.PGMOVMOV_VLJURD=PGMOVMOV_VLJURD
        self.PGMOVMOV_MORA=PGMOVMOV_MORA
        self.PGMOVMOV_VLMORA=PGMOVMOV_VLMORA
        self.PGMOVMOV_TXDESC=PGMOVMOV_TXDESC
        self.PGMOVMOV_VLDESC=PGMOVMOV_VLDESC
        self.PGMOVMOV_DIASD=PGMOVMOV_DIASD
        self.PGMOVMOV_BCOE=PGMOVMOV_BCOE
        self.PGMOVMOV_AGE=PGMOVMOV_AGE
        self.PGMOVMOV_BCOC=PGMOVMOV_BCOC
        self.PGMOVMOV_AGC=PGMOVMOV_AGC
        self.PGMOVMOV_NUMCH=PGMOVMOV_NUMCH
        self.PGMOVMOV_TARJA=PGMOVMOV_TARJA
        self.PGMOVMOV_TARJAN=PGMOVMOV_TARJAN
        self.PGMOVMOV_INT_CONT=PGMOVMOV_INT_CONT
        self.PGMOVMOV_INT_BAIXA=PGMOVMOV_INT_BAIXA
        self.PGMOVMOV_NNRO=PGMOVMOV_NNRO
        self.PGMOVMOV_NREF=PGMOVMOV_NREF
        self.PGMOVMOV_TPMOV_CNAB=PGMOVMOV_TPMOV_CNAB
        self.PGMOVMOV_OBS=PGMOVMOV_OBS
        self.PGMOVMOV_TPFOR=PGMOVMOV_TPFOR
        self.PGMOVMOV_SITPG=PGMOVMOV_SITPG
        self.PGMOVMOV_TPOP=PGMOVMOV_TPOP
        self.PGMOVMOV_MOD=PGMOVMOV_MOD
        self.PGMOVMOV_RC=PGMOVMOV_RC
        self.PGMOVMOV_FLPRO=PGMOVMOV_FLPRO
        self.PGMOVMOV_CLASSE_VALOR=PGMOVMOV_CLASSE_VALOR
        self.PGMOVMOV_PARF=PGMOVMOV_PARF
        self.PGMOVMOV_CPAR=PGMOVMOV_CPAR
        self.PGMOVMOV_CCUSTO=PGMOVMOV_CCUSTO
        self.PGMOVMOV_TPOD=PGMOVMOV_TPOD
        self.PGMOVMOV_VLTAB=PGMOVMOV_VLTAB
        self.PGMOVMOV_USU=PGMOVMOV_USU
        self.PGMOVMOV_TIME=PGMOVMOV_TIME
        self.PGMOVMOV_DTS=PGMOVMOV_DTS

def dict_to_tab(obj, ctx):
    """
    Converts object literal(dict) to a User instance.
    Args:
        obj (dict): Object literal(dict)
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """
    if obj is None:
        return None

    return Tabela(LICENCA=obj['LICENCA'], ITEM_RATEIO=obj['ITEM_RATEIO'], PGMOVMOV_CGC=obj['PGMOVMOV_CGC'], PGMOVMOV_NDUPL=obj['PGMOVMOV_NDUPL'], PGMOVMOV_TPLANC=obj['PGMOVMOV_TPLANC'], PGMOVMOV_DTE=obj['PGMOVMOV_DTE'], PGMOVMOV_DTEM=obj['PGMOVMOV_DTEM'], PGMOVMOV_DTV=obj['PGMOVMOV_DTV'], PGMOVMOV_DTN=obj['PGMOVMOV_DTN'], PGMOVMOV_VALOR=obj['PGMOVMOV_VALOR'], PGMOVMOV_VALORI=obj['PGMOVMOV_VALORI'], PGMOVMOV_PROJ=obj['PGMOVMOV_PROJ'], PGMOVMOV_IND=obj['PGMOVMOV_IND'], PGMOVMOV_VALIND=obj['PGMOVMOV_VALIND'], PGMOVMOV_VALORP=obj['PGMOVMOV_VALORP'], PGMOVMOV_VALORN=obj['PGMOVMOV_VALORN'], PGMOVMOV_TXJURD=obj['PGMOVMOV_TXJURD'], PGMOVMOV_VLJURD=obj['PGMOVMOV_VLJURD'], PGMOVMOV_MORA=obj['PGMOVMOV_MORA'], PGMOVMOV_VLMORA=obj['PGMOVMOV_VLMORA'], PGMOVMOV_TXDESC=obj['PGMOVMOV_TXDESC'], PGMOVMOV_VLDESC=obj['PGMOVMOV_VLDESC'], PGMOVMOV_DIASD=obj['PGMOVMOV_DIASD'], PGMOVMOV_BCOE=obj['PGMOVMOV_BCOE'], PGMOVMOV_AGE=obj['PGMOVMOV_AGE'], PGMOVMOV_BCOC=obj['PGMOVMOV_BCOC'], PGMOVMOV_AGC=obj['PGMOVMOV_AGC'], PGMOVMOV_NUMCH=obj['PGMOVMOV_NUMCH'], PGMOVMOV_TARJA=obj['PGMOVMOV_TARJA'], PGMOVMOV_TARJAN=obj['PGMOVMOV_TARJAN'], PGMOVMOV_INT_CONT=obj['PGMOVMOV_INT_CONT'], PGMOVMOV_INT_BAIXA=obj['PGMOVMOV_INT_BAIXA'], PGMOVMOV_NNRO=obj['PGMOVMOV_NNRO'], PGMOVMOV_NREF=obj['PGMOVMOV_NREF'], PGMOVMOV_TPMOV_CNAB=obj['PGMOVMOV_TPMOV_CNAB'], PGMOVMOV_OBS=obj['PGMOVMOV_OBS'], PGMOVMOV_TPFOR=obj['PGMOVMOV_TPFOR'], PGMOVMOV_SITPG=obj['PGMOVMOV_SITPG'], PGMOVMOV_TPOP=obj['PGMOVMOV_TPOP'], PGMOVMOV_MOD=obj['PGMOVMOV_MOD'], PGMOVMOV_RC=obj['PGMOVMOV_RC'], PGMOVMOV_FLPRO=obj['PGMOVMOV_FLPRO'], PGMOVMOV_CLASSE_VALOR=obj['PGMOVMOV_CLASSE_VALOR'], PGMOVMOV_PARF=obj['PGMOVMOV_PARF'], PGMOVMOV_CPAR=obj['PGMOVMOV_CPAR'], PGMOVMOV_CCUSTO=obj['PGMOVMOV_CCUSTO'], PGMOVMOV_TPOD=obj['PGMOVMOV_TPOD'], PGMOVMOV_VLTAB=obj['PGMOVMOV_VLTAB'], PGMOVMOV_USU=obj['PGMOVMOV_USU'], PGMOVMOV_TIME=obj['PGMOVMOV_TIME'], PGMOVMOV_DTS=obj['PGMOVMOV_DTS'])

topic = 'FLEETCOM_LOAD_2792'

schema_str = """
 {
  "type" : "record",
  "name" : "KsqlDataSourceSchema",
  "namespace" : "io.confluent.ksql.avro_schemas",
  "fields" : [ {
    "name" : "LICENCA",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "ITEM_RATEIO",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_CGC",
    "type" : [ "null", "long" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_NDUPL",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TPLANC",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_DTE",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_DTEM",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_DTV",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_DTN",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_VALOR",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_VALORI",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_PROJ",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_IND",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_VALIND",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_VALORP",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_VALORN",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TXJURD",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_VLJURD",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_MORA",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_VLMORA",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TXDESC",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_VLDESC",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_DIASD",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_BCOE",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_AGE",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_BCOC",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_AGC",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_NUMCH",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TARJA",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TARJAN",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_INT_CONT",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_INT_BAIXA",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_NNRO",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_NREF",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TPMOV_CNAB",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_OBS",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TPFOR",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_SITPG",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TPOP",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_MOD",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_RC",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_FLPRO",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_CLASSE_VALOR",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_PARF",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_CPAR",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_CCUSTO",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TPOD",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_VLTAB",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_USU",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_TIME",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "PGMOVMOV_DTS",
    "type" : [ "null", "int" ],
    "default" : null
  } ]
}
"""

sr_conf = {'url': 'http://189.2.113.196:38081'}
schema_registry_client = SchemaRegistryClient(sr_conf)

avro_deserializer = AvroDeserializer(schema_registry_client, schema_str, dict_to_tab)
string_deserializer = StringDeserializer('utf_8')

consumer_conf = {'bootstrap.servers': '189.2.113.196:19091,189.2.113.196:29092,189.2.113.196:39093',
                 'key.deserializer': string_deserializer,
                 'value.deserializer': avro_deserializer,
                 'group.id': 'fleetcom-2792',
                 'auto.offset.reset': 'earliest'}
#latest
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([topic])
logger = log(f"kafka-consumer", 10, 10, "w", 4, 0)

while True:
    try:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg = consumer.poll(1)

        if msg is None:
            print("sem dados")
            continue
        elif msg.error():
            logger.error('error: {}'.format(msg.error()))
        else:
            tab = msg.value()
            if tab is not None:
                __USER_DBMAKER = "SYSADM"
                __PASS_DBMAKER = "K5GhnHEr"
                __DB = 'DBCONTROL_6506_006'
                TABLE_OWNER, TABLE1, TABLE2 = 'DBCONTROL6506006', 'RECPGM06', 'RECPGB06'

                sql = f"select * from {TABLE_OWNER}.{TABLE1} where PGMOVMOV_CGC = {tab.PGMOVMOV_CGC} and PGMOVMOV_NDUPL = '{tab.PGMOVMOV_NDUPL}'"
                
                try:
                    con = conexao_dbmaker(10, __DB, __USER_DBMAKER, __PASS_DBMAKER).conectar()

                    try:
                        RECPGM = pd.read_sql_query(sql, con)
                        resultado = True
                    except:
                        RECPGM = pd.DataFrame()
                    finally:
                        if resultado:
                            con.fechar_conexao()
                        else:
                            con.fechar()
                except:
                    raise
                    
                sql = f"select * from {TABLE_OWNER}.{TABLE2} where PGMOVBAI_CGC = {tab.PGMOVMOV_CGC} and PGMOVBAI_NDUPL = '{tab.PGMOVMOV_NDUPL}'"

                try:
                    con = conexao_dbmaker(10, __DB, __USER_DBMAKER, __PASS_DBMAKER).conectar()

                    try:
                        RECPGB = pd.read_sql_query(sql, con)
                    except:
                        RECPGB = pd.DataFrame()
                    finally:
                        if resultado:
                            con.fechar_conexao()
                        else:
                            con.fechar()
                except:
                    raise
                
                cond1 = RECPGB.query(f"PGMOVBAI_CGC == {tab.PGMOVMOV_CGC} & PGMOVBAI_NDUPL == '{tab.PGMOVMOV_NDUPL}'").empty
                cond2 = RECPGM.fillna("null").query(f"PGMOVMOV_CGC == {tab.PGMOVMOV_CGC} & PGMOVMOV_NDUPL == '{tab.PGMOVMOV_NDUPL}' & (PGMOVMOV_VLTAB_{tab.ITEM_RATEIO} == 0 | PGMOVMOV_VLTAB_{tab.ITEM_RATEIO} == 'null')").empty
                cond3 = RECPGM.query(f"PGMOVMOV_CGC == {tab.PGMOVMOV_CGC} & PGMOVMOV_NDUPL == '{tab.PGMOVMOV_NDUPL}'").empty
                replece_none = lambda x: x.replace("'None'", "null")

                sql_insert_or_update_1 = ""
                sql_insert_or_update_2 = ""

                cast_tipo = lambda x: x if type(x) == int or type(x) == float else f"'{x}'" 

                if not cond1:
                    continue
                elif not cond2:
                    sql_insert_or_update_1 = f"UPDATE {TABLE_OWNER}.{TABLE1} SET PGMOVMOV_PARF_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_PARF)}, PGMOVMOV_CPAR_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_CPAR)}, PGMOVMOV_CCUSTO_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_CCUSTO)}, PGMOVMOV_TPOD_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_TPOD)}, PGMOVMOV_VLTAB_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_VLTAB)} WHERE PGMOVMOV_CGC={cast_tipo(tab.PGMOVMOV_CGC)} and PGMOVMOV_NDUPL={cast_tipo(tab.PGMOVMOV_NDUPL)}"
                elif cond3: 
                    convert_time = lambda x: int(datetime.datetime.timestamp(x))

                    sql_insert_or_update_1 = f"INSERT INTO {TABLE_OWNER}.{TABLE1} (PGMOVMOV_CGC, PGMOVMOV_NDUPL, PGMOVMOV_TPLANC, PGMOVMOV_DTE, PGMOVMOV_DTEM, PGMOVMOV_DTV, PGMOVMOV_DTN, PGMOVMOV_VALOR, PGMOVMOV_VALORI, PGMOVMOV_PROJ, PGMOVMOV_IND, PGMOVMOV_VALIND, PGMOVMOV_VALORP, PGMOVMOV_VALORN, PGMOVMOV_TXJURD, PGMOVMOV_VLJURD, PGMOVMOV_MORA, PGMOVMOV_VLMORA, PGMOVMOV_TXDESC, PGMOVMOV_VLDESC, PGMOVMOV_DIASD, PGMOVMOV_BCOE, PGMOVMOV_AGE, PGMOVMOV_BCOC, PGMOVMOV_AGC, PGMOVMOV_NUMCH, PGMOVMOV_TARJA, PGMOVMOV_TARJAN, PGMOVMOV_INT_CONT, PGMOVMOV_INT_BAIXA, PGMOVMOV_NNRO, PGMOVMOV_NREF, PGMOVMOV_TPMOV_CNAB, PGMOVMOV_OBS, PGMOVMOV_TPFOR, PGMOVMOV_SITPG, PGMOVMOV_TPOP, PGMOVMOV_MOD, PGMOVMOV_RC, PGMOVMOV_FLPRO, PGMOVMOV_CLASSE_VALOR, PGMOVMOV_PARF_{tab.ITEM_RATEIO}, PGMOVMOV_CPAR_{tab.ITEM_RATEIO}, PGMOVMOV_CCUSTO_{tab.ITEM_RATEIO}, PGMOVMOV_TPOD_{tab.ITEM_RATEIO}, PGMOVMOV_VLTAB_{tab.ITEM_RATEIO}, PGMOVMOV_USU, PGMOVMOV_TIME, PGMOVMOV_DTS) VALUES ({cast_tipo(tab.PGMOVMOV_CGC)}, {cast_tipo(tab.PGMOVMOV_NDUPL)}, {cast_tipo(tab.PGMOVMOV_TPLANC)}, {cast_tipo(tab.PGMOVMOV_DTE)}, {cast_tipo(tab.PGMOVMOV_DTEM)}, {cast_tipo(tab.PGMOVMOV_DTV)}, {cast_tipo(tab.PGMOVMOV_DTN)}, {cast_tipo(tab.PGMOVMOV_VALOR)}, {cast_tipo(tab.PGMOVMOV_VALORI)}, {cast_tipo(tab.PGMOVMOV_PROJ)}, {cast_tipo(tab.PGMOVMOV_IND)}, {cast_tipo(tab.PGMOVMOV_VALIND)}, {cast_tipo(tab.PGMOVMOV_VALORP)}, {cast_tipo(tab.PGMOVMOV_VALORN)}, {cast_tipo(tab.PGMOVMOV_TXJURD)}, {cast_tipo(tab.PGMOVMOV_VLJURD)}, {cast_tipo(tab.PGMOVMOV_MORA)}, {cast_tipo(tab.PGMOVMOV_VLMORA)}, {cast_tipo(tab.PGMOVMOV_TXDESC)}, {cast_tipo(tab.PGMOVMOV_VLDESC)}, {cast_tipo(tab.PGMOVMOV_DIASD)}, {cast_tipo(tab.PGMOVMOV_BCOE)}, {cast_tipo(tab.PGMOVMOV_AGE)}, {cast_tipo(tab.PGMOVMOV_BCOC)}, {cast_tipo(tab.PGMOVMOV_AGC)}, {cast_tipo(tab.PGMOVMOV_NUMCH)}, {cast_tipo(tab.PGMOVMOV_TARJA)}, {cast_tipo(tab.PGMOVMOV_TARJAN)}, {cast_tipo(tab.PGMOVMOV_INT_CONT)}, {cast_tipo(tab.PGMOVMOV_INT_BAIXA)}, {cast_tipo(tab.PGMOVMOV_NNRO)}, {cast_tipo(tab.PGMOVMOV_NREF)}, {cast_tipo(tab.PGMOVMOV_TPMOV_CNAB)}, {cast_tipo(tab.PGMOVMOV_OBS)}, {cast_tipo(tab.PGMOVMOV_TPFOR)}, {cast_tipo(tab.PGMOVMOV_SITPG)}, {cast_tipo(tab.PGMOVMOV_TPOP)}, {cast_tipo(tab.PGMOVMOV_MOD)}, {cast_tipo(tab.PGMOVMOV_RC)}, {cast_tipo(tab.PGMOVMOV_FLPRO)}, {cast_tipo(tab.PGMOVMOV_CLASSE_VALOR)}, {cast_tipo(tab.PGMOVMOV_PARF)}, {cast_tipo(tab.PGMOVMOV_CPAR)}, {cast_tipo(tab.PGMOVMOV_CCUSTO)}, {cast_tipo(tab.PGMOVMOV_TPOD)}, {cast_tipo(tab.PGMOVMOV_VLTAB)}, {cast_tipo(tab.PGMOVMOV_USU)}, {cast_tipo(tab.PGMOVMOV_TIME)}, {cast_tipo(tab.PGMOVMOV_DTS)})"
                
                sql_insert_or_update_1=replece_none(sql_insert_or_update_1)
                
                if sql_insert_or_update_1 != "":
                  try:
                      con = conexao_dbmaker(10, __DB, __USER_DBMAKER, __PASS_DBMAKER).conectar()

                      try: 
                          con.executar(sql_insert_or_update_1)                                           
                          con.efetivar()
                      except:
                          exc_type, exc_value, _ = sys.exc_info()
                          logger.error(f"ERROR {exc_type} {exc_value}")
                          logger.error(f"SQL| {sql_insert_or_update_1}")    
                          con.rollback()
                          logger.error("Erro ao adicionar os valores de chave: {} |PGMOVMOV_CGC: {} |PGMOVMOV_NDUPL: {} |PGMOVMOV_TPLANC: {} |PGMOVMOV_DTE: {} |PGMOVMOV_DTEM: {} |PGMOVMOV_DTV: {} |PGMOVMOV_DTN: {} |PGMOVMOV_VALOR: {} |PGMOVMOV_VALORI: {} |PGMOVMOV_PROJ: {} |PGMOVMOV_IND: {} |PGMOVMOV_VALIND: {} |PGMOVMOV_VALORP: {} |PGMOVMOV_VALORN: {} |PGMOVMOV_TXJURD: {} |PGMOVMOV_VLJURD: {} |PGMOVMOV_MORA: {} |PGMOVMOV_VLMORA: {} |PGMOVMOV_TXDESC: {} |PGMOVMOV_VLDESC: {} |PGMOVMOV_DIASD: {} |PGMOVMOV_BCOE: {} |PGMOVMOV_AGE: {} |PGMOVMOV_BCOC: {} |PGMOVMOV_AGC: {} |PGMOVMOV_NUMCH: {} |PGMOVMOV_TARJA: {} |PGMOVMOV_TARJAN: {} |PGMOVMOV_INT_CONT: {} |PGMOVMOV_INT_BAIXA: {} |F3: {} |PGMOVMOV_NNRO: {} |PGMOVMOV_NREF: {} |PGMOVMOV_TPMOV_CNAB: {} |F4: {} |PGMOVMOV_OBS: {} |PGMOVMOV_TPFOR: {} |PGMOVMOV_SITPG: {} |PGMOVMOV_TPOP: {} |PGMOVMOV_MOD: {} |PGMOVMOV_RC: {} |PGMOVMOV_FLPRO: {} |PGMOVMOV_EXEC_C14: {} |PGMOVMOV_CLASSE_VALOR: {} |F5: {} |CHAVE_NF: {} |ITEM_PARF: {} |PGMOVMOV_PARF: {} |PGMOVMOV_CPAR: {} |PGMOVMOV_CCUSTO: {} |PGMOVMOV_TPOD: {} |PGMOVMOV_VLTAB: {} |PGMOVMOV_DTINC: {} |PGMOVMOV_DTUALT: {} "
                              .format(tab.CHAVE, tab.PGMOVMOV_CGC, tab.PGMOVMOV_NDUPL, tab.PGMOVMOV_TPLANC, tab.PGMOVMOV_DTE, tab.PGMOVMOV_DTEM,
                                  tab.PGMOVMOV_DTV, tab.PGMOVMOV_DTN, tab.PGMOVMOV_VALOR, tab.PGMOVMOV_VALORI, tab.PGMOVMOV_PROJ, tab.PGMOVMOV_IND,
                                  tab.PGMOVMOV_VALIND, tab.PGMOVMOV_VALORP, tab.PGMOVMOV_VALORN, tab.PGMOVMOV_TXJURD, tab.PGMOVMOV_VLJURD, tab.PGMOVMOV_MORA,
                                  tab.PGMOVMOV_VLMORA, tab.PGMOVMOV_TXDESC, tab.PGMOVMOV_VLDESC, tab.PGMOVMOV_DIASD, tab.PGMOVMOV_BCOE, tab.PGMOVMOV_AGE,
                                  tab.PGMOVMOV_BCOC, tab.PGMOVMOV_AGC, tab.PGMOVMOV_NUMCH, tab.PGMOVMOV_TARJA, tab.PGMOVMOV_TARJAN, tab.PGMOVMOV_INT_CONT,
                                  tab.PGMOVMOV_INT_BAIXA, tab.F3, tab.PGMOVMOV_NNRO, tab.PGMOVMOV_NREF, tab.PGMOVMOV_TPMOV_CNAB, tab.F4, tab.PGMOVMOV_OBS,
                                  tab.PGMOVMOV_TPFOR, tab.PGMOVMOV_SITPG, tab.PGMOVMOV_TPOP, tab.PGMOVMOV_MOD, tab.PGMOVMOV_RC, tab.PGMOVMOV_FLPRO,
                                  tab.PGMOVMOV_EXEC_C14, tab.PGMOVMOV_CLASSE_VALOR, tab.F5, tab.CHAVE_NF, tab.ITEM_PARF, tab.PGMOVMOV_PARF,
                                  tab.PGMOVMOV_CPAR, tab.PGMOVMOV_CCUSTO, tab.PGMOVMOV_TPOD, tab.PGMOVMOV_VLTAB, tab.PGMOVMOV_DTINC, tab.PGMOVMOV_DTUALT))
                      finally:
                          con.fechar()
                  except:
                      logger.error("Erro de conexão com o banco de dados.")
                      pass
                

                def update_dados(x):
                  sql = f"UPDATE {TABLE_OWNER}.{TABLE1} SET"
                  sql += f' PGMOVMOV_DTE={cast_tipo(tab.PGMOVMOV_DTE)}' if x.PGMOVMOV_DTE != tab.PGMOVMOV_DTE else '' 
                  sql += f', PGMOVMOV_DTEM={cast_tipo(tab.PGMOVMOV_DTEM)}' if x.PGMOVMOV_DTEM != tab.PGMOVMOV_DTEM else '' 
                  sql += f', PGMOVMOV_DTV={cast_tipo(tab.PGMOVMOV_DTV)}' if x.PGMOVMOV_DTV != tab.PGMOVMOV_DTV else '' 
                  sql += f', PGMOVMOV_DTN={cast_tipo(tab.PGMOVMOV_DTN)}' if x.PGMOVMOV_DTN != tab.PGMOVMOV_DTN else '' 
                  sql += f', PGMOVMOV_VALOR={cast_tipo(tab.PGMOVMOV_VALOR)}' if x.PGMOVMOV_VALOR != tab.PGMOVMOV_VALOR else '' 
                  sql += f', PGMOVMOV_VALORI={cast_tipo(tab.PGMOVMOV_VALORI)}' if x.PGMOVMOV_VALORI != tab.PGMOVMOV_VALORI else '' 
                  sql += f', PGMOVMOV_PROJ={cast_tipo(tab.PGMOVMOV_PROJ)}' if x.PGMOVMOV_PROJ != tab.PGMOVMOV_PROJ else '' 
                  sql += f', PGMOVMOV_IND={cast_tipo(tab.PGMOVMOV_IND)}' if x.PGMOVMOV_IND != tab.PGMOVMOV_IND else '' 
                  sql += f', PGMOVMOV_VALIND={cast_tipo(tab.PGMOVMOV_VALIND)}' if x.PGMOVMOV_VALIND != tab.PGMOVMOV_VALIND else '' 
                  sql += f', PGMOVMOV_VALORP={cast_tipo(tab.PGMOVMOV_VALORP)}' if x.PGMOVMOV_VALORP != tab.PGMOVMOV_VALORP else '' 
                  sql += f', PGMOVMOV_VALORN={cast_tipo(tab.PGMOVMOV_VALORN)}' if x.PGMOVMOV_VALORN != tab.PGMOVMOV_VALORN else '' 
                  sql += f', PGMOVMOV_TXJURD={cast_tipo(tab.PGMOVMOV_TXJURD)}' if x.PGMOVMOV_TXJURD != tab.PGMOVMOV_TXJURD else '' 
                  sql += f', PGMOVMOV_VLJURD={cast_tipo(tab.PGMOVMOV_VLJURD)}' if x.PGMOVMOV_VLJURD != tab.PGMOVMOV_VLJURD else '' 
                  sql += f', PGMOVMOV_MORA={cast_tipo(tab.PGMOVMOV_MORA)}' if x.PGMOVMOV_MORA != tab.PGMOVMOV_MORA else '' 
                  sql += f', PGMOVMOV_VLMORA={cast_tipo(tab.PGMOVMOV_VLMORA)}' if x.PGMOVMOV_VLMORA != tab.PGMOVMOV_VLMORA else '' 
                  sql += f', PGMOVMOV_TXDESC={cast_tipo(tab.PGMOVMOV_TXDESC)}' if x.PGMOVMOV_TXDESC != tab.PGMOVMOV_TXDESC else '' 
                  sql += f', PGMOVMOV_VLDESC={cast_tipo(tab.PGMOVMOV_VLDESC)}' if x.PGMOVMOV_VLDESC != tab.PGMOVMOV_VLDESC else '' 
                  sql += f', PGMOVMOV_DIASD={cast_tipo(tab.PGMOVMOV_DIASD)}' if x.PGMOVMOV_DIASD != tab.PGMOVMOV_DIASD else '' 
                  sql += f', PGMOVMOV_BCOE={cast_tipo(tab.PGMOVMOV_BCOE)}' if x.PGMOVMOV_BCOE != tab.PGMOVMOV_BCOE else '' 
                  sql += f', PGMOVMOV_AGE={cast_tipo(tab.PGMOVMOV_AGE)}' if x.PGMOVMOV_AGE.strip() != tab.PGMOVMOV_AGE else '' 
                  sql += f', PGMOVMOV_BCOC={cast_tipo(tab.PGMOVMOV_BCOC)}' if x.PGMOVMOV_BCOC != tab.PGMOVMOV_BCOC else '' 
                  sql += f', PGMOVMOV_AGC={cast_tipo(tab.PGMOVMOV_AGC)}' if x.PGMOVMOV_AGC.strip() != tab.PGMOVMOV_AGC else '' 
                  sql += f', PGMOVMOV_NUMCH={cast_tipo(tab.PGMOVMOV_NUMCH)}' if x.PGMOVMOV_NUMCH.strip() != tab.PGMOVMOV_NUMCH else '' 
                  sql += f', PGMOVMOV_TARJA={cast_tipo(tab.PGMOVMOV_TARJA)}' if x.PGMOVMOV_TARJA.strip() != tab.PGMOVMOV_TARJA else '' 
                  sql += f', PGMOVMOV_TARJAN={cast_tipo(tab.PGMOVMOV_TARJAN)}' if x.PGMOVMOV_TARJAN.strip() != tab.PGMOVMOV_TARJAN else '' 
                  sql += f', PGMOVMOV_INT_CONT={cast_tipo(tab.PGMOVMOV_INT_CONT)}' if x.PGMOVMOV_INT_CONT != tab.PGMOVMOV_INT_CONT else '' 
                  sql += f', PGMOVMOV_INT_BAIXA={cast_tipo(tab.PGMOVMOV_INT_BAIXA)}' if x.PGMOVMOV_INT_BAIXA != tab.PGMOVMOV_INT_BAIXA else '' 
                  sql += f', PGMOVMOV_NNRO={cast_tipo(tab.PGMOVMOV_NNRO)}' if x.PGMOVMOV_NNRO.strip() != tab.PGMOVMOV_NNRO else '' 
                  sql += f', PGMOVMOV_NREF={cast_tipo(tab.PGMOVMOV_NREF)}' if x.PGMOVMOV_NREF.strip() != tab.PGMOVMOV_NREF else '' 
                  sql += f', PGMOVMOV_TPMOV_CNAB={cast_tipo(tab.PGMOVMOV_TPMOV_CNAB)}' if x.PGMOVMOV_TPMOV_CNAB != tab.PGMOVMOV_TPMOV_CNAB else '' 
                  sql += f', PGMOVMOV_OBS={cast_tipo(tab.PGMOVMOV_OBS)}' if x.PGMOVMOV_OBS.strip() != tab.PGMOVMOV_OBS else '' 
                  sql += f', PGMOVMOV_TPFOR={cast_tipo(tab.PGMOVMOV_TPFOR)}' if x.PGMOVMOV_TPFOR != tab.PGMOVMOV_TPFOR else '' 
                  sql += f', PGMOVMOV_SITPG={cast_tipo(tab.PGMOVMOV_SITPG)}' if x.PGMOVMOV_SITPG != tab.PGMOVMOV_SITPG else '' 
                  sql += f', PGMOVMOV_TPOP={cast_tipo(tab.PGMOVMOV_TPOP)}' if x.PGMOVMOV_TPOP != tab.PGMOVMOV_TPOP else '' 
                  sql += f', PGMOVMOV_MOD={cast_tipo(tab.PGMOVMOV_MOD)}' if x.PGMOVMOV_MOD != tab.PGMOVMOV_MOD else '' 
                  sql += f', PGMOVMOV_RC={cast_tipo(tab.PGMOVMOV_RC)}' if x.PGMOVMOV_RC != tab.PGMOVMOV_RC else '' 
                  sql += f', PGMOVMOV_FLPRO={cast_tipo(tab.PGMOVMOV_FLPRO)}' if x.PGMOVMOV_FLPRO.strip() != tab.PGMOVMOV_FLPRO else '' 
                  sql += f', PGMOVMOV_CLASSE_VALOR={cast_tipo(tab.PGMOVMOV_CLASSE_VALOR)}' if x.PGMOVMOV_CLASSE_VALOR != tab.PGMOVMOV_CLASSE_VALOR else '' 
                  sql += f', PGMOVMOV_PARF_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_PARF)}' if x[f"PGMOVMOV_PARF_{tab.ITEM_RATEIO}"] != tab.PGMOVMOV_PARF else '' 
                  sql += f', PGMOVMOV_CPAR_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_CPAR)}' if x[f"PGMOVMOV_CPAR_{tab.ITEM_RATEIO}"] != tab.PGMOVMOV_CPAR else '' 
                  sql += f', PGMOVMOV_CCUSTO_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_CCUSTO)}' if x[f"PGMOVMOV_CCUSTO_{tab.ITEM_RATEIO}"] != tab.PGMOVMOV_CCUSTO else '' 
                  sql += f', PGMOVMOV_TPOD_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_TPOD)}' if x[f"PGMOVMOV_TPOD_{tab.ITEM_RATEIO}"] != tab.PGMOVMOV_TPOD else '' 
                  sql += f', PGMOVMOV_VLTAB_{tab.ITEM_RATEIO}={cast_tipo(tab.PGMOVMOV_VLTAB)}' if x[f"PGMOVMOV_VLTAB_{tab.ITEM_RATEIO}"] != tab.PGMOVMOV_VLTAB else '' 
                  sql += f', PGMOVMOV_USU={cast_tipo(tab.PGMOVMOV_USU)}' if x.PGMOVMOV_USU != tab.PGMOVMOV_USU else '' 
                  sql += f', PGMOVMOV_TIME={cast_tipo(tab.PGMOVMOV_TIME)}' if x.PGMOVMOV_TIME != tab.PGMOVMOV_TIME else '' 
                  sql += f', PGMOVMOV_DTS={cast_tipo(tab.PGMOVMOV_DTS)}' if x.PGMOVMOV_DTS != tab.PGMOVMOV_DTS else ''
                  sql += f" WHERE PGMOVMOV_CGC = {tab.PGMOVMOV_CGC} and PGMOVMOV_NDUPL = '{tab.PGMOVMOV_NDUPL}'"
                  sql = sql.replace("SET,", "SET")
                  try:
                    con = conexao_dbmaker(10, __DB, __USER_DBMAKER, __PASS_DBMAKER).conectar()

                    try: 
                        con.executar(sql)                                           
                        con.efetivar()
                    except:
                        exc_type, exc_value, _ = sys.exc_info()
                        logger.error(f"ERROR {exc_type} {exc_value}")
                        logger.error(f"SQL| {sql}")    
                        con.rollback()
                    finally:
                        con.fechar()
                  except:
                    logger.error("Erro de conexão com o banco de dados.")
                    pass

                RECPGM.query(f"PGMOVMOV_CGC == {tab.PGMOVMOV_CGC} & PGMOVMOV_NDUPL == '{tab.PGMOVMOV_NDUPL}'").apply(update_dados, axis=1)

                
    except KeyboardInterrupt:
        break

consumer.close()