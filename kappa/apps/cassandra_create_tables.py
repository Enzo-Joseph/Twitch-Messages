from cassandra.cluster import Cluster
clstr=Cluster(['172.18.0.3'])
session=clstr.connect()

session.execute("DROP KEYSPACE IF EXISTS projet;")

qry=''' 
CREATE KEYSPACE IF NOT EXISTS projet 
WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};'''
session.execute(qry) 

qry=''' 
CREATE TABLE IF NOT EXISTS projet.wordcount (
  channel text,
  timestamp Timestamp,
  wordcount int,
  PRIMARY KEY (channel, timestamp)
);'''

session.execute(qry) 

qry='''
CREATE TABLE projet.wordfreq (
  word TEXT PRIMARY KEY,
  count INT
);'''
session.execute(qry)


qry='''
CREATE TABLE projet.msgperuser (
  channel TEXT,
  user TEXT,
  msgcount INT,
  PRIMARY KEY (channel, user)
)
'''

session.execute(qry)