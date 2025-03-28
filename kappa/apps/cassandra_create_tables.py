from cassandra.cluster import Cluster
clstr=Cluster(['172.18.0.4'])
session=clstr.connect()

session.execute("DROP KEYSPACE IF EXISTS projet;")

# Reset ketspace
qry=''' 
CREATE KEYSPACE IF NOT EXISTS projet 
WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};'''
session.execute(qry) 


####################
# Stream tables
####################

# wordcount table
qry=''' 
CREATE TABLE IF NOT EXISTS projet.wordcount (
  channel text,
  timestamp Timestamp,
  wordcount int,
  PRIMARY KEY (channel, timestamp, wordcount)
)
;
'''

session.execute(qry) 

# wordfreq table
qry='''
CREATE TABLE projet.wordfreq (
  channel TEXT,
  word TEXT,
  count INT,
  PRIMARY KEY (channel, word, count)
) 
;
'''
session.execute(qry)

# msgperuser table
qry='''
CREATE TABLE projet.msgperuser (
  channel TEXT,
  user TEXT,
  msgcount INT,
  PRIMARY KEY (channel, user, msgcount)
  )
'''
session.execute(qry)

####################
# Batch tables
####################


# wordcount_batch table
qry='''
CREATE TABLE projet.wordcount_batch (
  channel TEXT,
  timestamp TIMESTAMP,
  wordcount INT,
  PRIMARY KEY (channel, timestamp, wordcount)
)
'''
session.execute(qry)

# wordfreq_batch table
qry='''
CREATE TABLE projet.wordfreq_batch (
  channel TEXT,
  word TEXT,
  count INT,
  PRIMARY KEY (channel, word, count)
)
'''
session.execute(qry)

# msgperuser_batch table
qry='''
CREATE TABLE projet.msgperuser_batch (
  channel TEXT,
  user TEXT,
  msgcount INT,
  PRIMARY KEY (channel, user, msgcount)
)
'''
session.execute(qry)