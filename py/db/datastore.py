import sqlite3
 
class Datastore:
 
  def __init__(self,filepath):
    self.data_file = filepath

  def connect(self):
    self.conn = sqlite3.connect(self.data_file)
    self.conn.row_factory = sqlite3.Row
    return self.conn.cursor()

  def close(self):
    self.conn.close()

  def commit(self):
    self.conn.commit()
    
  def free(self, cursor):
    cursor.close()

  def execute(self, query, values = ''):
    cursor = self.connect()
    if values != '':
        cursor.execute(query, values)
    else:
        cursor.execute(query)
        
    return cursor