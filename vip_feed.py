import re
import sys
import os.path
import glob
from datetime import datetime,tzinfo
from optparse import OptionParser,make_option
from ConfigParser import SafeConfigParser
from csv import DictReader,Error as CSVError

class SimpleDatabase(object):
  """Simple class to interface with the VIP database"""
  
  def __init__(self,options):
    self.__dbtype = options['type']
    del options['type']
    
    try:
      if dbtype=='mysql':
        import MySQLdb
        from MySQLdb import connection,cursors
        options.update({'cursorclass':MySQLdb.cursors.DictCursor})
        self.__db = MySQLdb.connect(**options)
      elif dbtype=='mssql':
        options.update({'as_dict':True})
        sys.exit(options)
        self.__db = pymssql.connect(**options)
      
      self.__cursor = self.__db.cursor()
      
    except:
      print "Unexpected error: ",sys.exc_info()[0]
      return
  
  def query(self,statement):
    try:
      print "Querying database..."
      self.__cursor.execute(statement)
      
    finally:
      self.rows = self.__cursor.fetchall()
      print "Getting all %d rows..." % len(self.rows)
      self.fields = self.__cursor.description
      self.__cursor.close()   
      self.__db.close()
      
  def get_cursor(self):
    return self.__cursor
  
  def set_cursor(self,cursor):
    self.__cursor = cursor
    
  def get_db(self):
    return self.__db
    
  def set_db(self,db):
    self.__db = db

  cursor = property(get_cursor,set_cursor)
  db = property(get_db,set_db)

class FileParser(object):
  """Simple file parser"""
  def __init__(self,options):
    self.__read_file = options['file_name']
    self.__write_mode = options['write_mode']
    self.__parser_type = options['parser_type']
    self.__write_file = options['vip_file']
    
    if self.__parser_type=='regex':
      self.__parser = re.compile(options['regex'])
      
    elif self.__parser_type=='csv':
      self.__parser_cfg = options['cfg']

      self.__parser = dyn_class(options['parser_module'],options['parser_class'])
      self.__parser = self.__parser(open(self.__read_file),**self.__parser_cfg)
  
  def parse(self,template,static_opts=None):
    f = open(self.__read_file,'r')
    w = open(self.__write_file,self.__write_mode)

    if self.__parser_type=='csv':
      try:
        for line in self.__parser:
          w.write(template.format(**line))
      except CSVError:
        """Something..."""
    elif self.__parser_type=='regex':
      for line in f:
        line_data = self.__parser.match(line)
        
        if line_data!=None:
          line_data = line_data.groupdict()
  
          for k in line_data.iterkeys():
            line_data[k] = line_data[k].strip()

          w.write(template.format(**line_data))
    
    f.close()  
    w.close()
  
  def get_read_file(self):
    return self.__read_file
  
  def set_read_file(self,read_file):
    self.__read_file = read_file
  
  def get_write_file(self):
    return self.__write_file
  
  def set_write_file(self,write_file):
    self.__write_file = write_file
  
  read_file = property(get_read_file,set_read_file)
  write_file = property(get_write_file,set_write_file)
  
def main():
  """Set it off"""
  now = datetime.now()
  usage = "%prog [options] arg"
  version = "%prog .1"
  write_mode = 'a'
  
  parser = OptionParser(usage=usage,version=version,option_list=create_options_list())

  (opts,args) = parser.parse_args()
  
  if os.path.isfile(opts.config):
    # parse config file
    config = SafeConfigParser()
    config.read(opts.config)
    
    vip_file = "{0}vipFeed-{1}-{2}.xml".format(config.get('Main','output_dir'),config.get('Header','state_fips'),now.strftime('%Y-%m-%dT%H-%M-%S'))
    templates = get_templates(config.get('Main','template_dir'))
  
  for k in templates.iterkeys():
    section = k.title()
    template = open(templates[k],'r').read()
    
    if os.path.exists(vip_file):
      write_mode = 'a'
    else:
      write_mode = 'w+'
    
    if config.has_section(section):
      options = config_section_as_dict(config.items(section))
      options['vip_file'] = vip_file
    else:
      print "Config {0} not found".format(section)
      continue
      
    if k!='header':
      print k
      if options['parser_type']=='csv':
        if config.has_option(section,'delimiter'):
          if 'cfg' not in options:
            options['cfg'] = {}
          
          options['cfg']['delimiter'] = chr(config.getint(section,'delimiter'))

      options['file_name'] = "".join([config.get('DataSource','data_dir'),options['data']])
      options['write_mode'] = write_mode
      
#       extra_opts = {}
#       extra_opts['id'] = count
#       extra_opts['precinct_id'] = 0
#       extra_opts['state'] = 'IL' # should come from ini
      
      fp = FileParser(options)
      fp.parse(template)
      
    elif k=='header':
      options['script_start'] = now.strftime('%Y-%m-%dT%H:%M:%S')
      with open(vip_file,'w+') as w:
        old = w.read() # read everything in the file
        w.seek(0) # rewind
        w.write(template.format(**options)) # write the new line before
        w.write(old)
    
  with open(vip_file,write_mode) as w:
    w.write("</vip_object>")

def create_options_list():
  """Creates the optionsparser options list"""
  option_list = [
    make_option("-v", "--verbose", dest="verbose",
      default=False, action="store_true",
      help="Write status messages to stdout"),

    make_option("--config", type="string",
      dest="config", default="config.ini",
      help="Specifies a config file",
      metavar="CONFIG_FILE"),
  ]
  
  return option_list

def config_section_as_dict(config_items):
  """Changes the section list of tuples to a dictionary"""
  data_dict = {}
  
  for key,val in config_items:
    data_dict[key] = val
  
  return data_dict

def dyn_class(modname,classname):
  """Returns a class of classname from module modname."""
  module = __import__(modname)
  klass = getattr(module,classname)
  return klass

def get_templates(path):
  """Iterates through a directory"""
  templates = {}
  tmp = {}
  
  for file_name in glob.glob(os.path.join(path,'*.tmpl')):
    tmp = get_file_path_obj(file_name)
    templates[tmp['root']] = tmp['file']
    
  return templates

def get_file_path_obj(file_name):
  """Breaks a file path into various pieces"""
  head,tail = os.path.split(file_name)
  root,ext = os.path.splitext(tail)
#   valuelist = [file_name,head,tail,root,ext]
#   keylist = ['file','head','tail','root','ext']
#   map(operator.setitem, [file_obj]*len(keylist), keylist, valuelist)
  file_obj = {
    'file': file_name,
    'head': head,
    'tail': tail,
    'root': root,
    'ext': ext
  }
  return file_obj

if __name__ == "__main__":
  main()