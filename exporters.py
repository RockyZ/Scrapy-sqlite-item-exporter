"""
Item Exporters are used to export/serialize items into sqlite3 database.
"""

from scrapy.contrib.exporter import BaseItemExporter
import sqlite3

class SqliteItemExporter(BaseItemExporter):
    
    def __init__(self, file, **kwargs):
        self._configure(kwargs)
        self.conn = sqlite3.connect(file.name)
        self.conn.text_factory = str
    	self.created_tables = []
    
    def export_item(self, item):   		
    	item_class_name = type(item).__name__
    	
    	if item_class_name not in self.created_tables:
    		self._create_table(item_class_name, item.fields.iterkeys())
    		self.created_tables.append(item_class_name)
    	
    	field_list = []
    	value_list = []
    	for field_name in item.iterkeys():
    		field_list.append('[%s]' % field_name)
    		field = item.fields[field_name]
    		value_list.append(self.serialize_field(field, field_name, item[field_name]))
    	
    	sql = 'insert into [%s] (%s) values (%s)' % (item_class_name, ', '.join(field_list), ', '.join(['?' for f in field_list]))
    	self.conn.execute(sql, value_list)
    	self.conn.commit()
    		
    def _create_table(self, table_name, columns):
		sql = 'create table if not exists [%s] ' % table_name
		column_define = ', '.join(['[%s] text' % column for column in columns])
		sql += '(%s)' % column_define
		self.conn.execute(sql)
		self.conn.commit()
    	
    def __del__(self):
    	self.conn.close()
