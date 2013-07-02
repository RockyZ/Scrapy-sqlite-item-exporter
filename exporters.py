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
    		keys = None
    		if hasattr(item.__class__, 'keys'):
    			sqlite_keys = item.__class__.sqlite_keys
    		self._create_table(item_class_name, item.fields.iterkeys(), sqlite_keys)
    		self.created_tables.append(item_class_name)
    	
    	field_list = []
    	value_list = []
    	for field_name in item.iterkeys():
    		field_list.append('[%s]' % field_name)
    		field = item.fields[field_name]
    		value_list.append(self.serialize_field(field, field_name, item[field_name]))
    	
    	sql = 'insert or ignore into [%s] (%s) values (%s)' % (item_class_name, ', '.join(field_list), ', '.join(['?' for f in field_list]))
    	self.conn.execute(sql, value_list)
    	self.conn.commit()
    		
    def _create_table(self, table_name, columns, keys = None):
		sql = 'create table if not exists [%s] ' % table_name
		
		column_define = ['[%s] text' % column for column in columns]
		print 'type: %s' % type(keys)
		if keys:
			if len(keys) > 0:
				primary_key = 'primary key (%s)' % ', '.join(keys[0])
				column_define.append(primary_key)
				
			for key in keys[1:]:
				column_define.append('unique (%s)' % ', '.join(key))
		
		sql += '(%s)' % ', '.join(column_define)
		
		print 'sql: %s' % sql
		self.conn.execute(sql)
		self.conn.commit()
    	
    def __del__(self):
    	self.conn.close()