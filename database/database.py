from utils.checker import is_not_null, is_null
from utils.datetime import check_time_format, get_time_str_from_timestamp, get_time_str_now
from utils.logger import format_log
from database import database_settings as db_settings
from database import database_commands as db_commands

import mysql.connector
import copy
import traceback


class CommonDatabase(object):

	def __init__(self, database=None, config=None):
		if config is not None:
			self.config = config
		else:
			self.config = db_settings.DATABASE_CONFIG
		if database is not None:
			self.config['database'] = database
		self.initialize_database()


	def initialize_database(self):
		'''
		Initialize database
		'''
		# check database
		self.check_database()
		# connect to database
		self.database = mysql.connector.connect(**self.config)
		# check tables
		self.check_tables()


	def check_database(self):
		'''
		check whether the specific database exists
		'''
		# check whether database exists, if not, create.
		__config = copy.deepcopy(self.config)
		if 'database' in __config.keys():
			db_name = __config.pop('database')
		session = mysql.connector.connect(**__config)
		cursor = session.cursor()
		cursor.execute(
			f'CREATE DATABASE IF NOT EXISTS `{db_name}`'
		)
		cursor.close()
		session.close()


	def check_tables(self):
		'''
		check whether the specific tables exist
		'''
		cursor = self.get_cursor()
		if self.config['database'] == 'nichijou':
			cmds = db_commands.CREATE_NICHIJOU_TABLE_COMMANDS.values()
		else:
			cmds = db_commands.CREATE_ANIME_TABLE_COMMANDS.values()
		for cmd in cmds:
			cursor.execute(cmd)
		cursor.close()


	def execute(self, command: str):
		'''
		execute custom commands directly (without returning any results)
		'''
		try:
			cursor = self.get_cursor()
			cursor.execute(command)
			cursor.close()
		except Exception as e:
			self.log(format_log(
				info='exception caught when executing sql.',
				exception=e,
				traceback=traceback.format_exc(),
				values={
					'command': command
				}
			))



	def write(self, table: str, values: dict):
		'''
		write values to the specific table.
		override the item on duplicate key.
		'''
		try:
			keys = values.keys()
			
			cmd_line_table = f'INSERT INTO {table} '
			cmd_line_keys = f'({", ".join(f"`{key}`" for key in keys)}) '
			cmd_line_values = f'VALUES ({", ".join(f"%({key})s" for key in keys)}) '
			cmd_line_update = f'ON DUPLICATE KEY UPDATE {", ".join(f"`{key}` = %({key})s" for key in keys)}'
			
			command = '\n'.join([cmd_line_table, cmd_line_keys, cmd_line_values, cmd_line_update])

			cursor = self.get_cursor()
			cursor.execute(command, values)

			self.database.commit()
			cursor.close()
		except Exception as e:
			self.log(format_log(
				info='exception caught when writing to database.',
				exception=e,
				traceback=traceback.format_exc(),
				values={
					'table': table,
					'values': values,
				}
			))


	def update(self, table: str, primary_keys: list, values: dict):
		'''
		safe version of `write`. 
		this function would merge the original values in the database with new values.
		`primary_keys` should be provided to query data
		'''
		try:
			eq_conditions = {
				p_key: values[p_key] for p_key in primary_keys
			}
			old = self.read_all(table=table, keys=['*'], eq_conditions=eq_conditions)
			if len(old) > 1:
				raise Exception('primary_keys provided not unique')
			vals = {}
			if len(old) == 1:
				vals = old[0]
			for key in values.keys():
				vals[key] = values[key]
			self.write(table=table, values=vals)
		except Exception as e:
			self.log(format_log(
				info='exception caught when updating to database.',
				exception=e,
				traceback=traceback.format_exc(),
				values={
					'table': table,
					'primary_keys': primary_keys,
					'values': values,
				}
			))


	def log(self, info: str):
		'''
		log information into `log` table
		'''
		self.write(table='log', values={
			'time': get_time_str_now(),
			'content': info
		})


	def read_all(self, table: str, keys: list, eq_conditions: dict={}):
		'''
		read `keys` from `table` where `eq_conditions` (equal conditions) meet.
		use `keys = ['*']` if you want to select all columns.
		'''
		try:
			cursor = self.get_cursor(dictionary=True)

			if keys == ['*']:
				query_keys = '*'
			else:
				query_keys = ', '.join(
					[f'`{key}`' for key in keys]
				)
			query_conditions = ' AND '.join(
				[f'`{key}` = {repr(eq_conditions[key])}' for key in eq_conditions.keys()]
			)
			query = f'SELECT {query_keys} FROM {table}'
			if is_not_null(eq_conditions):
				query = f'{query} WHERE {query_conditions}'

			cursor.execute(query)
			res = cursor.fetchall()

			cursor.close()
			return res
		except Exception as e:
			self.log(format_log(
				info='exception caught when reading from database.',
				exception=e,
				traceback=traceback.format_exc(),
				values={
					'table': table,
					'keys': keys,
					'eq_conditions': eq_conditions
				}
			))


	def delete_log(self, time=None):
		'''
		delete log before `time`.
		time format: `YYYY-MM-DD HH:MM:SS`, or you can simply past timestamp
		'''
		try:
			if is_null(time):
				time = get_time_str_now()
			if isinstance(time, int):
				time = get_time_str_from_timestamp(time)
			if check_time_format(time):
				raise Exception('time format incorrect')
			cursor = self.get_cursor()

			delete = f'DELETE FROM `log` WHERE `time` <= {repr(time)}'
			
			cursor.execute(delete)
			self.database.commit()
			cursor.close()
		except Exception as e:
			self.log(format_log(
				info='exception caught when deleting logs.',
				exception=e,
				traceback=traceback.format_exc(),
				values={
					'time': time
				}
			))


	def get_cursor(self, dictionary=False):
		return self.database.cursor(dictionary=dictionary)
