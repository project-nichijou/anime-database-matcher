import traceback
from utils.logger import format_log
from database.database import CommonDatabase


class NichijouDatabase(CommonDatabase):

	def __init__(self):
		super().__init__('nichijou')


	def check_source_column(self, source):
		'''
		check whether source column exists
		'''
		try:
			cursor = self.get_cursor(dictionary=True)
			cursor.execute('DESC `anime`')
			res = cursor.fetchall()
			for item in res:
				if item['Field'] == source:
					return True
			return False
		except Exception as e:
			self.log(format_log(
				info='exception caught when checking source column.',
				exception=e,
				traceback=traceback.format_exc(),
				values={
					'source': source
				}
			))
			return False


	def add_source_column(self, source):
		'''
		create source column if not exist
		'''
		try:
			if not self.check_source_column(source=source):
				cursor = self.get_cursor()
				cursor.execute(f'ALTER TABLE `anime` ADD COLUMN `{source}` INT UNSIGNED')
				cursor.close()
		except Exception as e:
			self.log(format_log(
				info='exception caught when adding source column.',
				exception=e,
				traceback=traceback.format_exc(),
				values={
					'source': source
				}
			))


	def delete_match_fail(self, source, id):
		'''
		delete the resolved `match_fail` item
		'''
		try:
			cursor = self.get_cursor()

			delete = f'DELETE FROM `match_fail` WHERE `source` = {repr(source)} AND `id` = {repr(id)}'

			cursor.execute(delete)
			self.database.commit()
			cursor.close()
		except Exception as e:
			self.log(format_log(
				info='exception caught when deleting resolved match_fail items.',
				exception=e,
				traceback=traceback.format_exc(),
				values={
					'source': source,
					'id': id
				}
			))
