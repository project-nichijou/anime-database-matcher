from database.database import CommonDatabase


class NichijouDatabase(CommonDatabase):

	def __init__(self):
		super().__init__('nichijou')
