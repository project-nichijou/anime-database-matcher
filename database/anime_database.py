from database.database import CommonDatabase


class AnimeDatabase(CommonDatabase):

	def __init__(self, database: str):
		super().__init__(database=database)
