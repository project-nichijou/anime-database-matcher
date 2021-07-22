
def format_log(info: str, exception=None, traceback: str=None, values: dict={}):
	log = (
		f'INFO: {info}\n'
		f'------------\n'
	)
	if exception is not None:
		log = log + (
			f'EXCEPTION FOUNDED: \n'
			f'{repr(exception)}\n'
			f'------------\n'
		)
	if traceback is not None:
		log = log + (
			f'TRACEBACK: \n'
			f'{traceback}\n'
			f'------------\n'
		)
	for key in values.keys():
		log = log + (
			f'{key}: {repr(values[key])}\n'
		)
	return log
