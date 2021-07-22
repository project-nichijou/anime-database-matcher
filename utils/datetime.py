import time


def get_time_str_now():
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 

def get_time_str_from_timestamp(timestamp: int):
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

def get_date_str_now():
	return time.strftime("%Y-%m-%d", time.localtime())

def check_time_format(timestr: str):
	try:
		_ = time.strptime(timestr, "%Y-%m-%d %H:%M:%S")
		return True
	except:
		return False
