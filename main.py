from utils.checker import is_not_null, is_null
from database.anime_database import AnimeDatabase
from database.nichijou_database import NichijouDatabase
from utils import echo

import os
import sys
import json
import click
import editdistance


@click.group()
def cli():
	pass


@cli.command()
@click.argument('target')
def match(target: str):
	'''
	match `target`
	'''
	echo.push_subroutine(sys._getframe().f_code.co_name)
	
	# fetch data from database
	nichijou_db = NichijouDatabase()
	nichijou_db.add_source_column(target)
	anime_db = AnimeDatabase(target)
	
	source_data = anime_db.read_all('anime_name', ['id', 'name'])
	matched_data = nichijou_db.read_all('anime', ['nid', target])
	match_data = nichijou_db.read_all('anime_name', ['nid', 'name'])
	
	if is_null(source_data): source_data = []
	if is_null(matched_data): matched_data = []
	if is_null(match_data): match_data = []

	matched_data = [item for item in matched_data if is_not_null(item[target]) and item[target] != 0]

	# find duplicate items in `nichijou`
	if is_not_null(match_data):
		match_data = sorted(match_data, key=lambda x: x['name'])
		last = None
		dup_names = []
		for data in match_data:
			if last == data['name']:
				dup_names.append(last)
			last = data['name']

	# delete matched in `match_fail`
	if is_not_null(matched_data):
		for data in matched_data:
			nichijou_db.delete_match_fail(target, data[target])

	echo.clog(f'source length, {len(source_data)}')
	echo.clog(f'match length, {len(match_data)}')

	# delete matched in source
	if is_not_null(matched_data):
		matched_data = sorted(matched_data, key=lambda x: x[target])
	if is_not_null(source_data):
		source_data = sorted(source_data, key=lambda x: x['id'])

	j = 0
	del_cnt = 0	
	for matched in matched_data:
		while source_data[j]['id'] < matched[target]:
			if j < len(source_data) - 1: j += 1
			else: break
		while source_data[j]['id'] == matched[target]:
			source_data[j]['id'] = sys.maxsize
			del_cnt += 1
			if j < len(source_data) - 1: j += 1
			else: break
	if is_not_null(source_data):
		source_data = sorted(source_data, key=lambda x: x['id'])
	if del_cnt != 0:
		source_data = source_data[:(-1 * del_cnt)]
	
	# delete matched in match
	if is_not_null(matched_data):
		matched_data = sorted(matched_data, key=lambda x: x['nid'])
	if is_not_null(match_data):
		match_data = sorted(match_data, key=lambda x: x['nid'])
	
	j = 0
	del_cnt = 0

	for i in range(0, len(matched_data)):
		matched = matched_data[i]
		while match_data[j]['nid'] < matched['nid'] and j < len(match_data):
			if j < len(match_data) - 1: j += 1
			else: break
		while match_data[j]['nid'] == matched['nid'] and j < len(match_data):
			match_data[j]['nid'] = sys.maxsize
			del_cnt += 1
			if j < len(match_data) - 1: j += 1
			else: break
	
	if is_not_null(match_data):
		match_data = sorted(match_data, key=lambda x: x['nid'])
	if del_cnt != 0:
		match_data = match_data[:(-1 * del_cnt)]

	echo.clog('preprocess finished')
	echo.clog(f'source length, {len(source_data)}')
	echo.clog(f'match length, {len(match_data)}')

	# start matching
	processing = None
	dis_res = {}
	names = []

	cnt = 0

	for source in source_data:
		source_id = source['id']
		source_name = source['name']
		
		cnt += 1
		echo.clog(f'Handling {target} {cnt} / {len(source_data)}: ({source_id}, {source_name})')

		# initial case
		if is_null(processing):
			processing = source_id
			names.append(source_name)
		# match success and same
		elif processing == source_id * -1:
			names.append(source_name)
			if is_not_null(names):
				for name in names:
					nichijou_db.write('anime_name', {
						'nid': match_id,
						'name': name
					})
				names = []
			continue
		# continue to match
		elif processing == source_id:
			names.append(source_name)
		# new match start
		else:
			# save result to database
			if processing < 0:
				if is_not_null(names):
					for name in names:
						nichijou_db.write('anime_name', {
							'nid': match_id,
							'name': name
						})
				nichijou_db.write('anime', {
					'nid': match_id,
					target: -1 * processing
				})
			else:
				nichijou_db.write('match_fail', {
					'id': processing,
					'source': target,
					'dis': json.dumps(dis_res, ensure_ascii=False)
				})
			processing = source_id
			dis_res = {}
			names = [source_name]

		if source_name in dup_names:
			dis_arr = [{'name': source_name, 'dis': -1}]
		else:
			dis_arr = [{'name': '', 'dis': sys.maxsize} for _ in range(0, 5)]
			for match_ in match_data:
				match_id = match_['nid']
				match_name = match_['name']
				e_dis = editdistance.eval(source_name, match_name)
				dis_arr = sorted(dis_arr, key=lambda x: x['dis'])
				if e_dis == 0:
					processing = -1 * source_id
					nichijou_db.write('anime', {
						'nid': match_id,
						target: source_id
					})
					break
				elif e_dis < dis_arr[4]['dis']:
					dis_arr[4] = {'name': match_name, 'dis': e_dis}
		dis_res[source_name] = dis_arr


@cli.command()
@click.pass_context
@click.option('-f', '--config', 'config', type=click.Path(), default=None, help='match configuration file. `plan.txt` will be used if not specified.')
def run(ctx, config):
	'''
	perform matching task
	'''
	echo.push_subroutine(sys._getframe().f_code.co_name)

	targets = []
	root_dir = os.path.split(os.path.abspath(__file__))[0]
	if not os.path.exists(config):
		config = os.path.join(root_dir, './plan.txt')
	if not os.path.exists(config):
		echo.cexit('CONFIGURATION FILE NOT FOUND')
	with open(config, 'r') as f:
		targets = f.read().splitlines(keepends=False)
	echo.clog(f'targets found: {targets}')
	for target in targets:
		ctx.invoke(match, target=target)


if __name__ == '__main__':
	echo.init_subroutine()
	cli()
