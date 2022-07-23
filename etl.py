from ftplib import FTP

from lxml import etree
from zipfile import ZipFile
from tqdm import tqdm
import traceback
from threading import Thread

from transform import *
from utils import *
from sql import *


def thread_work(data, id, to_parse, ftp_data):
	ftp = get_ftp(ftp_data)
	cc = []
	index = 0
	while True:
		for directory, f in data[index:]:
			try:
				ftp.cwd(directory)
				zip_file = retr(ftp, f)
			except:
				ftp.close()
				ftp = get_ftp(ftp_data)
				break
			assert zip_file
			cc.append([zip_file, id])
			index += 1
		if index == len(data):
			break
	for d in cc:
		to_parse.append(d)
	ftp.close()

def thread_work2(data, r, f):
	parsed = []
	for [file, region] in data:
		try:
			zip_file = ZipFile(file)
		except:
			continue
		zip_names = zip_file.namelist()
		for name in zip_names:
			result = parse_xml(zip_file, name, region)
			if result:
				parsed.append(result)
		zip_file.close()
	for p in parsed:
		push_to_db(p)


def etl(ftp_data, coll, update_type, regions=None, regions_start=None, regions_end=None):
	"""
		regions - list of regions to parse
		regions_start, regions_end - borders that limit list of parsed regions
	"""
	ftp = get_ftp(ftp_data)
	ftp.cwd('/fcs_regions')
	if not regions:
		regions = get_regions(ftp, regions_start, regions_end)
	ftp.close()
	print(ts(), 'regions: {regions}'.format(regions=regions))
	files = {}
	start_region = -3
	end_region = -2
	threads = 100
	print(f'Totally {len(regions)} regions')
	for idx, region in enumerate(regions[:]):
		print(f'Left {len(regions) - 0 - idx} regions')
		to_parse = []
		ftp = get_ftp(ftp_data)
		while True:
			try:
				region_files = inc_files(coll, ftp, region) if update_type == 'inc' else all_files(coll, ftp, region)
			except:
				ftp.close()
				ftp = get_ftp(ftp_data)
				continue
			break
		files[region] = region_files
		try:
			region_id, = one_row_request("INSERT INTO regions (region) VALUES (%s) RETURNING id;",
										 [region], if_commit=True)
		except psycopg2.IntegrityError:
			region_id, = one_row_request("SELECT id FROM regions WHERE region = %s;", [region])
		print(f'{ts()} region {region} started')
		data = [[] for _ in range(threads)]
		index = 0
		total = 0
		for directory, region_dicts in region_files.items():
			for f in region_dicts:
				data[index].append((directory, f))
				index += 1
				total += 1
				if index == threads:
					index = 0
		th = [Thread(target=thread_work, args=(data[i], region_id, to_parse, ftp_data)) for i in range(threads)]
		[t.start() for t in th]
		[t.join() for t in th]
		assert len(to_parse) == total
		print(f'{ts()} region {region} acquired by ftp')
		ftp.close()
		data = [[] for _ in range(threads)]
		index = 0
		for [file, region] in to_parse:
			data[index].append([file, region])
			index += 1
			if index == threads:
				index = 0
		th = [Thread(target=thread_work2, args=(data[i], region_id, ftp_data)) for i in range(threads)]
		[t.start() for t in th]
		[t.join() for t in th]
		print(f'{ts()} region {regions[idx]} {idx} parsed && pushed')



def parse_xml(zip_file, name, region_id):
	try:
		if name.startswith('fcsNotificationZK'):
			xml_file = zip_file.open(name)
			return (0, etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}fcsNotificationZK'), region_id)
		elif name.startswith('fcsPurchaseProlongationZK'):
			xml_file = zip_file.open(name)
			return (1, etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}fcsPurchaseProlongationZK'), region_id)
		elif name.startswith('fcsProtocolZKAfterProlong'):
			xml_file = zip_file.open(name)
			return (2, etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}fcsProtocolZKAfterProlong'), region_id)
		elif name.startswith('fcsProtocolZK'):
			xml_file = zip_file.open(name)
			return (3, etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}fcsProtocolZK'), region_id)
	except etree.XMLSyntaxError:
		return None
	except Exception:
		traceback.print_exc()
		return None

def push_to_db(p):
	type, parsed, region_id = p
	try:
		if type == 0:
			for event, xml in parsed:
				if event == 'end':
					transform_notifications(xml, region_id)
					return
		elif type == 1:
			for event, xml in parsed:
				if event == 'end':
					transform_notifications_prolong(xml, region_id)
					return
		elif type == 2:
			for event, xml in parsed:
				if event == 'end':
					transform_protocols(xml, region_id, if_prolong=True)
					return
		elif type == 3:
			for event, xml in parsed:
				if event == 'end':
					transform_protocols(xml, region_id, if_prolong=False)
					return
	except Exception:
		return


def get_ftp(ftp_data):
	return FTP(ftp_data[0], ftp_data[1], ftp_data[2])