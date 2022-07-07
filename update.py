import argparse
from ftplib import FTP
from threading import Thread
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)

from etl import *
from transform import *

ftp_data = ('ftp.zakupki.gov.ru', 'free', 'free')

if __name__ == '__main__':
	# collect arguments
	parser = argparse.ArgumentParser(description='Update zakupki database.')

	parser.add_argument(choices=['all', 'inc'], dest='type', action='store')  # inc doesnt work now
	parser.add_argument('-p', '--protocols', dest='collections', action='append_const', const='protocols')
	parser.add_argument('-n', '--notifications', dest='collections', action='append_const', const='notifications')
	args = parser.parse_args()
	if not args.collections:  # if no collections provided, use all
		args.collections = ['notifications', 'protocols']

	print(ts(), 'Starting {type} update'.format(type=args.type))

	for i, coll in enumerate(args.collections):
		print(ts(), 'Updating {coll}'.format(coll=coll))
		# regions = get_regions(ftp, 0, 4)
		# print(regions)
		# threads = [Thread(target=etl, args=(ftp, coll, args.type, [region])) for region in regions]
		# [t.start() for t in threads]
		# [t.join() for t in threads]

		etl(ftp_data, coll, args.type)  # , regions=['Neneckij_AO'], regions_start=0, regions_end=86

	print('Data has been downloaded.')
