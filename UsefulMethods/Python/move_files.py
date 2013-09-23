import sys
import shutil
import glob
import os
import time

source_dir = '/mnt/intcluster/inflow/LexisNexisDNF2'
dest_dir = '/mnt/intcluster/inflow/LexisNexisDNF'
stop_count = 2500


def check_dir_exists(path):
	if not os.path.exists(path):
		os.makedirs(path)

x = 0

files = glob.glob(os.path.join(source_dir,'*'))


for file in files:
	filePath = os.path.join(source_dir, file)
	check_dir_exists(dest_dir)
	try:	
		if not os.path.exists(dest_dir + file):
			shutil.move(filePath, dest_dir)
	except:
		pass
		
	print filePath
	
	x = x + 1
	if x >= stop_count:
		#break
		print 'Waiting...'
		time.sleep(480)
		x = 0
	

print 'Finished'

	
