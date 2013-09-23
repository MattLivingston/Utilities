import sys
import shutil
import glob
import os
import time
import fnmatch
import os.path

source_dir = '/mnt/intcluster/inflow/LexisNexis_PostSplit/'
dest_dir = '/mnt/intcluster/inflow/LexisNexisDNF/'
stop_count = 1000


def check_dir_exists(path):
	if not os.path.exists(path):
		os.makedirs(path)

x = 0



#Start at the root
for root, dirnames, filenames in os.walk(source_dir):
		for filename in filenames:
			FileP = os.path.join(root,filename)
			print FileP
			check_dir_exists(dest_dir)
			try:	
				print 'Attempting to move ' + FileP
				shutil.move(FileP, dest_dir)
			except:
				print 'Couldnt move file ' + FileP
		
			print FileP
	
			x = x + 1
			if x >= stop_count:
				#break
				print 'Waiting...'
				time.sleep(480)
				x = 0
	

print 'Finished'

	
