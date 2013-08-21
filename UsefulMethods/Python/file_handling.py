import ntpath







def getFileName(path):
	"""
	returns file name from a path while handling the case of a tailing /
	requires: import ntpath
	"""
	head, tail = ntpath.split(path)
	return tail or ntpath.basename(head)	