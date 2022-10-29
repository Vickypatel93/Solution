import logging

def get_logger(name):
	path = '../info.log'
	# Gets or creates a logger
	logger = logging.getLogger(name)

	# set log level
	logger.setLevel(logging.DEBUG)

	# define file handler and set formatter
	file_handler = logging.FileHandler(path)
	formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
	file_handler.setFormatter(formatter)

	# add file handler to logger
	logger.addHandler(file_handler)

	return logging.getLogger(name)