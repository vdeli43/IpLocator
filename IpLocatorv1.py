#!/usr/bin/python

#print (gi.country_code_by_name('google.com'))
#print (gi.country_code_by_addr('64.233.161.99'))
#print (gi.country_name_by_addr('64.233.161.99'))

from collections import Counter
from collections import OrderedDict
from tqdm import tqdm
from pygeoip import GeoIP
from pygeoip import GeoIPError
from pygeoip import MEMORY_CACHE
from pygeoip import MMAP_CACHE
from configparser import ConfigParser
from pymssql import connect
from pymssql import InterfaceError
from pymssql import DatabaseError
from pymssql import OperationalError
from argparse import ArgumentParser
from datetime import datetime
from csv import writer
from csv import register_dialect
from csv import QUOTE_MINIMAL
from sys import exc_info
#from sys import OSError

field_Timestamp = {'CKClicks':'click_date', 'CKOpens':'tmstamp'}   # the date field to select from in each table (Clicks/ Opens)

class debugOutput():
	# create and write debug and output files

	register_dialect(
	'mydialect',
	delimiter = ',',
	quotechar = '"',
	doublequote = True,
	skipinitialspace = True,
	lineterminator = '\r\n',
	quoting = QUOTE_MINIMAL)
	
	def __init__(self, fileName, fileType=0):
		# fileType=0 -> csv, filType=1->normal text
		if fileType != 0 and fileType != 1:
			raise ValueError('Invalid Argument for fileType', debugOutput, fileName, fileType)
		self.__fileType = fileType
		self.__debugFile = open(fileName, 'w')
		if fileType == 0:
			self.__debugWriter = writer(self.__debugFile, dialect='mydialect')
			
	def __del__(self):
		try:
			self.__debugFile.close()
		except AttributeError:
			pass
	
	def write(self,line):
		if self.__fileType == 0:
			self.__debugWriter.writerow(line)
		else:
			 self.__debugFile.write(line)

		
def InitializeConnections(ConfigFile='IpLocator.ini'):
	# Initialization file (default=IpLocator.ini) with two sections:
	# [CONNECTION} section with connection string parameters and
	# [GEODATABASE} section with the pathname of the GeoIP database file
	# Returns: connection to MSSQL and gi handler to GeoIP database
	ConnectionString = ''
	config = ConfigParser()
	try:
		with open(ConfigFile) as f:
			config.read_file(f)
		Section = 'CONNECTION'
		if config.has_section(Section):
			#Driver = config[Section]['Driver'] # only used for odbc connections, not in pymssql or _mssql
			Server = config[Section]['Server']  # server\instance_name
			Database = config[Section]['Database']
			Uid = config[Section]['Uid']
			Pwd = config[Section]['Pwd']
		else:
			print('Section: {} does not exist in config file.'.format(Section))
			return None, None
		connectionString = {}
		connectionString['Server'] = Server
		connectionString['User'] = Uid
		connectionString['Password'] =  Pwd
		connectionString['Database'] = Database
		connection = connect(connectionString['Server'],connectionString['User'],connectionString['Password'], connectionString['Database'])
		
		Section = 'GEODATABASE'
		if config.has_section(Section):
			GeoIPFile = config[Section]['GeoIPFile']
			gi = GeoIP(GeoIPFile, flags=MEMORY_CACHE) #MEMORY_CACHE
		else:
			print('Section: {} does not exist in config file.'.format(Section))
			return None, None
	except IOError as e:
		print('{}'.format(e))
		return None, None
	except GeoIPError as e:
		print('{}'.format(e))
	except KeyError as e:
		print('Item {} does not exist in configuration file {}.'.format(e,ConfigFile))
		return None, None
	except InterfaceError as e:
		print('InterfaceError {}'.format(e))
		return None,None
	except DatabaseError as e:
		print('DatabaseError {}'.format(e))
		return None,None	
	except :
		print('{}\n{}'.format(connectionString, exc_info()[0]))
		return None, None
	return connection, gi


def argumentParser():
	table = ''
	output_file = ''
	progress_bar = ''
	parser = ArgumentParser(prog='IPLocator', description='Retrieve country origination of Clicks/ Opens')
	parser.add_argument('-C', '--config_file', default='IpLocator.ini', help='Pathname of ini file, default=IpLocator.init')
	parser.add_argument('-D', '--debug', default=False, action='store_true', help='Generate debug files as recipients_single_country.csv, TotalActions.csv and AggregatedActions.csv, Default = False')
	parser.add_argument('-T', '--table', default='Clicks', choices=['Clicks', 'Opens'], help='OperationalTable (Clicks/ Opens. Default=Clicks')
	parser.add_argument('-O', '--output_file', default='Out.txt', help='Output file name')
	parser.add_argument('-P', '--progress_bar', action='store_true', default=False, help='Display a progress bar when run in terminal, default = False')
	parser.add_argument('-FD', '--from_date', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help='From date - format YYY-MM-DD, default = none (from beginning of time)')
	parser.add_argument('-TD', '--to_date', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help='To date - format YYY-MM-DD, default = none (until today)')
	parser.add_argument('--fetchall', action='store_true', default=False, help='Fetch all data from DB instead a row at a time. CAUTION: needs lots! of memory')
	#args=parser.parse_args()
	args=parser.parse_args('-C IpLocatorZAX.ini -P -D -FD 2016-06-15'.split()) #-FD 2016-11-01
	args.table = 'CK' + args.table
	if args.from_date != None:
		args.from_date=args.from_date.strftime('%Y-%m-%d 00:00:00.000')
	if args.to_date != None:
		args.to_date=args.to_date.strftime('%Y-%m-%d 23:59:59.999')
	return args


def build_SQL(m_args):
	# Construct SQL SLECT STRINGS according to arguments
	sql_statement1 = "SELECT subid2, ip_address FROM {}".format(args.table) # the actual selection
	sql_statement2 = "SELECT count(*) FROM {}".format(args.table) # just a counter to get the number of rows
	if (args.from_date != None or args.to_date != None):
		if (args.from_date != None and args.to_date != None):
			part = " WHERE {} >= '{}' AND {} <= '{}'".format(field_Timestamp[args.table], args.from_date, field_Timestamp[args.table], args.to_date)
		elif args.from_date != None:
			part = " WHERE {} >= '{}'".format(field_Timestamp[args.table], args.from_date)
		else:
			part =" WHERE {} <= '{}'".format(field_Timestamp[args.table], args.to_date)
		sql_statement1 += part
		sql_statement2 += part
	return sql_statement1, sql_statement2

def aggregateRecipientActionsPerCountryISOCode(m_isbar, m_unit ,m_conn ,m_SQL, m_debug, m_dbgType, m_fetchall):
	numberOfRows = 0
	cur1 = m_conn.cursor()
	try:
		if m_fetchall:
			print("Fetching data, please wait...")
			cur1.execute(m_SQL[0])
			rows = cur1.fetchall()
		else:
			cur1.execute(m_SQL[1])
			numberOfRows = cur1.fetchone()[0]
			cur1.close()
			cur1 = m_conn.cursor()
			cur1.execute(m_SQL[0])
			rows = cur1
	except InterfaceError as e:
		print('InterfaceError {}'.format(e))
		cur1.close()
		return 0,None
	except DatabaseError as e:
		print('DatabaseError {}'.format(e))
		cur1.close()
		return 0,None
	recipients = {}
	counter = 0

	if m_isbar:
		if m_fetchall:
			pbar = tqdm(iterable=cur1,desc="Aggregating ",unit=m_unit, miniters=0, mininterval = 1) #, unit_scale=True)
		else:
			pbar = tqdm(total=numberOfRows,desc="Aggregating ",unit=m_unit, miniters=0, mininterval = 1) #, unit_scale=True)
	for r in rows:
		country_name = gi.country_code_by_addr(r[1])
		internal_id = r[0].split("_")[0][1:]
		if not internal_id.isdigit():
			continue
		if m_debug:
			m_dbgType.write([internal_id,country_name,r[1]])
		if internal_id not in recipients:
			recipients[internal_id] = {}
		if country_name not in recipients[internal_id]:
			recipients[internal_id][country_name] = 1
		else:
			recipients[internal_id][country_name] += 1
		counter += 1
		if m_isbar:
			pbar.update()
	if m_isbar:
		pbar.close()
	cur1.close()
	return counter, recipients

def GetListOfKeysCorrespondingToMaxValues(mahdict):
    max_list = list()
    max = -1
    for key in mahdict.keys():
        if mahdict[key] > max:
            max = mahdict[key]
            max_list = [key,]
        elif mahdict[key] == max:
            max_list.append(key)
    return max_list

def GetRelevantMaxCountry(mahdict):
    temp = GetListOfKeysCorrespondingToMaxValues(mahdict)
    if len(temp) == 1: return temp[0]
    if 'GB' in temp:
        return 'GB'
    elif 'US' in temp:
        return 'US'
    elif 'FR' in temp:
        return 'FR'
    return temp[0]


def assignBestRecipientCountryISOCode(m_recipients, m_debug, m_dbgType):
	# finds the most common Country ISOCode among recipient actions
	# returns a dict with recipient: ISOCode and the number of recipients with actions from more than one countries (multiples)
	m_multiples = 0
	m_recipients_single_country = {}
	for k in m_recipients:
		m_recipient_countries = m_recipients[k]
		if m_debug:
			m_debugrow = list()
			m_debugrow.append(k)
			m_debugrow.append(m_recipient_countries)
			m_dbgType.write(m_debugrow)
		if len(m_recipient_countries) > 1:
			m_multiples += 1
		m_recipients_single_country[k] = GetRelevantMaxCountry(m_recipient_countries) #Nikolas
	return m_multiples, m_recipients_single_country   

### MAIN ###
args = argumentParser()
SQL = build_SQL(args)
conn, gi = InitializeConnections(args.config_file)
if (conn == None or gi == None):
	exit(1)

if args.debug:
	try:
		dbgAggregatedActions = debugOutput('AggregatedActions.csv')
		dbgTotalActions = debugOutput('TotalActions.csv')
		dbgSingleCountry = debugOutput('recipients_single_country.csv')
	except (OSError, ValueError) as e:
		print(e.args)
		conn.close()
		errno = e.args[0]
		exit(errno)
	dbgAggregatedActions.write(["Recipient ID","Country ISO Codes"])		
	dbgTotalActions.write(["Recipient ID","CountryISOCode","IP"])
	dbgSingleCountry.write(["Recipient ID","Country ISO Code"])		

try:
	dbgOut = debugOutput(args.output_file,1)
except (OSError, ValueError) as e:
	print (e.args)
	conn.close()
	errno = e.args[0]
	exit(errno)

rows_read, recipients = aggregateRecipientActionsPerCountryISOCode(args.progress_bar, ' '+args.table[2:], conn, SQL, args.debug, dbgTotalActions, args.fetchall)
	
legend_from = args.from_date if args.from_date != None else 'the beginning of time'
legend_to = args.to_date if args.to_date != None else 'today'
dbgOut.write("{} rows, {} unique {} from {} until {}\n".format(rows_read, len(recipients) if recipients != None else 0, args.table[2:-1]+'ers', legend_from, legend_to))
if recipients == None:
	del dbgOut
	del dbgAggregatedActions
	del dbgTotalActions
	del dbgSingleCountry
	conn.close()
	exit()
	
multiples, recipients_single_country = assignBestRecipientCountryISOCode(recipients, args.debug, dbgAggregatedActions)

dbgOut.write ("{} multiples found in {} recipients\n\n".format(multiples,len(recipients)))

res = OrderedDict(sorted(Counter(recipients_single_country.values()).items(), key=lambda t: t[1], reverse=True))
for i in res:
	dbgOut.write("Country: {} -> {}ers: {}\n".format(i,args.table[2:-1], res[i]))


try:
	cur1 = conn.cursor()
	cur1.execute("""
	IF OBJECT_ID('RecipientsRealCountry', 'U') IS NOT NULL
		DROP TABLE RecipientsRealCountry
	CREATE TABLE RecipientsRealCountry(id INT NOT NULL, country VARCHAR(10))
	""")
	conn.commit()
	cur1.close()
except InterfaceError as e:
	print('InterfaceError {}'.format(e))
	exit()
except DatabaseError as e:
	if e.number == 2714 and e.severity == 16:
		# Database already exits
		pass
	else:
		print('DatabaseError {}'.format(e))
		exit()

cur1 = conn.cursor()
for resp in recipients_single_country:
	if args.debug:
		dbgSingleCountry.write([resp,recipients_single_country[resp]])
		try:
			cur1.execute("INSERT INTO RecipientsRealCountry VALUES(%d,%s)",(resp,recipients_single_country[resp]))
		except OperationalError as e:
			if e.args[0] == 245:# and e.severity == 16:
				# id must be numeric, TEST id
				break
			else:
				print('DatabaseError {}'.format(e))
				exit()
		finally:
			conn.commit()

if args.debug:
	del dbgAggregatedActions
	del dbgTotalActions
	del dbgSingleCountry
conn.close()
del dbgOut
