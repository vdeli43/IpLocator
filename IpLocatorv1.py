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
from pymssql import ProgrammingError
from argparse import ArgumentParser
from datetime import datetime
from csv import writer
from csv import register_dialect
from csv import QUOTE_MINIMAL
from sys import exc_info
#from sys import OSError

def createProcedureFromFile(filename, m_conn):
	# Open and read the file as a single buffer
	try:
		with open(filename, 'r') as fd:
			sqlFile = fd.read()
	except IOError as e:
		print('{}'.format(e))
		return False
	finally:
		fd.close()
	cur1 = m_conn.cursor(as_dict=False)
	try:
		command='SET ANSI_NULLS ON'
		cur1.execute(command)
		command='SET QUOTED_IDENTIFIER ON'
		cur1.execute(command)
		command=sqlFile
		cur1.execute(command)
	except (OperationalError, ProgrammingError) as msg:
		print ("Command skipped:{}\n{} ".format(command,msg))
		cur1.close()
		return False
	
	cur1.close()
	return True
		
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
			m_writeBlocks = int(config[Section]['WriteBlocks'])
		else:
			print('Section: {} does not exist in config file.'.format(Section))
			return None, None
		connectionString = {}
		connectionString['Server'] = Server
		connectionString['User'] = Uid
		connectionString['Password'] =  Pwd
		connectionString['Database'] = Database
		connection = connect(connectionString['Server'],connectionString['User'],connectionString['Password'], connectionString['Database'], autocommit=False)
		
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
	return connection, gi, m_writeBlocks


def argumentParser():
	table = ''
	output_file = ''
	progress_bar = ''
	parser = ArgumentParser(prog='IPLocator', description='Retrieve country origination of Clicks/ Opens')
	parser.add_argument('-C', '--config_file', default='IpLocator.ini', help='Pathname of ini file, default=IpLocator.init')
	parser.add_argument('-D', '--debug', default=False, action='store_true', help='Generate debug files as recipients_single_country.csv, TotalActions.csv and AggregatedActions.csv, Default = False')
	parser.add_argument('-T', '--table', default='Clicks', choices=['Clicks', 'Opens'], help='OperationalTable (Clicks/ Opensh. Default=Clicks')
	parser.add_argument('-O', '--output_file', default='Out.txt', help='Output file name')
	parser.add_argument('-P', '--progress_bar', action='store_true', default=False, help='Display a progress bar when run in terminal, default = False')
	parser.add_argument('-FD', '--from_date', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help='From date - format YYY-MM-DD, default = none (from beginning of time)')
	parser.add_argument('-TD', '--to_date', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help='To date - format YYY-MM-DD, default = none (until today)')
	parser.add_argument('--fetchall', action='store_true', default=False, help='Fetch all data from DB instead a row at a time. CAUTION: needs lots! of memory')
	args=parser.parse_args()
	args=parser.parse_args('-T Clicks -FD 2016-11-20 -P'.split()) #-FD 2016-11-01 -C IpLocatorZAX.ini
	#args.table = 'CK' + args.table
	if args.from_date != None:
		args.from_date=args.from_date.strftime('%Y-%m-%d 00:00:00.000')
	if args.to_date != None:
		args.to_date=args.to_date.strftime('%Y-%m-%d 23:59:59.999')
	return args

def aggregateRecipientActionsPerCountryISOCode(m_isbar, m_conn ,m_table, m_fromDate, m_toDate, m_debug, m_dbgType):
	m_unit = ' '+m_table
	numberOfRows = 0
	cur1 = m_conn.cursor(as_dict=False)
	try:
		print("Fetching {} data, please wait...".format(m_table))
		cur1.callproc('#RC_getActions', (m_table, m_fromDate , m_toDate ,'TRUE'))
		numberOfRows = [x for x in cur1][0][0]
		cur1.close()
		cur1 = m_conn.cursor(as_dict=False)
		cur1.callproc('#RC_getActions', (m_table, m_fromDate , m_toDate ,'FALSE'))
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
		pbar = tqdm(total=numberOfRows,desc="Aggregating ",unit=m_unit, miniters=0, mininterval = 1) #, unit_scale=True)
	for r in cur1:
		country_name = gi.country_code_by_addr(r[1])
		internal_id = r[0].split("_")[0][1:]
		try:
			internal_id = int(internal_id)
		except:
			continue
		#if not isinstance(internal_id,int):
			#print(internal_id)
		#if not internal_id.isdigit():
			#continue
		if internal_id > 1000000000 or internal_id  < 1:
			#print("Large Recipient_id found: {}".format(internal_id))
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
	m_recipients_single_country = list()
	for k, l in m_recipients.items():
		m_recipient_countries = l
		if m_debug:
			m_debugrow = list()
			m_debugrow.append(k)
			m_debugrow.append(m_recipient_countries)
			m_dbgType.write(m_debugrow)
		if len(m_recipient_countries) > 1:
			m_multiples += 1
		m_recipients_single_country.append((k,GetRelevantMaxCountry(m_recipient_countries))) #Nikolas
	return m_multiples, m_recipients_single_country   

def writeRecipientsSingleCountry(m_conn, m_recipients_single_country, m_debug, m_dbgSingleCountry, m_isbar, m_unit, m_writeBlocks=50, m_temp=True):
	# Write RecipientId,CountryISOCode to (temp) DB (Table (#)RecipientsRealCountry)
	# m_temp=True-> write a temp table
	# m_temp=False -> excute join proc and write the actual table
	if m_temp:
		m_table = '#RecipientsRealCountry'
		m_desc = 'Writing Temp Table '
	else:
		m_table = 'RecipientsRealCountry'
		m_desc = 'Writing Table '
	m_sqlCreateTable = """
	IF OBJECT_ID('{0}', 'U') IS NOT NULL
		DROP TABLE {0}
	CREATE TABLE {0}(id INT NOT NULL, country VARCHAR(10))
	""".format(m_table)
	m_sqlInsertRows = r"INSERT INTO {} VALUES(%d,%s)".format(m_table)
	try:
		cur1 = m_conn.cursor()
		cur1.execute(m_sqlCreateTable)
		m_conn.commit()
		cur1.close()
	except (InterfaceError, DatabaseError) as e:
		print('InterfaceError {}'.format(e))
		return(e.args[0])

	if m_temp == False:
		cur1 = m_conn.cursor(as_dict=False)
		try:
			print("Joining {} data, please wait...".format(m_table))
			cur1.callproc('#RC_findFaultyRecipients')
		except InterfaceError as e:
			print('InterfaceError {}'.format(e))
			cur1.close()
			return(e.args[0])
		except DatabaseError as e:
			print('DatabaseError {}'.format(e))
			cur1.close()
			return(e.args[0])
		m_recipients_single_country.clear()
		if m_isbar:
			join_pbar = tqdm(total=-1,desc="Joining ",unit=m_unit, miniters=0, mininterval = 1) #, unit_scale=True)
		for row in cur1:
			m_recipients_single_country.append(row)
			if m_isbar:
				join_pbar.update()
		join_pbar.close()
	
		if m_debug:
			for j in m_recipients_single_country:
				m_dbgSingleCountry.write(j)
	
	cur1 = m_conn.cursor()
	Start = 0
	End = m_writeBlocks
	if m_isbar:
		pbar = tqdm(total=len(m_recipients_single_country), desc=m_desc, unit=m_unit) #, unit_scale=True)
	while End < len(m_recipients_single_country):
		try:
			cur1.executemany(m_sqlInsertRows,m_recipients_single_country[Start:End])
		except OperationalError as e:
			print('DatabaseError {}'.format(e))
			return(e.args[0])
		finally:
			m_conn.commit()
		if m_isbar:
			pbar.update(m_writeBlocks)
		Start = End
		End = Start + m_writeBlocks
	if Start < len(m_recipients_single_country)-1:
		try:
			cur1.executemany(m_sqlInsertRows,m_recipients_single_country[Start:])
		except OperationalError as e:
			print('DatabaseError {}'.format(e))
			return(e.args[0])
		finally:
			m_conn.commit()
		if m_isbar:
			pbar.update(len(m_recipients_single_country)-Start)
			pbar.close()
	
	return 0
				
### MAIN ###
args = argumentParser()
conn, gi, writeBlocks = InitializeConnections(args.config_file)
if (conn == None or gi == None):
	exit(1)
dbgAggregatedActions = None
dbgTotalActions = None
dbgSingleCountry = None

try:
	dbgOut = debugOutput(args.output_file,1)
	if args.debug:
		dbgAggregatedActions = debugOutput('AggregatedActions.csv')
		dbgTotalActions = debugOutput('TotalActions.csv')
		dbgSingleCountry = debugOutput('recipients_single_country.csv')
		dbgAggregatedActions.write(["Recipient ID","Country ISO Codes"])		
		dbgTotalActions.write(["Recipient ID","CountryISOCode","IP"])
		dbgSingleCountry.write(["Recipient ID","Country ISO Code"])
except (OSError, ValueError) as e:
	print (e.args)
	conn.close()
	errno = e.args[0]
	exit(errno)

#create temp proc #RC_getActions
if createProcedureFromFile('RC_getActions.sql', conn) == False:
	exit(1)

rows_read, recipients = aggregateRecipientActionsPerCountryISOCode(args.progress_bar, conn, args.table, args.from_date, args.to_date, args.debug, dbgTotalActions)

legend_from = args.from_date if args.from_date != None else 'the beginning of time'
legend_to = args.to_date if args.to_date != None else 'today'
totalRecipientsFound = len(recipients)

dbgOut.write("{} rows, {} unique {} from {} until (not including) {}\n".format(rows_read, totalRecipientsFound if recipients != None else 0, args.table[:-1]+'ers', legend_from, legend_to))
if recipients == None:
	del dbgOut
	del dbgAggregatedActions
	del dbgTotalActions
	del dbgSingleCountry
	conn.close()
	exit()
	
multiples, recipients_single_country = assignBestRecipientCountryISOCode(recipients, args.debug, dbgAggregatedActions)

dbgOut.write ("{} multiples found in {} recipients\n".format(multiples,totalRecipientsFound))

# Create temp teble (all recipients, country code)
errno = writeRecipientsSingleCountry(conn, recipients_single_country, args.debug, dbgSingleCountry, args.progress_bar, ' '+args.table, m_writeBlocks=writeBlocks, m_temp=True)
if errno != 0:
	conn.close()
	exit(errno)
	
#create temp proc #RC_findFaultyRecipients
if createProcedureFromFile('RC_findFaultyRecipients.sql', conn) == False:
	exit(1)

# Create actual table (only recipients that exist in Recipients table and ...(see join proc)
errno = writeRecipientsSingleCountry(conn, recipients_single_country, args.debug, dbgSingleCountry, args.progress_bar, ' '+args.table, m_writeBlocks=writeBlocks, m_temp=False)
if errno != 0:
	conn.close()
	exit(errno)

dbgOut.write("{} misplaced recipients\n".format(len(recipients_single_country)))
res = sorted(Counter(country[1] for country in recipients_single_country).items(), key=lambda t: t[1] ,reverse=True)
for i in res:
	dbgOut.write("Country: {0:3s} -> {1:5s}ers: {2:>7d} {3:>6.2f}%\n".format(i[0], args.table[:-1], i[1], i[1]/totalRecipientsFound*100))

if args.debug:
	del dbgAggregatedActions
	del dbgTotalActions
	del dbgSingleCountry
conn.close()
del dbgOut

exit(errno)
