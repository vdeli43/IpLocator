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
from _mssql import connect
from _mssql import MSSQLDriverException
from _mssql import MSSQLDatabaseException
from argparse import ArgumentParser
from datetime import datetime
from csv import writer
from csv import register_dialect
from csv import QUOTE_MINIMAL

field_Timestamp = {'CKClicks':'click_date', 'CKOpens':'tmstamp'}   # the date field to select from in each table (Clicks/ Opens)

def InitializeConnections(ConfigFile='IpLocator.ini'):
	# Initialization file (default=IpLocator.ini) with two sections:
	# [CONNECTION} section with connection string parameters and
	# [GEODATABASE} section with the pathname of the GeoIP database file
	# Returns: connectionString toMSSQL and gi handler to GeoIP database
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
		connectionString['Password'] = Pwd
		connectionString['Database'] = Database
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
	except Error as e:
		print('{}\n{}'.format(ConnectionString,e))
		return None, None
	return connectionString, gi


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
	args=parser.parse_args('-P -D -FD 2016-11-01'.split()) #-FD 2016-11-01
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

def aggregateRecipientActionsPerCountryISOCode(m_isbar, m_unit ,m_connString,m_SQL, m_debug, m_fetchall):
	numberOfRows = 0
	cur1 = None
	try:
		connection = connect(m_connString['Server'],m_connString['User'],m_connString['Password'])
		connection.select_db(m_connString['Database'])
		if m_fetchall:
			print("Fetching data, please wait...")
			connection.execute_query(m_SQL[0])
			cur1 = [r for r in connection]
			connection.close()
		else:
			numberOfRows = connection.execute_scalar(m_SQL[1])
			connection.execute_query(m_SQL[0])
			cur1 = connection
	except MSSQLDriverException as e:
		print('MSSQLDriverException {}'.format(e))
		return 0,None
	except MSSQLDatabaseException as e:
		print('MSSQLDatabaseException {}'.format(e))
		return 0,None
	recipients = {}
	counter = 0
	
	if m_debug:
		actionsFile = open('TotalActions.csv', 'w')
		debugdatawriter = writer(actionsFile, dialect='mydialect')
		debugdatawriter.writerow(["Recipient ID","CountryISOCode","IP"])

	if m_isbar:
		if m_fetchall:
			pbar = tqdm(iterable=cur1,desc="Aggregating ",unit=m_unit, miniters=0, mininterval = 1) #, unit_scale=True)
		else:
			pbar = tqdm(total=numberOfRows,desc="Aggregating ",unit=m_unit, miniters=0, mininterval = 1) #, unit_scale=True)
	for r in cur1:
		country_name = gi.country_code_by_addr(r[1])
		internal_id = r[0].split("_")[0][1:]
		if m_debug:
			debugdatawriter.writerow([internal_id,country_name,r[1]])
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
	if m_debug:
		actionsFile.close()
	if m_fetchall:
		connection.close()
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

### MAIN ###
args = argumentParser()
SQL = build_SQL(args)
connString, gi = InitializeConnections(args.config_file)
if (connString == None or gi == None):
	exit()

register_dialect(
    'mydialect',
    delimiter = ',',
    quotechar = '"',
    doublequote = True,
    skipinitialspace = True,
    lineterminator = '\r\n',
    quoting = QUOTE_MINIMAL)

rows_read, recipients = aggregateRecipientActionsPerCountryISOCode(args.progress_bar, ' '+args.table[2:], connString, SQL, args.debug, args.fetchall)
	
f = open(args.output_file, 'w')
legend_from = args.from_date if args.from_date != None else 'the beginning of time'
legend_to = args.to_date if args.to_date != None else 'today'
f.write ("{} rows, {} unique {} from {} until {}\n".format(rows_read, len(recipients) if recipients != None else 0, args.table[2:-1]+'ers', legend_from, legend_to))
if recipients == None:
	f.close()
	exit()
	
recipients_single_country = {}
multiples = 0

if args.debug:
	aggregatedActionsFile = open('AggregatedActions.csv', 'w')
	debugdatawriter = writer(aggregatedActionsFile, dialect='mydialect')
	debugdatawriter.writerow(["Recipient ID","Country ISO Codes"])
	
for k in recipients:
	recipient_countries = recipients[k]
	if args.debug:
		debugrow = list()
		debugrow.append(k)
		debugrow.append(recipient_countries)
		debugdatawriter.writerow(debugrow)
	if len(recipient_countries) > 1:
		multiples += 1
	recipients_single_country[k] = GetRelevantMaxCountry(recipient_countries)

if args.debug:
	aggregatedActionsFile.close()

f.write ("{} multiples found in {} recipients\n\n".format(multiples,len(recipients)))

res = OrderedDict(sorted(Counter(recipients_single_country.values()).items(), key=lambda t: t[1], reverse=True))
for i in res:
	f.write("Country: {} -> {}ers: {}\n".format(i,args.table[2:-1], res[i]))

if args.debug:
	outputDebugFile = open('recipients_single_country.csv', 'w')
	debugdatawriter = writer(outputDebugFile, dialect='mydialect')
	debugdatawriter.writerow(["Recipient ID","Country ISO Code"])
	for resp in recipients_single_country:
		debugdatawriter.writerow([resp,recipients_single_country[resp]])
	outputDebugFile.close()

