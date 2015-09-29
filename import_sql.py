# Python: out of the box
import os, sys, json, re

# Python: installed packages
import requests, pyodbc
import mysql.connector as mydb

# Debugging tools
from pprint import pprint

############
# SETTINGS #
############

# URL to use for web site SLUGs
URL = 'http://www.yourwpwebsite.org/'

# Meeting types: SQL Server columns to tsml plugin codes to meeting types
MTG_COLUMNS = {
    'WHEELCHAIR': 'X',
    'SMOKELOC': 'SM',
    'MENLOC': 'M',
    'WOMENLOC': 'W',
    'LGBT': 'G',
    'YoungPeople': 'Y',
    'Agnostic': 'A'
}

# Map of SQL Server MTGTYPE column to tsml plugin codes for meeting types
# L (Literature) mapped to B (Big Book)
# V (Varied) not mapped (is that redundant?)
MTGTYPE_MAP = {
    'SD': 'SP',
    'BB': 'B',
    'FF': 'XT',
    'GV': 'GR',
    'O': 'O',
    'C': 'C',
    'D': 'D',
    'T': 'TR',
    'S': 'ST',
    'B': 'BE',
    'L': 'B',
    'A': 'A'
}

# API KEY From Google Geocode API
API_KEY = 'yourapikey'
EXECUTE_GEOCODE = 1

# Connection string to SQL SERVER production database from Linux
PROD_SQL_CONNECT = 'DRIVER={FreeTDS};SERVER=yoursqlserver.com;PORT=1433;DATABASE=yourdb;UID=youruser;PWD=yourpass;TDS_Version=7.2;'

# Whether or not to execute the SQL on the destination
EXECUTE_SQL = 0
# Use phony data
FAKE_ROW = 0

############
# CODEBASE #
############

if(FAKE_ROW):
    ms_cursor = (
        { 'ID': 1, 'Address': '613 annin st phila pa 19147'},
        { 'ID': 2, 'Address': '1811 Baombrodge Street phila pa 19147'},
    )
else:
    # Connect to MySQL for writing (later)
    my_con = mydb.connect(
        host='localhost',
        user='yourmysqluser',
        password='yourmysqlpass',
        database='yourmysqldb'
    )
    my_cur = my_con.cursor()

    # Connect to MS SQL for reading
    ms_con = pyodbc.connect(PROD_SQL_CONNECT, autocommit=True)
    ms_cursor = ms_con.cursor()

    # Customize the query. 'post-content' becomes the meeting details (a few sentences). The ADDRESS is what is Geocoded
    # and inserted as the final meeting address. This will remove duplicates and fix bad addresses.
    sql = "SELECT CAST(pk_Id AS VARCHAR(36)) AS ID, ISNULL([star 1], '') AS post_content, * FROM Sepennaa_Meeting"
    ms_cursor.execute(sql)

for ms_row in ms_cursor:
    if(ms_row.Address):
        cache_filename = 'cache/' + re.sub('[^0-9a-zA-Z]+', '', ms_row.Address.lower()) + '.json'

        if(os.path.isfile(cache_filename)):
            print('Cache hit on ' + ms_row.Address + ', loading from disk...')
            # grab the json from the cache
            with open(cache_filename, 'r') as f:
                cached_json = f.read()

            try:
                # JSON is valid
                address_data = json.loads(cached_json)
            except:
                # Invalid JSON, delete the file, get on next run
                os.remove(cache_filename)

        elif EXECUTE_GEOCODE:
            print('Cache miss on ' + ms_row.Address + ', checking Google Maps...')

            payload = {'key': API_KEY, 'address': ms_row.Address}
            api_request_url = 'https://maps.googleapis.com/maps/api/geocode/json'

            # Send the request to Google Maps
            r = requests.get(api_request_url, params=payload)
            address_data = r.json()

            # Write cache file if status == OK
            if(address_data['status'] == 'OK'):
                with open(cache_filename, 'w') as f:
                    json.dump(address_data, f, indent=4, separators=(',', ': '))

        # We have the data in 'address_data', let's do something with
        # each address and the associated meeting information.
        # Test to see if LOCATION address exists. If so, return the id from MySQL
        address_components = {}

        for address_component in address_data['results'][0]['address_components']:
            for address_component_type in address_component['types']:
                address_components[address_component_type] = address_component['long_name']

        address_components['lat'] = address_data['results'][0]['geometry']['location']['lat']
        address_components['lng'] = address_data['results'][0]['geometry']['location']['lng']

        """
EXAMPLE OF address_components:

{'administrative_area_level_1': 'Pennsylvania',
 'administrative_area_level_2': 'Bucks County',
 'administrative_area_level_3': 'Middletown',
 'country': 'United States',
 'lat': 40.1993373,
 'lng': -74.9299692,
 'locality': 'Langhorne',
 'political': 'United States',
 'postal_code': '19047',
 'route': 'Newtown-Langhorne Road',
 'street_number': '1110'}
        """

        if 'street_number' in address_components and 'route' in address_components:
            gc_address = address_components['street_number'] + ' ' + address_components['route']
        else:
            gc_address = ''

        if 'locality' in address_components:
            gc_city = address_components['locality']
        elif 'neighborhood' in address_components:
            gc_city = address_components['neighborhood']
        else:
            gc_city = ''

        if 'postal_code' in address_components:
            gc_zip = address_components['postal_code']
        else:
            gc_zip = ''

        post_name = re.sub('[^0-9a-zA-Z]+', '-', gc_address + ' ' + gc_city + ' PA ' + gc_zip).lower()
        guid = URL + 'locations/' + post_name

        # Get the region
        region_id = 0
        if ms_row.County == 'MONTGOMERY':
            ms_row.County = 'MONTGOMERY COUNTY'
        my_cur.execute("SELECT term_id FROM wp_terms WHERE name = %(county)s ORDER BY term_id LIMIT 1", { 'county': ms_row.County.strip() })
        my_rows = my_cur.fetchall()
        for my_row in my_rows:
            region_id = my_row[0]

        # Check to see if location exists
        my_cur.execute("SELECT ID from wp_posts WHERE post_type='locations' AND post_name = %(post_name)s", { 'post_name': post_name })
        my_rows = my_cur.fetchall()

        if(my_cur.rowcount > 0):
            for my_row in my_rows:
                location_id = my_row[0]
            print('Found existing location, wp_posts ID: ' + str(location_id))
        else:
            # If location doesn't exist, insert it, get the inserted id from MySQL
            my_insert_sql = """INSERT INTO wp_posts (
            ID,
            post_author,
            post_date,
            post_date_gmt,
            post_content,
            post_title,
            post_excerpt,
            post_status,
            comment_status,
            ping_status,
            post_password,
            post_name,
            to_ping,
            pinged,
            post_modified,
            post_modified_gmt,
            post_content_filtered,
            post_parent,
            guid,
            menu_order,
            post_type,
            post_mime_type,
            comment_count)
            VALUES ('', %(author_id)s, now(), now(), '', %(post_title)s, '', 'publish', 'closed', 'closed', '', %(post_name)s, '', '', now(), now(), '', 0, %(guid)s, 0, 'locations', '', 0)
            """

            # Insert location as custom post type
            # post_author, post_title, post_name, guid
            my_cur.execute(my_insert_sql, { 'author_id': 1, 'post_title': gc_address, 'post_name': post_name, 'guid': guid })
            location_id = my_cur.lastrowid
            print('Inserted new location, wp_posts ID: ' + str(location_id))

        # Delete and re-insert post metadata info for location - handles updates in source database
        my_cur.execute("DELETE FROM wp_postmeta WHERE post_id = %(post_id)s", { 'post_id': location_id })
        my_insert_sql = """INSERT INTO wp_postmeta (meta_id, post_id, meta_key, meta_value) VALUES
            ('', %(post_id)s, 'formatted_address', %(formatted_address)s),
            ('', %(post_id)s, 'address', %(address)s),
            ('', %(post_id)s, 'city', %(city)s),
            ('', %(post_id)s, 'state', 'PA'),
            ('', %(post_id)s, 'postal_code', %(postal_code)s),
            ('', %(post_id)s, 'country', 'US'),
            ('', %(post_id)s, 'latitude', %(latitude)s),
            ('', %(post_id)s, 'longitude', %(longitude)s),
            ('', %(post_id)s, 'region', %(region_id)s),
            ('', %(post_id)s, 'mssql_location_pk_id', %(mssql_location_pk_id)s)
        """
        my_cur.execute(my_insert_sql, {
            'post_id': location_id,
            'formatted_address': gc_address + ', ' + gc_city + ', PA ' + gc_zip +', US',
            'address': gc_address,
            'city' : gc_city,
            'postal_code': gc_zip,
            'latitude': address_components['lat'],
            'longitude': address_components['lng'],
            'region_id': region_id,
            'mssql_location_pk_id': ms_row.pk_Id
        })
            
        my_con.commit()

        # Prepare to populate meeting postmeta data
        if(ms_row.Day == "Sunday"):
            mtg_day = 0
        elif(ms_row.Day == "Monday"):
            mtg_day = 1
        elif(ms_row.Day == "Tuesday"):
            mtg_day = 2
        elif(ms_row.Day == "Wednesday"):
            mtg_day = 3
        elif(ms_row.Day == "Thursday"):
            mtg_day = 4
        elif(ms_row.Day == "Friday"):
            mtg_day = 5
        elif(ms_row.Day == "Saturday"):
            mtg_day = 6

        mtg_time = str(ms_row.MTGTIME)[11:16]

        # postmeta key "types" construction
        mtg_options = ''
        mtg_options_total = 0
        mtg_options_index = 0
        mtg_options_code_used = []

        # These are options that are boolean columns in the SQL Server source database
        for mtg_options_column, mtg_options_code in MTG_COLUMNS.items():
            if(getattr(ms_row, mtg_options_column)):
                mtg_options = mtg_options + 'i:' + str(mtg_options_index) + ';s:' + str(len(mtg_options_code)) + ':"' + mtg_options_code + '";'
                mtg_options_code_used.append(mtg_options_code)
                mtg_options_total += 1
                mtg_options_index += 1

        # These are options that are characters mashed into a string in a single column
        # For extra win, some are a superset of others! I.e.: BB (Big Book) versus B (beginners).
        # So, we start with the longest codes, stripping them from the mashed string before the
        # next loop.
        for mtg_options_mssql_code, mtg_options_code in MTGTYPE_MAP.items():
            if ms_row.MTGTYPE is not None and mtg_options_mssql_code in ms_row.MTGTYPE and mtg_options_code not in mtg_options_code_used:
                mtg_options = mtg_options + 'i:' + str(mtg_options_index) + ';s:' + str(len(mtg_options_code)) + ':"' + mtg_options_code + '";'
                ms_row.MTGTYPE.replace(mtg_options_mssql_code, '')
                mtg_options_total += 1
                mtg_options_index += 1

        mtg_options = 'a:' + str(mtg_options_total) + ':{' + mtg_options + '}'

        mtg_post_name = (re.sub('[^0-9a-zA-Z]+', '-', ms_row.GROUPNAME) + '-' + ms_row.Day + '-' + mtg_time.replace(':', '-')).lower()

        # Now, let's see if the meeting exists
        my_cur.execute("SELECT post_id from wp_postmeta WHERE meta_key = 'mssql_meeting_pk_id' AND meta_value = %(mssql_meeting_pk_id)s", { 'mssql_meeting_pk_id': ms_row.pk_Id })
        my_rows = my_cur.fetchall()

        if(my_cur.rowcount > 0):
            for my_row in my_rows:
                meeting_id = my_row[0]
            my_cur.execute("UPDATE wp_posts SET post_title = %(post_title)s, post_content = %(post_content)s, post_name = %(post_name)s, guid = %(guid)s WHERE ID = %(ID)s", { 'post_title': ms_row.GROUPNAME, 'post_content': ms_row.post_content, 'ID': meeting_id, 'post_name': mtg_post_name, 'guid': URL + 'meetings/' + mtg_post_name })
        else:
            # Meeting does not exist, insert it
            my_insert_sql = """INSERT INTO wp_posts (
            ID,
            post_author,
            post_date,
            post_date_gmt,
            post_content,
            post_title,
            post_excerpt,
            post_status,
            comment_status,
            ping_status,
            post_password,
            post_name,
            to_ping,
            pinged,
            post_modified,
            post_modified_gmt,
            post_content_filtered,
            post_parent,
            guid,
            menu_order,
            post_type,
            post_mime_type,
            comment_count)
            VALUES ('', %(author_id)s, now(), now(), %(post_content)s, %(post_title)s, '', 'publish', 'closed', 'closed', '', %(post_name)s, '', '', now(), now(), '', %(post_parent)s, %(guid)s, 0, 'meetings', '', 0)
            """

            # Insert meeting as custom post type, child post of location
            my_cur.execute(my_insert_sql, { 'author_id': 1, 'post_content': ms_row.post_content, 'post_title': ms_row.GROUPNAME, 'post_name': mtg_post_name, 'post_parent': location_id, 'guid': URL + 'meetings/' + mtg_post_name })
            meeting_id = my_cur.lastrowid

        # Delete meeting metadata; recreate with new data
        my_cur.execute("DELETE FROM wp_postmeta WHERE post_id = %(meeting_id)s", { 'meeting_id': meeting_id })
        my_insert_sql = """INSERT INTO wp_postmeta (meta_id, post_id, meta_key, meta_value) VALUES
        ('', %(post_id)s, 'day', %(day)s),
        ('', %(post_id)s, 'time', %(time)s),
        ('', %(post_id)s, 'types', %(types)s),
        ('', %(post_id)s, 'region', %(region)s),
        ('', %(post_id)s, 'mssql_meeting_pk_id', %(mssql_meeting_pk_id)s)
        """
        my_cur.execute(my_insert_sql, {
            'post_id': meeting_id,
            'day': mtg_day,
            'time': mtg_time,
            'types': mtg_options,
            'region': region_id,
            'mssql_meeting_pk_id': ms_row.pk_Id
        })
        my_con.commit()
