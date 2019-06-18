from collections import namedtuple
import csv
import re
import shelve
import sys
import time

import requests
import xmltodict

from CSV_Gen import CSV_Gen



endpoint_url = "http://classify.oclc.org/classify2/Classify"  # OCLC Classify API URL
base_querystring = "?summary=true&maxRecs=1"
ns = {"classify": "http://classify.oclc.org"} 



csv_gen = CSV_Gen()


def get_person_and_role(name_string):
    if 'editor' in name_string:
        role = 'editor'
        name_string = name_string.replace(', editor', '')
    else:
        role = 'author'
    tidy_name_string = name_string.replace('[person]', '').strip()
    tidy_name_string = re.sub(r',\s{1}[0-9]{4}-[0-9]{0,4}', '', tidy_name_string)
    return tidy_name_string, role

def get_organisation(name_string):
    return name_string.replace('[organisation]', '').strip()



@csv_gen.transform_field_value('all_names')
def extract_names(names_string):

    names = {
        'persons': {'authors': [], 'editors': []},
        'organisations': [],
    }
    for name_string in names_string.split(' ; '):
        if '[person]' in name_string:
            person, role = get_person_and_role(name_string)
            names['persons'][f'{role}s'].append(person)
        elif '[organisation]' in name_string:
            organisation = get_organisation(name_string)
            names['organisations'].append(organisation)
    return names

@csv_gen.transform_field_value('isbn')
def split_isbn(isbn):
    return isbn.split(' ; ')[0]





cache = set()


LookupData = namedtuple('LookupData', ['type', 'value'])



def determine_lookup_data(book):
    isbn = book['isbn']
    if isbn:
        return LookupData('isbn', isbn)
    title = book['title']
    if title:
        authors = book['all_names']['persons']['authors']
        if authors:
            return LookupData('author_title', (authors[0], title))  
        else:
            organisations = book['all_names']['organisations']
            if organisations:
                return LookupData('author_title', (organisations[0], title))
            else:
                return LookupData('title', title)
    return None


def get_OCLC_data(lookup_data):
    
    
    print(lookup_data)
    if lookup_data in cache:
        print(f'{lookup_data} in cache!')
        with shelve.open('cache.shelve') as cache_shelve:
            return cache_shelve[repr(lookup_data)]

    if lookup_data.type in ['isbn', 'wi']:
        query = f'&{lookup_data.type}={lookup_data.value}'
    elif lookup_data.type == 'author_title':
        query = f'&author={lookup_data.value[0]}'
        query += f'&title={lookup_data.value[1]}'
    elif lookup_data.type == 'title':
        query = f'&title={lookup_data.value}'

    query = endpoint_url + base_querystring + query

    try:
        response = requests.get(query)
    except TimeoutError: # if a timeout
        print('Timeout error raised; waiting 5 minutes.')
        time.sleep(300)  # wait ages
        return get_OCLC_data(lookup_data) # then just call the function again?
    
    if response.status_code == 200:
        with shelve.open('cache.shelve') as cache_shelve:
            cache_shelve[repr(lookup_data)] = response.content
        return response.content


def OCLC_data_to_code_and_data_dict(data):
    try:
        xml_dict = xmltodict.parse(data) # Parse to dict
        resp_code = xml_dict['classify']['response']['@code']
        resp_data = xml_dict['classify']
        return int(resp_code), resp_data
    except KeyError:
        return None, None



def OCLC_lookup(lookup_data):
    
    if not lookup_data:
        return None

    OCLC_data = get_OCLC_data(lookup_data)

    resp_code, resp_data = OCLC_data_to_code_and_data_dict(OCLC_data)
    print('Resp code:', resp_code)
    if resp_code is None or resp_code >= 100:
        return None

    if resp_code == 4:
        ''' If multiple, get work identifier and lookup again '''
        wi = resp_data['works']['work']['@wi']
        lookup_data2 = LookupData('wi', wi)
        return OCLC_lookup(lookup_data2)

    if resp_data.get('recommendations'):
        return resp_data



if __name__ == '__main__':
    csv_gen.file_path = 'bnb_records_to_1961.csv'
    csv_gen.field_renames = [('^ALL', csv_gen.snakecase)]

    rewrite_fields = []

    request_count = 0

    for i, book in enumerate(csv_gen.stream()):      


        ## up to 133374
        if i < 133374:
            continue

        request_count += 1
        if request_count % 31 == 0:
            print('Sleeping...')
            time.sleep(5)
            print('------------------------------------------------')
        if request_count % 149 == 0:
            print('Long sleeping...')
            time.sleep(30)
            print('------------------------------------------------')
            
        print('Tackling row', i)

        if not rewrite_fields:
            rewrite_fields = [field for field in book]
            rewrite_fields.append('ddc')
            rewrite_fields.append('OCLC_search_dump')
            with open('To1961.csv', 'a') as output:
                writer = csv.DictWriter(output, fieldnames=rewrite_fields, restval='')
                writer.writeheader()
 
        lookup_data = determine_lookup_data(book)
    
        OCLC_data = OCLC_lookup(lookup_data)
        if OCLC_data:
            try:
                ddc_most_popular = OCLC_data['recommendations']['ddc']['mostPopular']['@nsfa']
            except TypeError:
                try:
                    ddc_most_popular = OCLC_data['recommendations']['ddc']['mostPopular'][0]['@nsfa']
                except:
                    pass
            except KeyError:
                pass
            finally:
                try:
                    print('Result:', ddc_most_popular)
                    book['ddc'] = ddc_most_popular
                    book['OCLC_search_dump'] = repr(dict(OCLC_data['work']))
                except:
                    pass

        with open('To1961.csv', 'a') as output:
            writer = csv.DictWriter(output, fieldnames=rewrite_fields, restval='')
            writer.writerow(book)


        print('------------------------------------------------')




        #print(resp_code, type(resp_code))



