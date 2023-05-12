#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


from __future__ import annotations

import re

from oc_meta.plugins.ra_processor import RaProcessor


class JalcProcessing(RaProcessor):
    def __init__(self, citing_entities:str=None, orcid_index:str=None, doi_csv:str=None, publishers_filepath: str = None):
        super(JalcProcessing, self).__init__(citing_entities, orcid_index, doi_csv, publishers_filepath)

    def csv_creator(self, item:dict) -> dict:
        doi = item["doi"]
        doi_without_prefix = re.sub('^doi:', '', doi)
        if (doi and self.doi_set and doi_without_prefix in self.doi_set) or (doi and not self.doi_set):
            title = self.get_ja(item['title_list'])[0]['title'] if 'title_list' in item else ''  # Future Water Availability in the Asian Monsoon Region: A Case Study in Indonesia (no available in japanese)
            authors_list = self.get_authors(item)
            authors_string_list, editors_string_list = self.get_agents_strings_list(doi_without_prefix, authors_list)
            issue = item['issue'] if 'issue' in item else ''
            volume = item['volume'] if 'volume' in item else ''

            metadata = {
                'id': doi,
                'title': title,
                'author': '; '.join(authors_string_list),
                'issue': issue,
                'volume': volume,
                'venue': self.get_venue(item),
                'pub_date': self.get_pub_date(item),
                'page': self.get_jalc_pages(item),
                'type': self.get_type(item),
                'publisher': self.get_publisher_name(item),
                'editor': ''
            }
            return self.normalise_unicode(metadata)
        
    @classmethod
    def get_ja(cls, field: list) -> list:  # [{'publisher_name': '筑波大学農林技術センター', 'lang': 'ja'}]
        if all('lang' in item for item in field):
            ja = [item for item in field if item['lang'] == 'ja']
            ja = list(filter(lambda x: x['type'] != 'before' if 'type' in x else x, ja))
            if ja:
                return ja
            en = [item for item in field if item['lang'] == 'en']
            en = list(filter(lambda x: x['type'] != 'before' if 'type' in x else x, en))
            if en:
                return en
        return field

    def get_jalc_pages(self, item: dict) -> str:
        first_page = item['first_page'] if 'first_page' in item else ''
        last_page = item['last_page'] if 'last_page' in item else ''
        page_list = list()
        if first_page:
            page_list.append(first_page)
        if last_page:
            page_list.append(last_page)
        return self.get_pages(page_list)


    def get_publisher_name(self, item: dict) -> str:
        '''
        This function aims to return a publisher's name. If a mapping was provided,
        it is used to find the publisher's standardized name from its id or DOI prefix.

        :params doi: the item's DOI
        :type doi: str
        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string with the name of the publisher, for example, 'American Medical Association (AMA)'. Finally, if there is no publisher, the output is an empty string.
        '''
        publisher = self.get_ja(item['publisher_list'])[0]['publisher_name'] if 'publisher_list' in item else ''
        data = {
            'publisher': publisher,
            'prefix': item.get('prefix')
        }
        publisher = data['publisher']
        prefix = data['prefix']
        if self.publishers_mapping:
            member_dict = next(
                ({member: data} for member, data in self.publishers_mapping.items() if prefix in data['prefixes']),
                None)
            if member_dict:
                member = list(member_dict.keys())[0]
                name = f"{member_dict[member]['name']}"
            else:
                name = publisher
        else:
            name = publisher

        return name

    def get_authors(self, data: dict) -> list:
        authors = list()
        if data.get("creator_list"):
            creators = data.get("creator_list")
            for c in creators:
                agent = {"role": "author"}
                names = c['names'] if 'names' in c else ''
                ja_name = self.get_ja(names)[0]
                last_name = ja_name['last_name'] if 'last_name' in ja_name else ''
                first_name = ja_name['first_name'] if 'first_name' in ja_name else ''
                full_name = ''
                if last_name:
                    full_name += last_name
                    if first_name:
                        full_name += f', {first_name}'
                agent["name"] = full_name
                agent["family"] = last_name
                agent["given"] = first_name
                authors.append(agent)
        return authors

    def get_venue(self, data: dict) -> str:
        venue_name = ''
        if 'journal_title_name_list' in data:
            candidate_venues = self.get_ja(data['journal_title_name_list'])
            if candidate_venues:
                full_venue = [item for item in candidate_venues if 'type' in item if item['type'] == 'full']
                if full_venue:
                    venue_name = full_venue[0]['journal_title_name']
                elif candidate_venues:
                    venue_name = candidate_venues[0]['journal_title_name']
        if 'journal_id_list' in data:
            journal_ids = [journal_id for journal_id in data['journal_id_list']]
            journal_ids = sorted(journal_ids)
        else:
            journal_ids = list()
        return f"{venue_name} [{' '.join(journal_ids)}]" if journal_ids else venue_name
        # 'Journal of Developments in Sustainable Agriculture [issn:1880-3016 issn:1880-3024 jid:jdsa]'

    @classmethod
    def get_type(cls, data:dict) -> str:
        if data.get('content_type'):
            content_type = data['content_type']
            if content_type == 'JA':
                br_type = 'journal article'
            elif content_type == 'BK':
                br_type = 'book'
            elif content_type == 'RD':
                br_type = 'dataset'
            elif content_type == 'EL':
                br_type = 'other'
            elif content_type == 'GD':
                br_type = 'other'
            return br_type
        else:
            return ''
    
    @classmethod
    def get_pub_date(cls, data) -> str:
        pub_date_dict = data['publication_date'] if 'publication_date' in data else ''
        pub_date_list = list()
        year = pub_date_dict['publication_year'] if 'publication_year' in pub_date_dict else ''
        if year:
            pub_date_list.append(year)
            month = pub_date_dict['publication_month'] if 'publication_month' in pub_date_dict else ''
            if month:
                pub_date_list.append(month)
                day = pub_date_dict['publication_day'] if 'publication_day' in pub_date_dict else ''
                if day:
                    pub_date_list.append(day)
        return '-'.join(pub_date_list)
