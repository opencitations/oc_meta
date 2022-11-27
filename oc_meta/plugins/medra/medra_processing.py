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

from datetime import datetime
from typing import List, Tuple

from bs4 import BeautifulSoup
from oc_idmanager import DOIManager, ORCIDManager

from oc_meta.lib.csvmanager import CSVManager
from oc_meta.plugins.ra_processor import RaProcessor


class MedraProcessing(RaProcessor):
    def __init__(self, orcid_index:str=None, doi_csv:str=None):
        self.doi_set = CSVManager.load_csv_column_as_set(doi_csv, 'id') if doi_csv else None
        orcid_index = orcid_index if orcid_index else None
        self.orcid_index = CSVManager(orcid_index)
        self._om = ORCIDManager()
    
    def csv_creator(self, xml_soup:BeautifulSoup) -> dict:
        try:
            br_type = self.get_br_type(xml_soup)
        except UnboundLocalError:
            print(xml_soup)
            raise(UnboundLocalError)
        metadata: dict = getattr(self, f"extract_from_{br_type.replace(' ', '_')}")(xml_soup)
        return self.normalise_unicode(metadata)
    
    def extract_from_book(self, xml_soup:BeautifulSoup) -> dict:
        ids = [self.get_id(xml_soup)]
        ids.extend(self.get_isbn(xml_soup))
        title = xml_soup.find('Title').find('TitleText').get_text()
        authors, editors = self.get_contributors(xml_soup)
        return {
            'id': ' '.join(ids),
            'title': title,
            'author': '; '.join(authors),
            'issue': '',
            'volume': '',
            'venue': '',
            'pub_date': self.get_pub_date(xml_soup),
            'pages': '',
            'type': 'book',
            'publisher': self.get_publisher(xml_soup),
            'editor': '; '.join(editors)
        }

    def extract_from_book_chapter(self, xml_soup:BeautifulSoup) -> dict:
        venue_ids = self.get_isbn(xml_soup)
        monographic_work = xml_soup.find('MonographicWork')
        content_item = xml_soup.find('ContentItem')
        title_chapter = content_item.find('Title').find('TitleText').get_text()
        title_book = monographic_work.find('Title').find('TitleText').get_text()
        authors, editors = self.get_contributors(content_item)
        venue_name = title_book if title_book != title_chapter else ''
        return {
            'id': self.get_id(xml_soup),
            'title': title_chapter,
            'author': '; '.join(authors),
            'issue': '',
            'volume': '',
            'venue': self.build_venue_string(venue_name, venue_ids),
            'pub_date': self.get_pub_date(xml_soup),
            'pages': '',
            'type': 'book chapter',
            'publisher': self.get_publisher(xml_soup),
            'editor': '; '.join(editors)
        }

    def extract_from_journal_article(self, xml_soup:BeautifulSoup) -> dict:
        serial_publication = xml_soup.find('SerialPublication')
        serial_work = serial_publication.find('SerialWork')
        publisher_name = self.get_publisher(serial_work)
        journal_issue = xml_soup.find('JournalIssue')
        volume = journal_issue.find('JournalVolumeNumber')
        volume = volume.get_text() if volume else ''
        issue = journal_issue.find('JournalIssueNumber')
        issue = issue.get_text() if issue else ''
        content_item = xml_soup.find('ContentItem')
        title = content_item.find('Title').find('TitleText').get_text()
        authors, editors = self.get_contributors(xml_soup)
        venue_name, venue_ids = self.get_venue(xml_soup)
        return {
            'id': self.get_id(xml_soup),
            'title': title,
            'author': '; '.join(authors),
            'issue': issue,
            'volume': volume,
            'venue': self.build_venue_string(venue_name, venue_ids),
            'pub_date': self.get_pub_date(content_item),
            'pages': self.get_pages(content_item),
            'type': 'journal article',
            'publisher': publisher_name,
            'editor': '; '.join(editors)
        }
    
    def build_venue_string(self, venue_name, venue_ids):
        if venue_name and venue_ids:
            venue = f"{venue_name} [{' '.join(venue_ids)}]"
        elif venue_name and not venue_ids:
            venue = venue_name
        elif venue_ids and not venue_name:
            venue = f"[{' '.join(venue_ids)}]"
        elif not venue_ids and not venue_name:
            venue = ''
        return venue

    def extract_from_series(self, xml_soup:BeautifulSoup) -> dict:
        venue_name, venue_ids = self.get_venue(xml_soup)
        ids = [self.get_id(xml_soup)]
        ids.extend(venue_ids)
        return {
            'id': ' '.join(ids),
            'title': venue_name,
            'author': '',
            'issue': '',
            'volume': '',
            'venue': '',
            'pub_date': self.get_pub_date(xml_soup),
            'pages': '',
            'type': 'series',
            'publisher': self.get_publisher(xml_soup.find('SerialPublication').find('SerialWork')),
            'editor': ''
        }
    
    def get_id(self, context:BeautifulSoup) -> str:
        doi_manager = DOIManager(use_api_service=False)
        return doi_manager.normalise(context.find('DOI').get_text(), include_prefix=True)
    
    def get_isbn(self, context:BeautifulSoup) -> str:
        product_identifiers: List[BeautifulSoup] = context.findAll('ProductIdentifier')
        isbn_list = list()
        if product_identifiers:
            for product_identifier in product_identifiers:
                if product_identifier.find('ProductIDType').get_text() in {'02', '15'}:
                    self.isbn_worker(product_identifier.find('IDValue').get_text(), isbn_list)
        return isbn_list
    
    def get_contributors(self, context:BeautifulSoup) -> Tuple[list, list]:
        contributors:List[BeautifulSoup] = context.findAll('Contributor')
        authors = list(); editors = list()
        contributor_roles = {'A': authors, 'B': editors}
        for i, contributor in enumerate(contributors):
            contributor_role = contributor.find('ContributorRole').get_text()[0]
            person_name_inverted = contributor.find('PersonNameInverted')
            corporate_name = contributor.find('CorporateName')
            person_name = contributor.find('PersonName')
            names_before_key = contributor.find('NamesBeforeKey')
            key_names = contributor.find('KeyNames')
            unnamed_persons = contributor.find('UnnamedPersons')
            if person_name_inverted:
                author = person_name_inverted.get_text()
            elif names_before_key and key_names:
                author = f'{key_names.get_text()}, {names_before_key.get_text()}'
            elif key_names and not names_before_key:
                author = f'{key_names.get_text()},'
            elif corporate_name:
                author = corporate_name.get_text()
            elif person_name:
                author = person_name.get_text()
            elif unnamed_persons:
                continue
            else:
                raise(ValueError('No author name'))
            is_there_name_id = contributor.find('NameIdentifier')
            sequence_number = contributor.find('SequenceNumber')
            sequence_number = int(sequence_number.get_text()) if sequence_number else i
            if is_there_name_id:
                name_id = self._om.normalise(is_there_name_id.find('IDValue').get_text(), include_prefix=True)
                author += f' [{name_id}]'
            contributor_roles[contributor_role].append((sequence_number, author))
        contributor_roles = {k:[ra[1] for ra in sorted(v, key=lambda x:x[0])] for k,v in contributor_roles.items()}
        return contributor_roles['A'], contributor_roles['B']
    
    def get_pub_date(self, context:BeautifulSoup) -> str:
        raw_date = context.find('PublicationDate')
        if not raw_date:
            raw_date = context.find('Date')
        if not raw_date:
            return ''
        raw_date = raw_date.get_text()
        try:
            clean_date = datetime.strptime(raw_date, '%Y%m%d').strftime('%Y-%m-%d')
        except ValueError:
            try:
                clean_date = datetime.strptime(raw_date, '%Y%m').strftime('%Y-%m')
            except ValueError:
                clean_date = datetime.strptime(raw_date, '%Y').strftime('%Y')
        return clean_date

    def get_pages(self, context:BeautifulSoup) -> str:
        page_run = context.find('PageRun')
        if page_run:
            starting_page = page_run.find('FirstPageNumber')
            ending_page = page_run.find('LastPageNumber')
            starting_page = starting_page.get_text() if starting_page else None
            ending_page = ending_page.get_text() if ending_page else None
            if starting_page and '-' in starting_page:
                starting_page = f'"{starting_page}"'
            if ending_page and '-' in ending_page:
                ending_page = f'"{ending_page}"'
            pages = f'{starting_page}-{ending_page}' if starting_page and ending_page else starting_page if starting_page else ''
        else:
            pages = ''
        return pages
    
    def get_publisher(self, context:BeautifulSoup) -> str:
        publisher = context.find('Publisher')
        if publisher:
            publisher_name = publisher.find('PublisherName')
            if publisher:
                return publisher_name.get_text()
        return ''
    
    def get_venue(self, context:BeautifulSoup) -> str:
        serial_publication = context.find('SerialPublication')
        serial_work = serial_publication.find('SerialWork')
        serial_work_titles:List[BeautifulSoup] = serial_work.findAll('Title')
        venue_name = None
        for serial_work_title in serial_work_titles:
            if serial_work_title.find('TitleType').get_text() == '01':
                venue_name = serial_work_title.find('TitleText').get_text()
            elif serial_work_title.find('TitleType').get_text() == '05':
                venue_name = serial_work_title.find('TitleText').get_text()
        serial_versions:List[BeautifulSoup] = serial_publication.findAll('SerialVersion')
        venue_ids = list()
        for serial_version in serial_versions:
            product_id_type = serial_version.find('ProductIDType')
            if serial_version.find('ProductForm').get_text() in {'JD', 'JB'} and product_id_type:
                if product_id_type.get_text() == '07':
                    id_value = serial_version.find('IDValue').get_text()
                    id_value = id_value + 'X' if len(id_value) == 7 else id_value
                    self.issn_worker(id_value, venue_ids)
        return venue_name, venue_ids
    
    @classmethod
    def get_br_type(cls, xml_soup:BeautifulSoup) -> str:
        if xml_soup.find('DOIMonographicProduct') or xml_soup.find('DOIMonographicWork'):
            br_type = 'book'
        elif xml_soup.find('DOIMonographChapterWork') or xml_soup.find('DOIMonographChapterVersion'):
            br_type = 'book chapter'
        elif xml_soup.find('DOISerialArticleWork') or xml_soup.find('DOISerialArticleVersion'):
            br_type = 'journal article'
        elif xml_soup.find('DOISerialIssueWork'):
            br_type = 'journal issue'
        elif xml_soup.find('DOISerialTitleWork'):
            br_type = 'series'
        return br_type