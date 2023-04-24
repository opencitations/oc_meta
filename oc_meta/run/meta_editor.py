#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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

from argparse import ArgumentParser

from rdflib import URIRef

from oc_meta.plugins.editor import MetaEditor

if __name__ == '__main__':
    arg_parser = ArgumentParser('meta_editor.py', description='This script edits OpenCitations Meta triplestore, RDF and provenance')
    arg_parser.add_argument('-c', '--config', dest='config', required=True, help='Configuration file directory')
    arg_parser.add_argument('-op', '--operation', dest='operation', required=True, choices=['update', 'delete', 'sync', 'merge'], help='The CRUD operation to perform')
    arg_parser.add_argument('-s', '--subject', dest='res', required=True, type=URIRef, help='The subject entity')
    arg_parser.add_argument('-p', '--property', dest='property', required=False, help='The property')
    arg_parser.add_argument('-o', '--object', dest='value', required=False, help='The value')
    arg_parser.add_argument('-ot', '--other', dest='other', required=False, help='Other res to be merged with res')
    arg_parser.add_argument('-r', '--resp', dest='resp_agent', required=True, help='Your ORCID')
    args = arg_parser.parse_args()
    meta_editor = MetaEditor(args.config, args.resp_agent)
    if args.operation == 'update':
        meta_editor.update_property(args.res, args.property, args.value)
    elif args.operation == 'delete':
        meta_editor.delete(args.res, args.property)
    elif args.operation == 'sync':
        meta_editor.sync_rdf_with_triplestore(args.res)
    elif args.operation == 'merge':
        meta_editor.merge(URIRef(args.res), URIRef(args.other))