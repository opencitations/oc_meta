import unittest

from oc_meta.plugins.pubmed.get_publishers import ExtractPublisherDOI


class MyTestCase(unittest.TestCase):

    def test_get_pub_API_crossref(self):
        e_p_doi = ExtractPublisherDOI({})
        pub_1016 = e_p_doi.extract_publishers_v("10.1016/0005-2728(75)90129-2")[0]
        pub_1136 = e_p_doi.extract_publishers_v("10.1136/bmj.4.5994.440")[0]
        pub_1021 = e_p_doi.extract_publishers_v("10.1021/bi00695a008")[0]
        pub_1007 = e_p_doi.extract_publishers_v("10.1007/bmj.4.5994.440")[0]
        pub_1097 = e_p_doi.extract_publishers_v("10.1097/00003246-197507000-00003")[0]
        pub_1378 = e_p_doi.extract_publishers_v("10.1378/chest.68.6.814")[0]
        pub_1055 = e_p_doi.extract_publishers_v("10.1055/s-0028-1106478")[0]
        pub_1080 = e_p_doi.extract_publishers_v("10.1080/0743580750908416")[0]
        pub_1210 = e_p_doi.extract_publishers_v("10.1210/endo-97-4-855")[0]
        pub_1159 = e_p_doi.extract_publishers_v("10.1159/000458949")[0]


        self.assertEqual(pub_1016, "Elsevier BV")
        self.assertEqual(pub_1136, "BMJ")
        self.assertEqual(pub_1021, "American Chemical Society (ACS)")
        self.assertEqual(pub_1007, "Springer Science and Business Media LLC")
        self.assertEqual(pub_1097, "Ovid Technologies (Wolters Kluwer Health)")
        self.assertEqual(pub_1378, "Elsevier BV")
        self.assertEqual(pub_1055, "Georg Thieme Verlag KG")
        self.assertEqual(pub_1080, "Informa UK Limited")
        self.assertEqual(pub_1210, "The Endocrine Society")
        self.assertEqual(pub_1159, "S. Karger AG")

    def test_get_pref_info_from_dict(self):
        dataicte_doi = "10.14454/FXWS-0523"
        crossref_doi = "10.1021/bi00695a008"
        made_up_dict_crossref = {'10.1021': {'name': 'A Fake Publisher', 'crossref_member': '239', 'from': 'Crossref'}}
        made_up_dict_external = {'10.14454': {'name': 'A Fake Datacite Publisher', 'from': 'datacite'}}

        e_p_doi_1 = ExtractPublisherDOI(made_up_dict_crossref)
        e_p_doi_2 = ExtractPublisherDOI(made_up_dict_external)

        pub_d = e_p_doi_2.extract_publishers_v(dataicte_doi)[0]
        pub_c = e_p_doi_1.extract_publishers_v(crossref_doi)[0]
        self.assertEqual(pub_d, "A Fake Datacite Publisher")
        self.assertEqual(pub_c, "A Fake Publisher")

    def test_get_pub_API_datacite(self):
        e_p_doi_3 = ExtractPublisherDOI({})
        dataicte_doi = "10.14454/FXWS-0523"
        datacite_doi2 = "10.15468/dl.7xagnb"
        pub_d = e_p_doi_3.extract_publishers_v(dataicte_doi)[0]
        pub_d2 = e_p_doi_3.extract_publishers_v(datacite_doi2)[0]
        self.assertEqual(pub_d, "DataCite")
        self.assertEqual(pub_d2, "The Global Biodiversity Information Facility")

    def test_get_pub_API_medra(self):
        e_p_doi_4 = ExtractPublisherDOI({})
        medra_doi = "10.3233/ds-210053"
        medra_doi_no_pub = "10.48255/9788891322968"
        pub_m = e_p_doi_4.extract_publishers_v(medra_doi)[0]
        pub_m2 = e_p_doi_4.extract_publishers_v(medra_doi_no_pub)[0]
        self.assertEqual(pub_m, "IOS Press")
        self.assertEqual(pub_m2, "unidentified")

    def test_get_pub_cnki(self):
        e_p_doi_5 = ExtractPublisherDOI({})
        cnki_doi = "10.13336/j.1003-6520.hve.20160308018"
        pub_c = e_p_doi_5.extract_publishers_v(cnki_doi)[0]
        expected = "CNKI Publisher (unspecified)"
        self.assertEqual(pub_c, expected)










if __name__ == '__main__':
    unittest.main()
