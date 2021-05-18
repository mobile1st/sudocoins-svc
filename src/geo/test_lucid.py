import json
import unittest
from lucid import Lucid


class TestLucidLocales(unittest.TestCase):
    def setUp(self):
        self.lucidCodes = {}
        with open("clc.json", encoding='utf-8') as clc_json:
            clc = json.load(clc_json)
            for region, languages in clc.items():
                for language, lucidCode in languages.items():
                    self.lucidCodes[int(lucidCode)] = (region, language)

        with open("regions.json", encoding='utf-8') as regions_json:
            region_data = json.load(regions_json)
            self.languages = region_data['languages']
            self.regions = region_data['regions']
            self.continents = region_data['continents']
            self.lucidLocales = region_data['lucidLocales']

    def test_lucid_regions_have_name(self):
        for lRegion in self.lucidLocales.keys():
            region_def = self.regions.get(lRegion)
            self.assertIsNotNone(region_def, "missing region definition for lucid region=" + lRegion)

    def test_lucid_languages_have_name(self):
        for region, lLangList in self.lucidLocales.items():
            for lang in lLangList:
                lang_def = self.languages.get(lang)
                self.assertIsNotNone(lang_def, "missing language definition for region=" + region + " language=" + lang)

    def test_lucid_region_codes(self):
        for region, lLangList in self.lucidLocales.items():
            for lang in lLangList:
                code = Lucid.region_code(region, lang)
                clc = self.lucidCodes.get(code)
                print(region, lang, code, clc[0], self.regions[region], clc[1], self.languages[lang])
                self.assertIsNotNone(clc, 'no clc mapping for ' + str(code))

    def test_regions_have_continent(self):
        region_continent = {}
        for continent in self.continents:
            for region in continent['regions']:
                region_continent[region] = continent['name']

        for region in self.regions.keys():
            self.assertIsNotNone(region_continent[region], "missing continent for region=" + region)
