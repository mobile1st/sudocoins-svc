import boto3
import xml.etree.ElementTree as Xml
from datetime import datetime
from util import sudocoins_logger

log = sudocoins_logger.get()

s3 = boto3.resource('s3')
sitemap_bucket_name = 'sitemaps.sudocoins.com'
sitemap_bucket = s3.Bucket(sitemap_bucket_name)
sitemap_max_size = 50000


def upload_file_to_s3(key, body):
    response = s3.meta.client.put_object(
        Bucket=sitemap_bucket_name,
        Key=key,
        Body=body,
        ContentType='application/xml'
    )
    log.debug(f'put_object response: {response}')


class Sitemap(object):
    _url = 'https://app.sudocoins.com/art/social/'
    _now = datetime.now().strftime('%Y-%m-%d')
    _name = None
    _last_modified = None
    _root = None
    _modified = False

    def __init__(self, name, last_modified, root):
        self._name = name
        self._last_modified = last_modified
        self._root = root

    def __len__(self):
        return len(self._root)

    def __str__(self) -> str:
        last_modified = 'NEW' if self.is_new() else self._last_modified
        if self._root is None:
            return f'({last_modified}) {self._name}: NOT LOADED'

        return f'({last_modified}) {self._name}: {self.__len__()} elements'

    @staticmethod
    def from_header(name, last_modified):
        return Sitemap(name, last_modified, None)

    @staticmethod
    def from_art_ids(name, arts):
        if len(arts) > sitemap_max_size:
            raise Exception(f'Sitemap cannot contain more than {sitemap_max_size} URLs. Break into multiple sitemaps.')

        return Sitemap(name, None, Sitemap.generate(arts))

    @staticmethod
    def from_xml_string(name, last_modified, xml_string):
        return Sitemap(name, last_modified, Sitemap.get_root_xml(xml_string))

    @staticmethod
    def get_root_xml(xml_string):
        Xml.register_namespace('', 'http://www.sitemaps.org/schemas/sitemap/0.9')
        return Xml.fromstring(xml_string)

    @staticmethod
    def generate(arts):
        root = Xml.Element('urlset')
        root.attrib['xmlns'] = 'http://www.sitemaps.org/schemas/sitemap/0.9'
        root.attrib['xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
        root.attrib['xsi:schemaLocation'] = ('http://www.sitemaps.org/schemas/sitemap/0.9 '
                                             'http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd')
        for art_id in arts:
            Sitemap.add_art_xml(root, art_id)
        return root

    @classmethod
    def add_art_xml(cls, root, art_id):
        doc = Xml.SubElement(root, 'url')
        Xml.SubElement(doc, 'loc').text = f'{cls._url}{art_id}'
        Xml.SubElement(doc, 'lastmod').text = cls._now
        Xml.SubElement(doc, 'changefreq').text = 'yearly'

    def get_name(self):
        return self._name

    def get_xml_string(self):
        return Xml.tostring(self._root, encoding='unicode', method='xml')

    def get_s3_url(self):
        return f'http://{sitemap_bucket_name}/{self._name}'

    def is_new(self):
        return self._last_modified is None

    def is_modified(self):
        return self._modified

    def load_from_s3(self):
        if self.is_new():
            return self

        sitemap_obj = sitemap_bucket.Object(self._name)
        sitemap_str = sitemap_obj.get()['Body'].read().decode('utf-8')
        self._root = Sitemap.get_root_xml(sitemap_str)
        log.info(f'successfully loaded sitemap: {self}')
        return self

    def write_to_file(self):
        tree = Xml.ElementTree(self._root)
        tree.write(self._name, encoding='utf-8', xml_declaration=True)

    def write_to_s3(self):
        log.info(f'uploading sitemap to s3: {self}')
        upload_file_to_s3(self.get_name(), self.get_xml_string())

    def add(self, arts):
        if not arts:
            return
        actual_size = self.__len__()
        input_size = len(arts)
        if actual_size + input_size > sitemap_max_size:
            raise Exception(f'Sitemap cannot contain more than {sitemap_max_size} URLs. Break into multiple sitemaps.')
        log.info(f'adding {input_size} elements to the existing {actual_size} elements')
        self._modified = True
        for art_id in arts:
            Sitemap.add_art_xml(self._root, art_id)


class Sitemaps(object):
    _version = 'v1'
    _sitemap_index_name = 'sitemaps.xml'
    _sitemap_name_prefix = f'sitemap-{_version}-'
    _sitemap_extension = '.xml'

    def __init__(self):
        sitemaps = sitemap_bucket.objects.filter(Prefix=self._sitemap_name_prefix)
        self._sitemaps = [Sitemap.from_header(obj.key, obj.last_modified)
                          for obj in sorted(sitemaps, key=Sitemaps.get_idx)]

    def __iter__(self):
        return iter(self._sitemaps)

    def __str__(self) -> str:
        return 'Sitemaps: [\n' + ',\n'.join(map(str, self._sitemaps)) + '\n]'

    @staticmethod
    def get_idx(s3_obj):
        return int(s3_obj.key.replace(Sitemaps._sitemap_name_prefix, '').replace(Sitemaps._sitemap_extension, ''))

    def __get_next_sitemap_name(self):
        return f'{self._sitemap_name_prefix}{len(self._sitemaps)}{self._sitemap_extension}'

    def get_s3_url(self):
        return f'http://{sitemap_bucket_name}/{self._sitemap_index_name}'

    def add(self, arts):
        if not self._sitemaps:
            log.debug('there were no sitemaps hence creating new in batches')
            self.__write_in_batches(arts)
            return

        last_sitemap = self._sitemaps[len(self._sitemaps) - 1].load_from_s3()
        last_sitemap_size = len(last_sitemap)

        if sitemap_max_size - last_sitemap_size == 0:
            log.debug('last sitemap is full')
            self.__write_in_batches(arts)
            return

        input_size = len(arts)
        if sitemap_max_size - last_sitemap_size - input_size >= 0:
            log.debug('last sitemap has empty slots and the input fits in')
            last_sitemap.add(arts)
            return

        log.debug('last sitemap has empty slots and the input is more than the empty slots')
        arts_copy = arts.copy()
        available = sitemap_max_size - last_sitemap_size
        last_sitemap.add(arts_copy[:available])
        del arts_copy[:available]
        self.__write_in_batches(arts_copy)

    def __write_in_batches(self, arts):
        for batch in [arts[i:i + sitemap_max_size] for i in range(0, len(arts), sitemap_max_size)]:
            self._sitemaps.append(Sitemap.from_art_ids(self.__get_next_sitemap_name(), batch))

    def write_sitemaps_to_s3(self):
        uploaded = False
        for sitemap in self._sitemaps:
            if sitemap.is_new() or sitemap.is_modified():
                sitemap.write_to_s3()
                uploaded = True
        if uploaded:
            self.__write_sitemap_index_to_s3()
        return uploaded

    def __write_sitemap_index_to_s3(self):
        root = Xml.Element('sitemapindex')
        root.attrib['xmlns'] = 'http://www.sitemaps.org/schemas/sitemap/0.9'
        for sitemap in self._sitemaps:
            sitemap_elem = Xml.SubElement(root, 'sitemap')
            Xml.SubElement(sitemap_elem, 'loc').text = sitemap.get_s3_url()

        xml_string = Xml.tostring(root, encoding='unicode', method='xml')
        log.info(f'uploading sitemap-index to s3')
        upload_file_to_s3(self._sitemap_index_name, xml_string)
