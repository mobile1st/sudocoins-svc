import json
import random
from datetime import datetime
from util.sudocoins_encoder import SudocoinsEncoder


class ArtDocument(object):
    def __init__(self):
        self._art_id = None
        self._name = None
        self._description = None
        self._labels = None
        self._tags = None
        self._price = None

    def art_id(self, art_id):
        self._art_id = art_id
        return self

    def name(self, name):
        self._name = name
        return self

    def description(self, description):
        self._description = description
        return self

    def rekognition_labels(self, labels):
        self._labels = set(labels) if labels else set()
        return self

    def user_provided_tags(self, tags):
        self._tags = set(tags) if tags else set()
        return self

    def price(self, price):
        self._price = price
        return self

    def __str__(self):
        return f'art_id: {self._art_id}, name: {self._name}, desc: {self._description}, labels: {self._labels}, tags: {self._tags}'

    def _get_blob(self):
        fields = {
            'art_id': self._art_id,
            'category': 'art'
        }
        if self._name:
            fields['name'] = self._name
        if self._description:
            fields['description'] = self._description
        if self._labels:
            fields['labels'] = self._labels
        if self._tags:
            fields['tags'] = self._tags
        return fields

    def to_kendra_doc(self, data_source_id, job_execution_id):
        blob = self._get_blob()
        doc = {
            'Id': self._art_id,
            'Blob': json.dumps(blob, cls=SudocoinsEncoder),
            'ContentType': 'PLAIN_TEXT',
            'Attributes': [
                {
                    'Key': '_data_source_id',
                    'Value': {
                        'StringValue': data_source_id
                    }
                },
                {
                    'Key': '_data_source_sync_job_execution_id',
                    'Value': {
                        'StringValue': job_execution_id
                    }
                },
                {
                    'Key': '_created_at',
                    'Value': {
                        'DateValue': datetime.now().isoformat()
                    }
                }
            ]
        }
        if self._name:
            doc['Title'] = self._name
        tags_and_labels = self._labels.union(self._tags)
        if tags_and_labels:
            doc['Attributes'].append({
                'Key': 'labels',
                'Value': {
                    'StringListValue': list(tags_and_labels)
                }
            })
        doc['Attributes'].append({
            'Key': 'price',
            'Value': {
                'LongValue': random.randint(0, 10000)
            }
        })

        return doc
