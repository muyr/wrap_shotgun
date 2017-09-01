# -*- coding: utf-8 -*-
###################################################################
# Author: Mu yanru
# Date  : 2017.6
# Email : muyanru345@163.com
###################################################################


from shotgun_api3 import Shotgun
import ConfigParser
import sys
import os


class Singleton(type):
    def __init__(self, *args, **kwargs):
        super(Singleton, self).__init__(*args, **kwargs)
        self.__instance = None

    def __call__(self, *args, **kwargs):
        if self.__instance is None:
            self.__instance = super(Singleton, self).__call__(*args, **kwargs)
            return self.__instance
        else:
            return self.__instance


class ShotgunObj(Shotgun):
    __metaclass__ = Singleton

    def __init__(self, base_url, script_name, api_key):
        super(ShotgunObj, self).__init__(base_url,
                                         script_name=script_name,
                                         api_key=api_key)

    def find_one_by_one(self, entity, filterList, fields):
        index = 1
        summarizeDataDict = self.summarize(entity_type=entity,
                                           filters=filterList,
                                           summary_fields=[{'field': 'id', 'type': 'count'}, ])
        count = summarizeDataDict['summaries']['id']
        while True:
            result = self.find(entity, filterList, fields, limit=1, page=index)
            if result:
                yield result[0], index, count
                index += 1
            else:
                break


class MyShotgun(ShotgunObj):
    import pkgutil, json
    data = pkgutil.get_data('wrap_shotgun', 'SG_CONFIG.json')
    settingDict = json.loads(data)
    url = settingDict.get('shotgun').get('url')
    script = settingDict.get('shotgun').get('script')
    key = settingDict.get('shotgun').get('key')

    def __init__(self):
        super(MyShotgun, self).__init__(self.url, self.script, self.key)


if __name__ == '__main__':
    my_sg = MyShotgun()
