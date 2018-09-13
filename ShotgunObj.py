# -*- coding: utf-8 -*-
###################################################################
# Author: Mu yanru
# Date  : 2017.6
# Email : muyanru345@163.com
###################################################################
__all__ = ['MyShotgun']

from shotgun_api3 import Shotgun


class Singleton(type):
    def __init__(cls, *args, **kwargs):
        super(Singleton, cls).__init__(*args, **kwargs)
        cls.__instance = None

    def __call__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(Singleton, cls).__call__(*args, **kwargs)
            return cls.__instance
        else:
            return cls.__instance


class ShotgunObj(Shotgun):
    __metaclass__ = Singleton

    def __init__(self, base_url, script_name, api_key):
        super(ShotgunObj, self).__init__(base_url,
                                         script_name=script_name,
                                         api_key=api_key)

    def find_one_by_one(self, entity, filter_list, field_list):
        index = 1
        summarize_data_dict = self.summarize(entity_type=entity,
                                             filters=filter_list,
                                             summary_fields=[{'field': 'id', 'type': 'count'}, ])
        count = summarize_data_dict['summaries']['id']
        while True:
            result = self.find(entity, filter_list, field_list, limit=1, page=index)
            if result:
                yield result[0], index, count
                index += 1
            else:
                break


class MyShotgun(ShotgunObj):
    def __init__(self):
        import config
        super(MyShotgun, self).__init__(config.URL, config.SCRIPT, config.KEY)


if __name__ == '__main__':
    sg = MyShotgun()
    print sg.find('Project', [['name', 'is', 'dayu-demo']])
