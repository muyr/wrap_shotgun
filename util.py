#!/usr/bin/env python
# -*- coding: utf-8 -*-
###################################################################
# Author: Mu yanru
# Date  : 2018.2
# Email : muyanru345@163.com
###################################################################
__all__ = ['get_shotgun_schema', 'get_shotgun_entity']

from ShotgunObj import MyShotgun


def get_shotgun_schema(table, clean=True):
    my_sg = MyShotgun()
    fields = my_sg.schema_field_read(table)
    if clean:
        result_dict = {}
        for name in fields.keys():
            result_dict[name] = {'field_type': fields[name]['data_type']['value'],
                                 'field_label': fields[name]['name']['value'],
                                 'editable': fields[name]['editable']['value']
                                 }
        return {table: result_dict}
    else:
        return fields


def get_shotgun_entity(visible=True):
    my_sg = MyShotgun()
    data_dict = my_sg.schema_entity_read()
    if visible:
        return {table: table_dict.get('name', {}).get('value')
                for table, table_dict in data_dict.items()
                if table_dict.get('visible', {}).get('value', False) and not table.count('Connection')}


def aaa():
    pass


if __name__ == '__main__':
    import json

    # print json.dumps(get_shotgun_schema('Reply'))
    print get_shotgun_entity()
