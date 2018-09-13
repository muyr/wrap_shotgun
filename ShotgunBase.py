# -*- coding: utf-8 -*-
###################################################################
# Author: Mu yanru
# Date  : 2016.11
# Email : muyanru345@163.com
###################################################################
__all__ = ['Asset', 'Attachment', 'Department', 'Group', 'HumanUser', 'LocalStorage',
           'MyCustomEntity', 'Note', 'Project',
           'Reply', 'Sequence', 'Shot', 'Status', 'Step', 'Task', 'TimeLog', 'Version', 'any_']

import json
import pkgutil
from ShotgunObj import MyShotgun


def any_(*args):
    return {'filter_operator': 'any', 'filters': list(args)}


def convert_to_sg(value):
    if len(value) == 1:
        real_value = value[0]
        if isinstance(real_value, SGEntityBase):
            return real_value.to_shotgun_entity()
        elif isinstance(real_value, list):
            return [i.to_shotgun_entity() if isinstance(i, SGEntityBase) else i for i in real_value]
        else:
            return real_value
    else:
        return value


OP_MAP = {
    'addressing': {'operator': 'is is_not contains not_contains in type_is type_is_not name_contains '
                               'name_not_contains name_starts_with name_ends_with',
                   'group': ''},
    'checkbox': {'operator': 'is is_not',
                 'group': ''},
    'currency': {'operator': 'is is_not less_than greater_than between not_between in not_in',
                 'group': ''},
    'date': {'operator': 'is is_not greater_than less_than in_last not_in_last in_next not_in_next '
                         'in_calendar_day in_calendar_week in_calendar_month in_calendar_year between in not_in',
             'group': 'exact day week month quarter year clustered_date',
             'summary': 'record_count count earliest latest'},
    'date_time': {'operator': 'is is_not greater_than less_than in_last not_in_last in_next not_in_next '
                              'in_calendar_day in_calendar_week in_calendar_month in_calendar_year between in not_in',
                  'group': 'exact day week month quarter year clustered_date',
                  'summary': 'record_count count earliest latest'},
    'duration': {'operator': 'is is_not greater_than less_than between in not_in',
                 'group': '',
                 'summary': 'record_count count sum minimum maximum average]'},
    'entity': {'operator': 'is is_not type_is type_is_not name_contains name_not_contains name_is in not_in',
               'group': 'exact entitytype firstletter',
               'summary': 'record_count count'},
    'float': {'operator': 'is is_not greater_than less_than between in not_in',
              'group': ''},
    'image': {'operator': 'is is_not',
              'group': ''},
    'list': {'operator': 'is is_not in not_in',
             'group': ''},
    'multi_entity': {'operator': 'is is_not type_is type_is_not name_contains name_not_contains in not_in',
                     'group': ''},
    'number': {'operator': 'is is_not less_than greater_than between not_between in not_in',
               'group': '',
               'summary': 'record_count count sum maximum minimum average'},
    'password': {'operator': '',
                 'group': ''},
    'percent': {'operator': 'is is_not greater_than less_than between in not_in',
                'group': ''},
    'serializable': {'operator': '',
                     'group': ''},
    'status_list': {'operator': 'is is_not in not_in',
                    'group': 'exact',
                    'summary': 'record_count status_list status_percentage'},
    'summary': {'operator': '',
                'group': ''},
    'tag_list': {'operator': 'is is_not name_contains name_not_contains name_id',
                 'group': ''},
    'text': {'operator': 'is is_not contains not_contains starts_with ends_with in not_in',
             'group': 'exact firstletter',
             'summary': 'record_count count'},
    'timecode': {'operator': 'is is_not greater_than less_than between in not_in',
                 'group': ''},
    'url': {'operator': '',
            'group': ''},
}


class SGField(object):
    def __init__(self, field_code, field_label, field_type):
        self.field_code = field_code
        self.field_label = field_label
        self.field_type = field_type
        self.cache_data = None

    def __get__(self, instance, owner):
        if instance is None:
            # 通过宿主类直接访问, 返回描述器本身（一般是query时作为参数 Project.name）
            if hasattr(owner, 'chain'):
                setattr(self, 'chain', getattr(owner, 'chain'))
            return self

        if (self.field_code not in instance.cache_data.keys()) and instance.id:
            # 该 field 不存在，查询 shotgun，并将其添加到实例中的cache_data，然后返回实例中的cache_data
            sg_fields = instance.sg_fields
            result_fields = []
            # 当访问的field未查询过时
            if sg_fields[self.field_code].get('later', False):
                # 如果该 field 是later读取的，那么只查询该 field
                result_fields.append(self.field_code)
            else:
                # 如果该 field 是积极读取的，那么把该 Entity 所有积极读取的属性都查询
                result_fields = [sg_fields[k].get('inner_code', k) for k in sg_fields.keys() if
                                 not sg_fields[k].get('later', False)]
            my_sg = MyShotgun()
            print 'find_one'
            result = my_sg.find_one(instance.sg_table, [['id', 'is', instance.id]], result_fields)
            if result is None:
                raise Exception('Shotgun doesn\'t have this id[%d] %s!' % (instance.id, owner.sg_table))
            instance.fill_data(result)
        self.cache_data = instance.cache_data.get(self.field_code)
        return self.cache_data

    def __set__(self, instance, value):
        self.cache_data = convert_to_sg(value)
        instance.cache_data.update({self.field_code: self.cache_data})

    def __repr__(self):
        return "{}<{}>".format(self.__class__.__name__, self.field_label)

    def __getattr__(self, item):
        if item.endswith('_'):
            item = item[:-1]
        if item in OP_MAP.get(self.field_type).get('operator', '').split():
            if hasattr(self, 'chain'):
                return lambda *value: ['{}.{}'.format(getattr(self, 'chain', ''), self.field_code), item,
                                       convert_to_sg(value)]
            else:
                return lambda *value: [self.field_code, item, convert_to_sg(value)]
        else:
            raise Exception('{}({}) has no filter operator <{}>'.format(self.field_code, self.field_type, item))


class FileField(SGField):
    _cached_data_dict = {}
    sg_fields_List = ['content_type', 'mime-type', 'link_type', 'name',
                      'local_path', 'local_path_windows', 'local_path_linux', 'local_path_mac', 'local_path_windows',
                      'local_storage', 'url', 'id', 'type']

    def __set__(self, instance, value):
        super(FileField, self).__set__(instance, value)
        for k in self.sg_fields_List:
            setattr(self, k, value.get(k, None))

    def __repr__(self):
        return "File Field{name:'%s', url:'%s'}" % (self.name, self.url)

    def download(self, base_dir=None):
        if self._cached_data_dict.get('link_type', 'upload') == 'local':
            return self._cached_data_dict['local_path']
        if not base_dir:
            import os
            base_dir = os.environ['TMP']
        sg = MyShotgun()
        return sg.download_attachment(attachment=self._cached_data,
                                      file_path='%s/%s' % (base_dir, self._cached_data.get('name')))


class ProxyEntity(SGField):
    _cached_data_dict = {}
    _cached_data = None

    def __get__(self, instance, owner):
        data_dict = super(ProxyEntity, self).__get__(instance, owner)

        if data_dict is self:
            return data_dict

        if not data_dict:
            return data_dict

        key_name = '{0}_{1}_{2}'.format(instance.sg_table, instance.id, self.field_code)
        if key_name not in self._cached_data_dict.keys():
            self._cached_data_dict.update({key_name: SGEntityBase.from_shotgun_entity(data_dict)})
        return self._cached_data_dict.get(key_name)

    def __getattr__(self, item):
        try:
            return super(ProxyEntity, self).__getattr__(item)
        except Exception as e:
            if item in globals().keys():
                if hasattr(self, 'chain'):
                    return type(item, (globals()[item],),
                                {'chain': '{}.{}.{}'.format(getattr(self, 'chain'), self.field_code, item)})
                else:
                    return type(item, (globals()[item],), {'chain': '{}.{}'.format(self.field_code, item)})
            else:
                raise e


class MultiProxyEntity(SGField):
    _cached_data_dict = {}

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value_list = super(MultiProxyEntity, self).__get__(instance, owner)
        if not value_list:
            return value_list
        key_name = '{0}_{1}_{2}'.format(instance.sg_table, instance.id, self.field_code)
        if key_name not in self._cached_data_dict.keys():
            self._cached_data_dict.update({key_name: [SGEntityBase.from_shotgun_entity(value) for value in value_list]})
        return self._cached_data_dict.get(key_name)


class SGMetaClass(type):
    def __new__(mcs, name, bases, clsdict):
        if bases[0] is object:
            return super(SGMetaClass, mcs).__new__(mcs, name, bases, clsdict)
        data_dict = json.loads(pkgutil.get_data('sg_schema', '{}.json'.format(name)))
        sg_table = data_dict.keys()[0]
        fields = data_dict[sg_table]
        clsdict.update({'sg_fields': fields, 'type': name, 'sg_table': sg_table})
        for field_code, field_dict in fields.items():
            field_label = field_dict['field_label']
            field_type = field_dict['field_type']
            if field_type == 'entity':
                clsdict.update({field_code: ProxyEntity(field_code, field_label, field_type)})
            elif field_type == 'multi_entity':
                clsdict.update({field_code: MultiProxyEntity(field_code, field_label, field_type)})
            elif field_type == 'url':
                clsdict.update({field_code: FileField(field_code, field_label, field_type)})
            else:
                clsdict.update({field_code: SGField(field_code, field_label, field_type)})
        return super(SGMetaClass, mcs).__new__(mcs, name, bases, clsdict)


class SGEntityBase(object):
    __metaclass__ = SGMetaClass
    sg_table = None
    _repr_field = 'code'

    def __init__(self):
        self.cache_data = {}

    def fill_data(self, data_dict):
        self.cache_data.update({self._repr_field: data_dict.get('name')})
        self.cache_data.update(data_dict)
        for attr, attr_setting in self.sg_fields.items():
            if 'inner_code' in attr_setting.keys() and attr_setting.get('inner_code') in data_dict.keys():
                self.cache_data.update({attr: data_dict[attr_setting.get('inner_code')]})

    def __repr__(self):
        return "%s{id:%s, name:'%s'}" % (
            self.__class__.__name__,
            self.cache_data.get('id'),
            self.cache_data.get('name') or self.cache_data.get(self._repr_field))

    def to_dict(self):
        result_dict = self.to_shotgun_entity()
        result_dict.update(self.cache_data)
        return result_dict

    def add(self):
        if self.cache_data.get('id'):
            raise Exception('{}<{}> has already in shotgun database'.format(self.sg_table, self.cache_data.get('id')))
        my_sg = MyShotgun()
        return my_sg.create(self.sg_table, self.cache_data)

    def update(self):
        if not self.cache_data.get('id'):
            raise Exception('It is not in shotgun database. you should create it first.')
        result_dict = {}
        for k, v in self.cache_data.items():
            editable = self.sg_fields.get(k, {'editable': False}).get('editable', True)
            if editable:
                result_dict[k] = v
        my_sg = MyShotgun()
        return my_sg.update(self.sg_table, self.cache_data.get('id'), result_dict)

    def upload(self, path, field_name):
        my_sg = MyShotgun()
        return my_sg.upload(self.sg_table, self.cache_data.get('id'), path=path, field_name=field_name)

    def upload_thumbnail(self, path):
        my_sg = MyShotgun()
        return my_sg.upload_thumbnail(self.sg_table, self.cache_data.get('id'), path=path)

    def delete(self):
        if not self.cache_data.get('id'):
            raise Exception('It is not in shotgun database. you should create it first.')
        my_sg = MyShotgun()
        return my_sg.delete(self.sg_table, self.id)

    @classmethod
    def summarize(cls, *args, **kwargs):
        filters = list(args)
        my_sg = MyShotgun()
        return my_sg.summarize(cls.sg_table, filters, **kwargs)

    @classmethod
    def query(cls, *args, **kwargs):
        result = cls.query_dict(*args, **kwargs)
        return [cls.from_shotgun_entity(entity) for entity in result] if result else []

    @classmethod
    def query_dict(cls, *args, **kwargs):
        filters = list(args)
        my_sg = MyShotgun()
        result_fields = [cls.sg_fields[k].get('inner_code', k) for k in cls.sg_fields.keys() if
                         not cls.sg_fields[k].get('later', False)]
        result_fields.extend(kwargs.get('extra_fields', []))
        return my_sg.find(cls.sg_table, filters, result_fields)

    @classmethod
    def shotgun_entity(cls, _id):
        return {'id': _id, 'type': cls.sg_table}

    def to_shotgun_entity(self):
        return self.__class__.shotgun_entity(self.cache_data.get('id'))

    @classmethod
    def from_shotgun_entity(cls, data_dict):
        class_name = data_dict['type']
        if class_name not in globals().keys():
            raise Exception('There is no shotgun wrap type <{}> '.format(class_name))
        entity = globals()[class_name]()
        entity.fill_data(data_dict)
        return entity


class Project(SGEntityBase):
    pass


class Shot(SGEntityBase):
    pass


class Sequence(SGEntityBase):
    pass


class Task(SGEntityBase):
    _repr_field = 'content'


class Version(SGEntityBase):
    pass


class Asset(SGEntityBase):
    pass


class HumanUser(SGEntityBase):
    pass


class Department(SGEntityBase):
    pass


class Step(SGEntityBase):
    pass


class Status(SGEntityBase):
    pass


class TimeLog(SGEntityBase):
    _repr_field = 'date'


class MyCustomEntity(SGEntityBase):
    pass


class Note(SGEntityBase):
    _repr_field = 'subject'

    def note_thread_read(self, entity_fields=None):
        sg = MyShotgun()
        return sg.note_thread_read(self.id, entity_fields)


class Attachment(SGEntityBase):
    pass


class LocalStorage(SGEntityBase):
    pass


class Reply(SGEntityBase):
    _repr_field = 'content'


class Group(SGEntityBase):
    pass


if __name__ == '__main__':
    prj = Project.query(Project.name.is_('dayu-demo'))[0]
    print prj
