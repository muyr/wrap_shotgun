# -*- coding: utf-8 -*-
###################################################################
# Author: Mu yanru
# Date  : 2016.11
# Email : muyanru345@163.com
###################################################################

import datetime as dt
import json
import pkgutil
from collections import defaultdict
from ShotgunObj import MyShotgun
import __builtin__


class MyNone(object):
    pass


class FieldBase(object):
    def __init__(self, field_code, field_label, field_type, editable):
        self.field_code = field_code
        self.field_label = field_label
        self.field_type = field_type
        self.editable = editable

    def __get__(self, instance, owner):
        if instance is None: return self
        value = instance._cache_data.get(self.field_code, MyNone)
        if (value is MyNone) and instance.id:
            _fields = instance._fields_
            sg_fields = [_fields[k].get('inner_code', k) for k in _fields.keys()]
            my_sg = MyShotgun()
            print 'find_one'
            result = my_sg.find_one(instance._sg_table, [['id', 'is', instance.id]], sg_fields)
            if result is None:
                raise Exception('Shotgun doesn\'t have this id[%d] %s!' % (instance.id, owner._sg_table))
            instance._initData_(result)
            value = instance._cache_data.get(self.field_code, MyNone)
        return value

    def __set__(self, instance, value):
        if not self.editable:
            raise Exception(
                'Entity "%s" This field "%s" is not editable!' % (instance._sg_table, self.field_code))


class SGField(FieldBase):
    def __set__(self, instance, value):
        super(SGField, self).__set__(instance, value)
        if isinstance(value, (self.field_type, MyNone)) or (value is None):
            instance._cache_data[self.field_code] = value
        else:
            raise TypeError('{} need {}, not {}'.format(self.field_code, self.field_type, type(value)))


class FileField(FieldBase):
    _cached_data = None
    _fields_List = ['content_type', 'mime-type', 'link_type', 'name',
                    'local_path', 'local_path_windows', 'local_path_linux', 'local_path_mac', 'local_path_windows',
                    'local_storage', 'url', 'id', 'type']

    def __get__(self, instance, owner):
        dataDict = super(FileField, self).__get__(instance, owner)
        if isinstance(dataDict, self.field_type):
            self._cached_data = dataDict
            for f in self._fields_List:
                setattr(self, f, dataDict.get(f, None))
            return self
        else:
            return None

    def __set__(self, instance, value):
        super(FileField, self).__set__(instance, value)

    def __repr__(self):
        return "File{name:'%s', url:'%s'}" % (self.name, self.url)

    def download(self, baseDir=None):
        if self._cached_data.get('link_type', 'upload') == 'local':
            return self._cached_data['local_path']
        if not baseDir:
            import os
            baseDir = os.environ['TMP']
        sg = MyShotgun()
        return sg.download_attachment(attachment=self._cached_data,
                                      file_path='%s/%s' % (baseDir, self._cached_data.get('name')))

    def dataDict(self):
        return self._cached_data


class ProxyEntity(FieldBase):
    _cached_data_dict = {}

    def __get__(self, instance, owner):
        dataDict = super(ProxyEntity, self).__get__(instance, owner)
        # and dataDict.has_key('type') and dataDict.has_key('id')
        # 有理由认为，这里从shotgun获取到的是包含了id 和 type 的字典， 要不然就是None
        if isinstance(dataDict, self.field_type):
            keyName = '{0}_{1}_{2}'.format(instance._sg_table, instance.id, self.field_code)
            if not self._cached_data_dict.has_key(keyName):
                self._cached_data_dict.update({keyName: SGEntityBase.fromShotgunEntity(dataDict)})
            return self._cached_data_dict.get(keyName)
        else:
            return None

    def __set__(self, instance, value):
        super(ProxyEntity, self).__set__(instance, value)
        # TODO: more validate
        # TODO: support SGEntityBase
        if isinstance(value, (self.field_type, MyNone)) or (value is None):
            instance._cache_data[self.field_code] = value
        else:
            raise TypeError('{} need {}, not {}'.format(self.field_code, 'dict', type(value)))


class MultiProxyEntity(FieldBase):
    _cached_data_dict = {}

    def __get__(self, instance, owner):
        valueList = super(MultiProxyEntity, self).__get__(instance, owner)
        if isinstance(valueList, self.field_type) and valueList:
            keyName = '{0}_{1}_{2}'.format(instance._sg_table, instance.id, self.field_code)
            if not self._cached_data_dict.has_key(keyName):
                self._cached_data_dict.update({keyName: [SGEntityBase.fromShotgunEntity(value) for value in valueList]})
            return self._cached_data_dict.get(keyName)
        else:
            return []

    def __set__(self, instance, value):
        super(MultiProxyEntity, self).__set__(instance, value)
        # TODO: more validate
        # TODO: support SGEntityBase
        if isinstance(value, (list, MyNone)) or (value is None):
            instance._cache_data[self.field_code] = value
        else:
            raise TypeError('{} need {}, not {}'.format(self.field_code, 'list', type(value)))


class SGMetaClass(type):
    def __new__(cls, name, bases, clsdict):
        data = pkgutil.get_data('MORE_CLOUD.MORE_SG.ShotgunSchema', '{}.json'.format(name))
        fields = json.loads(data)
        clsdict.update({'_fields_': fields, '_sg_table': name, '_display_name': name, 'type': name})
        for field_code, field_dict in fields.items():
            field_label = field_dict['field_label']
            field_type = field_dict['field_type']
            editable = field_dict.get('editable', True)
            if field_type == 'entity':
                clsdict.update({field_code: ProxyEntity(field_code, field_label, dict, editable)})
            elif field_type == 'multi_entity':
                clsdict.update({field_code: MultiProxyEntity(field_code, field_label, list, editable)})
            elif field_type == 'url':
                clsdict.update({field_code: FileField(field_code, field_label, dict, editable)})
            elif field_type == 'date_time':
                clsdict.update({field_code: SGField(field_code, field_label, dt.datetime, editable)})
            else:
                clsdict.update(
                    {field_code: SGField(field_code, field_label, getattr(__builtin__, field_type), editable)})
        return super(SGMetaClass, cls).__new__(cls, name, bases, clsdict)


class SGEntityBase(object):
    def __init__(self, key=None):
        self.id = key
        self._cache_data = defaultdict(MyNone)

    def _initData_(self, dataDict):
        for k, v in self._fields_.items():
            attr = v['inner_code'] if v.has_key('inner_code') else k
            if k in dataDict.keys():
                self._cache_data[k] = dataDict[k]
                if v.has_key('inner_code'):
                    # 如果从sg查询得到外链的结果，有name，没有inner_code，那么把inner_code属性值设置为 name的值
                    self._cache_data[v['inner_code']] = dataDict[k]
            elif attr in dataDict.keys():
                # 如果从sg查询本身得到的结果，没有name，但有name 的 inner_code，那么把name属性值设置为 inner_code的值
                self._cache_data[k] = dataDict[attr]

    def __repr__(self):
        return "%s{id:%s, %s:'%s'}" % (
            self._display_name, self.id, 'name', getattr(self, 'name', None))

    def toPyDict(self):
        resultDict = self.toShotgunEntity()
        resultDict.update(self._cache_data)
        return resultDict

    def commit(self):
        resultDict = {}
        for k, v in self._cache_data.items():
            editable = self._fields_.get(k, {'editable': False}).get('editable', True)
            if editable:
                resultDict[k] = v
        my_sg = MyShotgun()
        return my_sg.update(self._sg_table, self.id, resultDict)

    @classmethod
    def query(cls, *args, **kwargs):
        result = cls.query_dict(*args, **kwargs)
        return [cls.fromShotgunEntity(entity) for entity in result] if result else []

    @classmethod
    def query_dict(cls, *args, **kwargs):
        filters = []
        if kwargs:
            for k, v in kwargs.items():
                if k == 'id':
                    filters.append([k, 'is', v])
                    continue
                if k not in cls._fields_.keys():
                    raise '{0} has no attribute {1}'.format(cls._sg_table, k)
                else:
                    key = cls._fields_[k].get('inner_code', k)
                    if isinstance(v, list) and v:
                        values = [i if not isinstance(i, SGEntityBase) else i.toShotgunEntity() for i in v]
                        filters.append([key, 'in', values])
                    else:
                        value = v if not isinstance(v, SGEntityBase) else v.toShotgunEntity()
                        filters.append([key, 'is', value])
        if len(args) == 1:
            filters.append([cls._fields_['name'].get('inner_code', 'name'), 'is', args[0]])
        my_sg = MyShotgun()
        print 'find'
        sg_fields = [cls._fields_[k].get('inner_code', k) for k in cls._fields_.keys()]
        result = my_sg.find(cls._sg_table, filters, sg_fields)
        return result

    @classmethod
    def fromString(cls, content):
        result = cls.query(content)
        return result[0] if result else None

    @classmethod
    def shotgunEntity(cls, _id):
        return {'id': _id, 'type': cls._sg_table}

    def toShotgunEntity(self):
        return self.__class__.shotgunEntity(self.id)

    @classmethod
    def fromShotgunEntity(cls, dataDict):
        className = dataDict['type']
        if not globals().has_key(className):
            raise Exception('shotgun type <%s> you give doesn\'t match class <%s>' % (className, cls._sg_table))
        entity = globals()[className](dataDict['id'])
        entity._initData_(dataDict)
        return entity

    @classmethod
    def getShotgunSchema(cls, clean=True):
        my_sg = MyShotgun()
        fields = my_sg.schema_field_read(cls._sg_table)
        if clean:
            resultDict = {}
            for name in fields.keys():
                resultDict[name] = {"field_type": fields[name]['data_type']['value'],
                                    "field_label": fields[name]['name']['value'],
                                    "editable": fields[name]['editable']['value']
                                    }
            return resultDict
        else:
            return fields


class Project(SGEntityBase):
    __metaclass__ = SGMetaClass


class Shot(SGEntityBase):
    __metaclass__ = SGMetaClass


class Sequence(SGEntityBase):
    __metaclass__ = SGMetaClass


class Task(SGEntityBase):
    __metaclass__ = SGMetaClass


class Version(SGEntityBase):
    __metaclass__ = SGMetaClass


class Asset(SGEntityBase):
    __metaclass__ = SGMetaClass


class HumanUser(SGEntityBase):
    __metaclass__ = SGMetaClass


class Department(SGEntityBase):
    __metaclass__ = SGMetaClass


class Step(SGEntityBase):
    __metaclass__ = SGMetaClass


class Status(SGEntityBase):
    __metaclass__ = SGMetaClass


class TimeLog(SGEntityBase):
    __metaclass__ = SGMetaClass


class CustomNonProjectEntity01(SGEntityBase):
    __metaclass__ = SGMetaClass


CustomNonProjectEntity01._display_name = 'MyPerson'
MyPerson = CustomNonProjectEntity01


class CustomEntity02(SGEntityBase):
    __metaclass__ = SGMetaClass


CustomEntity02._display_name = 'ProjectPerson'
ProjectPerson = CustomEntity02


class CustomEntity05(SGEntityBase):
    __metaclass__ = SGMetaClass


CustomEntity05._display_name = 'OtherWorks'
OtherWorks = CustomEntity05


class CustomEntity08(SGEntityBase):
    __metaclass__ = SGMetaClass


CustomEntity08._display_name = 'PublishWorkFile'
PublishWorkFile = CustomEntity08


class CustomEntity09(SGEntityBase):
    __metaclass__ = SGMetaClass


CustomEntity09._display_name = 'PublishElement'
PublishElement = CustomEntity09


class Note(SGEntityBase):
    __metaclass__ = SGMetaClass


class Attachment(SGEntityBase):
    __metaclass__ = SGMetaClass


class LocalStorage(SGEntityBase):
    __metaclass__ = SGMetaClass


if __name__ == '__main__':
    # user = HumanUser.fromString('xiaoao guo')
    # print user.name
    prj = Project.fromString('other')
    for i in Task.query(project=prj):
        print i.entity
        # seq = Sequence.fromString('pl')
        # sg = MyShotgun()
        # v = sg.find_one('Version', [['id', 'is', 28869]], ['image'])
        # print v
        # v = Version(28869)
        # print v.sg_uploaded_movie
        # print v.image
        # print v.sg_uploaded_movie.download()
        # print v.entity
        # a = Attachment.fromFileField(v.sg_uploaded_movie)
        # n = Note(11553)
        # for i in n.attachments:
        #     print i.this_file.download('d:/mu_data')
        # print a.download(baseDir='d:/mu_data')
        # print user
        # t = Task.query(task_assignees=user)[0]
        # print t.entity
        # print t.entity
        # task = Task(24779)
        # print task.task_assignees
        # t = Task()
        # t.content = 'test'
        # print t
        # i = MyPerson.fromString('muyanru')
        # p = Project.fromString()
        # ProjectPerson.query(name='muyanru', project=)
        # print i.sg_project_people
        # print i._display_name
        # seq = Sequence.fromString('pl')
        # prj = Project.fromString('wkz-test')
        # print seq, prj
        # print Task.query(sg_asset_type='prp', project=prj, sg_status_list='ip')
        # print Task.query(sequence=seq)[0].upstream_tasks
        # print Status.query_dict()
