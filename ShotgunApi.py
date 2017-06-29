# -*- coding: utf-8 -*-
###################################################################
# Author: Mu yanru
# Date  : 2016.11
# Email : muyanru345@163.com
###################################################################

from shotgun_api3 import Shotgun
import datetime
import json
import pkgutil
import os
import sys
from collections import defaultdict
import ConfigParser


class MyNone(object):
    pass


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


class ShotgunObj(object):
    __metaclass__ = Singleton

    def __init__(self, url, login=None, password=None, script_name=None, api_key=None):
        self._url = url
        self._login = login
        self._password = password
        self._script_name = script_name
        self._api_key = api_key
        self._sg = self.connectShotgun()

    def connectShotgun(self):
        try:
            if self._login is None:
                sg = Shotgun(self._url, script_name=self._script_name, api_key=self._api_key)
            else:
                sg = Shotgun(self._url, login=self._login, password=self._password)
            print 'connect succesfully!'
            return sg
        except:
            print 'connect failed!'
            return None

    def getEntityFields(self, entity):
        return self._sg.schema_field_read(entity)

    def findEntity(self, entity, filter, fields):
        return self._sg.find_one(entity, filter, fields)

    def findEntityOneByOne(self, entity, filterList, fields):
        index = 1
        summarizeDataDict = self._sg.summarize(entity_type=entity,
                                               filters=filterList,
                                               summary_fields=[{'field': 'id', 'type': 'count'}, ])
        count = summarizeDataDict['summaries']['id']
        while True:
            result = self._sg.find(entity, filterList, fields, limit=1, page=index)
            if result:
                yield result[0], index, count
                index += 1
            else:
                break

    def findEntities(self, entity, filter, fields):
        return self._sg.find(entity, filter, fields)

    def createEntity(self, table, dataDict):
        return self._sg.create(table, dataDict)

    def updateEntity(self, table, sg_id, dataDict):
        return self._sg.update(table, sg_id, dataDict)

    def upload(self, sgORM, movFile):
        return self._sg.upload(sgORM.__class__.__name__, sgORM.id, movFile, 'sg_uploaded_movie',
                               os.path.basename(movFile))


class MyShotgun(ShotgunObj):
    def __init__(self):
        _config = ConfigParser.SafeConfigParser()
        _config.read(os.path.realpath(sys.path[0]) + '/sg.conf')
        url = _config.get('shotgun', 'url')
        script = _config.get('shotgun', 'script')
        key = _config.get('shotgun', 'key')
        super(MyShotgun, self).__init__(url, script_name=script, api_key=key)


class FieldBase(object):
    def __init__(self, field_code, field_label, field_type, editable):
        self.field_code = field_code
        self.field_label = field_label
        self.field_type = field_type
        self.editable = editable

    def __get__(self, instance, owner):
        if instance is None: return self
        value = instance._cache_data.get(self.field_code, MyNone)
        if value is MyNone:
            ps = MyShotgun()
            result = ps.findEntity(instance.__class__.__name__, [['id', 'is', instance._id]], instance._fields_.keys())
            if result is None:
                raise Exception('Shotgun doesn\'t have this id[%d] %s!' % (instance._id, owner.__name__))
            instance._initData_(result)
            value = result[self.field_code]
        return value

    def __set__(self, instance, value):
        if not self.editable:
            raise Exception(
                'Entity "%s" This field "%s" is not editable!' % (instance.__class__.__name__, self.field_code))


class SGField(FieldBase):
    def __set__(self, instance, value):
        super(SGField, self).__set__(instance, value)
        if isinstance(value, (self.field_type, MyNone)) or (value is None):
            instance._cache_data[self.field_code] = value
        else:
            raise TypeError('{} need {}, not {}'.format(self.field_code, self.field_type, type(value)))


class ProxyEntity(FieldBase):
    def __get__(self, instance, owner):
        value = super(ProxyEntity, self).__get__(instance, owner)
        entity = globals()[value['type']].fromShotgunEntity(value)
        return entity

    def __set__(self, instance, value):
        super(ProxyEntity, self).__set__(instance, value)
        if isinstance(value, dict) or isinstance(value, MyNone) or (value is None):
            instance._cache_data[self.field_code] = value
        else:
            raise TypeError('{} need {}, not {}'.format(self.field_code, 'dict', type(value)))


class MultiProxyEntity(FieldBase):
    def __get__(self, instance, owner):
        valueList = super(MultiProxyEntity, self).__get__(instance, owner)
        entities = [globals()[value['type']].fromShotgunEntity(value) for value in valueList]
        return entities

    def __set__(self, instance, value):
        super(MultiProxyEntity, self).__set__(instance, value)
        if isinstance(value, (list, MyNone)) or (value is None):
            instance._cache_data[self.field_code] = value
        else:
            raise TypeError('{} need {}, not {}'.format(self.field_code, 'list', type(value)))


class SGMetaClass(type):
    def __new__(cls, name, bases, clsdict):
        data = pkgutil.get_data('ShotgunSchema', '{}.json'.format(name))
        fields = json.loads(data)
        clsdict.update({'_fields_': fields})
        for field_code, field_dict in fields.items():
            field_label = field_dict['field_label']
            field_type = field_dict['field_type']
            editable = field_dict.get('editable', True)
            if field_type == 'ProxyEntity':
                clsdict.update({field_code: ProxyEntity(field_code, field_label, dict, editable)})
            elif field_type == 'MultiProxyEntity':
                clsdict.update({field_code: MultiProxyEntity(field_code, field_label, list, editable)})
            else:
                clsdict.update({field_code: SGField(field_code, field_label, eval(field_type), editable)})
        return super(SGMetaClass, cls).__new__(cls, name, bases, clsdict)


class SGEntityBase(object):
    _repr_field = 'name'

    def __init__(self, key):
        self._id = key
        self._cache_data = defaultdict(MyNone)

    def _initData_(self, dataDict):
        for k, v in dataDict.items():
            if k in self._fields_.keys():
                self._cache_data[k] = v

    def __repr__(self):
        return "%s{'id':%s, '%s':'%s'}" % (
            self.__class__.__name__, self._id, self._repr_field, getattr(self, self._repr_field, MyNone))

    def toPyData(self):
        resultDict = self.toShotgunEntity()
        resultDict.update(self._cache_data)
        return resultDict

    def commit(self):
        ps = MyShotgun()
        resultDict = {}
        for k, v in self._cache_data.items():
            editable = self._fields_.get(k, {'editable': False}).get('editable', True)
            if editable:
                resultDict[k] = v
        return ps.updateEntity(self.__class__.__name__, self._id, resultDict)

    @classmethod
    def query(cls, *args, **kwargs):
        ps = MyShotgun()
        filters = []
        if kwargs:
            for k, v in kwargs.items():
                if k not in cls._fields_.keys():
                    print '{0} has no attribute {1}'.format(cls.__name__, k)
                else:
                    filters.append([k, 'is', v if not isinstance(v, SGEntityBase) else v.toShotgunEntity()])
        elif len(args) == 1:
            filters.append([cls._repr_field, 'is', args[0]])
        else:
            raise Exception('one arg, or key=word style!')
        result = ps.findEntities(cls.__name__, filters, cls._fields_.keys())
        return [cls.fromShotgunEntity(entity) for entity in result] if result else []

    @classmethod
    def fromString(cls, content):
        result = cls.query(content)
        return result[0] if result else None

    @classmethod
    def shotgunEntity(cls, _id):
        return {'id': _id, 'type': cls.__name__}

    def toShotgunEntity(self):
        return self.__class__.shotgunEntity(self._id)

    @classmethod
    def fromShotgunEntity(cls, dataDict):
        className = dataDict['type']
        if cls.__name__ != className:
            raise Exception('shotgun type <%s> you give doesn\'t match class <%s>' % (className, cls.__name__))
        else:
            entity = cls(dataDict['id'])
            entity._initData_(dataDict)
            return entity


class Project(SGEntityBase):
    __metaclass__ = SGMetaClass


class Shot(SGEntityBase):
    __metaclass__ = SGMetaClass
    _repr_field = 'code'


class Sequence(SGEntityBase):
    __metaclass__ = SGMetaClass
    _repr_field = 'code'


class Task(SGEntityBase):
    __metaclass__ = SGMetaClass
    _repr_field = 'content'


class Version(SGEntityBase):
    __metaclass__ = SGMetaClass
    _repr_field = 'code'


class Asset(SGEntityBase):
    __metaclass__ = SGMetaClass
    _repr_field = 'code'


class HumanUser(SGEntityBase):
    __metaclass__ = SGMetaClass
    _repr_field = 'login'


class Department(SGEntityBase):
    __metaclass__ = SGMetaClass


class Step(SGEntityBase):
    __metaclass__ = SGMetaClass
    _repr_field = 'code'


class Status(SGEntityBase):
    __metaclass__ = SGMetaClass
    _repr_field = 'code'


class TimeLog(SGEntityBase):
    __metaclass__ = SGMetaClass
    _repr_field = 'date'


# A tool for author to get shotgun entity's fields
def _getEntityFields(entity):
    a = MyShotgun()
    fields = a.getEntityFields(entity)
    for name in fields.keys():
        fieldType = fields[name]['data_type']['value']
        print name, fieldType, fields[name]['name']['value'], fields[name]['editable']['value']


if __name__ == '__main__':
    user = HumanUser.fromString('postgres')
    print user.email
    for timelog in TimeLog.query(user=user):
        print timelog
        # print user.commit()
        # for t in Task.query(task_assignees=user, project=p):
        #     print t.content, t.entity, t.step, t.task_assignees
        # for p in Status.query():
        #     print p
