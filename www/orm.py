#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import logging; logging.basicConfig(level=logging.INFO)
import aiomysql

async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 10),
        loop=loop
    )

async def select(sql, args, size=None):
    logging.info(sql, args)
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        # aiomysql.DictCursor 将数据以dict形式返回的cursor
        await cur.execute(sql.replace('?', '%s'), args or ())
        # 用参数替换而非直接SQL字符串，可防止SQL注入攻击
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('row returned: %s' % len(rs))
        return rs

async def execute(sql, args):
# 用于INSERT, UPDATE, DELETE,因为它们需要相同的参数，并返回一个整数表示影响的行数。
    logging.info(sql,args)
    global __pool
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'),args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected

# ORM


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='integer(20)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default )

class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称：
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table:%s)' % (name, tableName))
        # 获取所有的Field和主域名
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        # 以上for循环和if确保新的class（类，用于对应表）有且仅有一个主键
        for k in mappings.keys():
            attrs.pop(k)
        # 从类属性里删去表示表格数据的属性
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        # escaped_fields 加了反引号的fields，除主键外的属性名list
        attrs['__mappings__'] = mappings # 保存属性名和列字段的映射关系，k为属性名，v为列的字段类型
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句：
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) value (%s)' % (tableName,', '.join(escaped_fields), primaryKey, '?'*(len(escaped_fields)+1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        # 根据主键位置，更新其他属性值
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        # mysql语句中反引号``表示非mysql保留字段,一般将表名、库名加上反引号以保证执行。
        # 返回的attrs被修改，与表格相关的数据被保存在__mappings__里
        return type.__new__(cls, name, bases, attrs)

class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError("'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)
        # getattr 取得属性值，不存在则返回默认值（即第三个参数）
        # 直接调用self.__getattr__再作if判断可能会好些，因为可以避免表格列名存在同名属性

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            # __mappings__为属性名和字段类型的对应字典，上句找出了字段类型
            if field.default is not None:
                # ‘如果字段类有default值’
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)  # 读出default值后，别忘了存进去
        return value

    # 类方法申明，首个必要参数cls为当前类.
    @classmethod
    async def find(cls, pk):
        # 'find object by primary key.'
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        # 以主键为依据查找，结果必然只有一个，这边的参数'1'可能可以省略
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        # 主键放在最后一个
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    @classmethod
    async def findAll(cls, field, value):
        # find all items where field is value
        if field in cls.__mappings__:
            rs = await select('%s where `%s`=?' % (cls.__select__, field), [value])
        else:
            logging.info('%s not exist in table %s' % (field, cls.__table__))
        return [cls(**i) for i in rs]

    async def findNumber(self, field, value):
        # find items amount where field is value
        if field in self.__mappings__:
            rs = await select('SELECT COUNT(`%s`) where `%s`=?' % (field, field), [value], 1)
        else:
            logging.info('%s not exist in table %s' % (field, self.__table__))
        return rs[0]

    async def update(self):
        rows = await execute(self.__update__, list(map(self.getValueOrDefault, self.__fields__ + [self.__primary_key__])))
        # 也可以使用save方法的代码，本质一样
        if rows != 1:
            logging.warn('failed to update record: affected rows: %s' % rows)

    async def remove(self):
        rows = await execute(self.__delete__,self.getValue(self.__primaty_key__))
        if rows != 1:
            logging.warn('failed to remove record: affected rows: %s' % rows)


class User(Model):
    __table__ = 'users'

    id = IntegerField(primary_key=True)
    name = StringField()

# 小思考：
# 1.该orm方法并没有对表的数据进行检验，
#   也就是说，
#   class User(Model):
#    __table__ = 'users'
#
#   id = IntegerField(primary_key = True)
#    name = StringField()
#    之后， 产生一条表数据时，User(id = 123, name = 'Garrin')是通过的， User(id = 'boss', name = 'Garrin', age = 20)也是通过的，
#    实例的条目数据存储在dict中，表的属性存储在__mappings__中，互无验证要求。
#    改进方法： 可在Model的init中加入验证代码。

# 2.类（表）申明后，并没有连接mysql创建表的代码。

