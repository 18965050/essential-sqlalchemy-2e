from datetime import datetime
from sqlalchemy import (MetaData, Table, Numeric, Column,ForeignKey, create_engine)

if __name__=='__main__':
	isOracle=False
	isMySql=True

if isOracle:
	# oracle dll load
	import ctypes
	ctypes.WinDLL('D:/installed/instantclient_12_1/oci.dll')

metadata = MetaData()
#################################Oracle Table################################################
if isOracle:
	from sqlalchemy import Sequence
	from sqlalchemy.dialects.oracle import *
	seqCookies = Sequence('seq_oracle_cookies', minvalue=1, maxvalue=9999999999, start=1,
	                      increment=1, metadata=metadata)

	seqUsers = Sequence('seq_oracle_users', minvalue=1, maxvalue=9999999999, start=1,
	                    increment=1, metadata=metadata)

	seqLineItems = Sequence('seq_oracle_line_items', minvalue=1, maxvalue=9999999999, start=1,
	                        increment=1, metadata=metadata)

	cookies = Table('oracle_cookies', metadata,
	                Column('cookie_id', NUMBER(10), seqCookies, primary_key=True),
	                Column('cookie_name', VARCHAR2(50), index=True),
	                Column('cookie_recipe_url', VARCHAR2(255)),
	                Column('cookie_sku', VARCHAR2(55)),
	                Column('quantity', NUMBER(5)),
	                Column('unit_cost', NUMBER(12, 2))
	                )

	users = Table('oracle_users', metadata,
	              Column('user_id', NUMBER(10), seqUsers, primary_key=True),
	              Column('username', VARCHAR2(15), nullable=False, unique=True),
	              Column('email_address', VARCHAR2(255), nullable=False),
	              Column('phone', VARCHAR2(20), nullable=False),
	              Column('password', VARCHAR2(25), nullable=False),
	              Column('created_on', TIMESTAMP(6), default=datetime.now),
	              Column('updated_on', TIMESTAMP(6), default=datetime.now, onupdate=datetime.now)
	              )

	orders = Table('oracle_orders', metadata,
	               Column('order_id', NUMBER(10), primary_key=True),
	               Column('user_id', ForeignKey('oracle_users.user_id')),
	               )

	line_items = Table('oracle_line_items', metadata,
	                   Column('line_items_id', NUMBER(10), seqLineItems, primary_key=True),
	                   Column('order_id', ForeignKey('oracle_orders.order_id')),
	                   Column('cookie_id', ForeignKey('oracle_cookies.cookie_id')),
	                   Column('quantity', NUMBER(10)),
	                   Column('extended_cost', NUMBER(12, 2))
	                   )

	engine = create_engine('oracle://ins:instest@192.168.27.240:1521/ins', echo=True)

#################################MySql Table################################################
if isMySql:
	from sqlalchemy.dialects.mysql import *
	cookies = Table('mysql_cookies', metadata,
	                Column('cookie_id', INTEGER(10), primary_key=True,autoincrement=True),
	                Column('cookie_name', VARCHAR(50), index=True),
	                Column('cookie_recipe_url', VARCHAR(255)),
	                Column('cookie_sku', VARCHAR(55)),
	                Column('quantity', SMALLINT(5)),
	                Column('unit_cost', NUMERIC(12, 2))
	                )

	users = Table('mysql_users', metadata,
	              Column('user_id', INTEGER(10), primary_key=True, autoincrement=True),
	              Column('username', VARCHAR(15), nullable=False, unique=True),
	              Column('email_address', VARCHAR(255), nullable=False),
	              Column('phone', VARCHAR(20), nullable=False),
	              Column('password', VARCHAR(25), nullable=False),
	              Column('created_on', DATETIME(), default=datetime.now),
	              Column('updated_on', DATETIME(), default=datetime.now, onupdate=datetime.now)
	              )

	orders = Table('mysql_orders', metadata,
	               Column('order_id', INTEGER(10), primary_key=True),
	               Column('user_id', ForeignKey('mysql_users.user_id')),
	               )

	line_items = Table('mysql_line_items', metadata,
	                   Column('line_items_id', INTEGER(10), primary_key=True),
	                   Column('order_id', ForeignKey('mysql_orders.order_id')),
	                   Column('cookie_id', ForeignKey('mysql_cookies.cookie_id')),
	                   Column('quantity', INTEGER(10)),
	                   Column('extended_cost', NUMERIC(12, 2))
	                   )

	engine = create_engine('mysql+pymysql://chaos:chaos@ins-test:3306/chaos_demo',echo=True)

connection = engine.connect()

if isOracle:
	connection.execute('''
	BEGIN
	    EXECUTE IMMEDIATE 'drop table ins.oracle_line_items';
	    EXCEPTION WHEN OTHERS THEN NULL;
	END;
	''')

	connection.execute('''
	BEGIN
	    EXECUTE IMMEDIATE 'drop table ins.oracle_orders';
	    EXCEPTION WHEN OTHERS THEN NULL;
	END;
	''')

	connection.execute('''
	BEGIN
	    EXECUTE IMMEDIATE 'drop table ins.oracle_users';
	    EXCEPTION WHEN OTHERS THEN NULL;
	END;
	''')

	connection.execute('''
	BEGIN
	    EXECUTE IMMEDIATE 'drop table ins.oracle_cookies';
	    EXCEPTION WHEN OTHERS THEN NULL;
	END;
	''')

	connection.execute('''
	BEGIN
	    EXECUTE IMMEDIATE 'drop sequence ins.seq_oracle_line_items';
	    EXCEPTION WHEN OTHERS THEN NULL;
	END;
	''')

	connection.execute('''
	BEGIN
	    EXECUTE IMMEDIATE 'drop sequence ins.seq_oracle_cookies';
	    EXCEPTION WHEN OTHERS THEN NULL;
	END;
	''')

	connection.execute('''
	BEGIN
	    EXECUTE IMMEDIATE 'drop sequence ins.seq_oracle_users';
	    EXCEPTION WHEN OTHERS THEN NULL;
	END;
	''')

if isMySql:
	connection.execute('drop table if exists mysql_line_items')
	connection.execute('drop table if exists mysql_orders')
	connection.execute('drop table if exists mysql_users')
	connection.execute('drop table if exists mysql_cookies')

metadata.create_all(engine)

ins = cookies.insert().values(
	cookie_name="chocolate chip",
	cookie_recipe_url="http://some.aweso.me/cookie/recipe.html",
	cookie_sku="CC01",
	quantity="12",
	unit_cost="0.50"
)

print(str(ins))

result = connection.execute(ins)

print(result.inserted_primary_key)

from sqlalchemy import insert

ins = insert(cookies).values(
	cookie_name="chocolate chip",
	cookie_recipe_url="http://some.aweso.me/cookie/recipe.html",
	cookie_sku="CC01",
	quantity="12",
	unit_cost="0.50"
)
print(str(ins))

ins = cookies.insert()

# 事务
transaction=connection.begin()
result = connection.execute(ins, cookie_name='dark chocolate chip',
                            cookie_recipe_url='http://some.aweso.me/cookie/recipe_dark.html',
                            cookie_sku='CC02',
                            quantity='1',
                            unit_cost='0.75')
# transaction.rollback()
transaction.commit()
print(result.inserted_primary_key)

inventory_list = [
	{
		'cookie_name': 'peanut butter',
		'cookie_recipe_url': 'http://some.aweso.me/cookie/peanut.html',
		'cookie_sku': 'PB01',
		'quantity': 24,
		'unit_cost': 0.25
	},
	{
		'cookie_name': 'oatmeal raisin',
		'cookie_recipe_url': 'http://some.okay.me/cookie/raisin.html',
		'cookie_sku': 'EWW01',
		'quantity': 100,
		'unit_cost': 1.00
	}
]

result = connection.execute(ins, inventory_list)

customer_list = [
	{
		'username': "cookiemon",
		'email_address': "mon@cookie.com",
		'phone': "111-111-1111",
		'password': "password"
	},
	{
		'username': "cakeeater",
		'email_address': "cakeeater@cake.com",
		'phone': "222-222-2222",
		'password': "password"
	},
	{
		'username': "pieguy",
		'email_address': "guy@pie.com",
		'phone': "333-333-3333",
		'password': "password"
	}
]

ins = users.insert()
result = connection.execute(ins, customer_list)

from sqlalchemy import insert

ins = insert(orders).values(user_id=1, order_id=1)
result = connection.execute(ins)

ins = insert(line_items)
order_items = [
	{
		'order_id': 1,
		'cookie_id': 1,
		'quantity': 2,
		'extended_cost': 1.00
	},
	{
		'order_id': 1,
		'cookie_id': 3,
		'quantity': 12,
		'extended_cost': 3.00
	}
]
result = connection.execute(ins, order_items)

ins = insert(orders).values(user_id=2, order_id=2)
result = connection.execute(ins)

ins = insert(line_items)
order_items = [
	{
		'order_id': 2,
		'cookie_id': 1,
		'quantity': 24,
		'extended_cost': 12.00
	},
	{
		'order_id': 2,
		'cookie_id': 4,
		'quantity': 6,
		'extended_cost': 6.00
	}
]
result = connection.execute(ins, order_items)

# 基本select
from sqlalchemy.sql import select

s = select([cookies])
print(str(s))
rp = connection.execute(s)
results = rp.fetchall()  # 获取所有数据
first_row = results[0]
print(first_row[1])
print(first_row.cookie_name)  # 获取字段值方式一
print(first_row[cookies.c.cookie_name])  # 获取字段值方式二

# 通过table对象select
s = cookies.select()
rp = connection.execute(s)
for record in rp:
	print(record.cookie_name)

# 查询表中的几个字段
s = select([cookies.c.cookie_name, cookies.c.quantity])
rp = connection.execute(s)
print(rp.keys())
results = rp.fetchall()
print(results)

# 排序(默认升序)
s = select([cookies.c.cookie_name, cookies.c.quantity])
s = s.order_by(cookies.c.quantity, cookies.c.cookie_name)
rp = connection.execute(s)
for cookie in rp:
	print('{} - {}'.format(cookie.quantity, cookie.cookie_name))

# 排序(降序)
from sqlalchemy import desc, asc

s = select([cookies.c.cookie_name, cookies.c.quantity])
s = s.order_by(desc(cookies.c.quantity))
rp = connection.execute(s)
for cookie in rp:
	print('{} - {}'.format(cookie.quantity, cookie.cookie_name))

# 限制结果条数
s = select([cookies.c.cookie_name, cookies.c.quantity])
s = s.order_by(cookies.c.quantity)
s = s.limit(2)
rp = connection.execute(s)
print([result.cookie_name for result in rp])

# 函数使用
from sqlalchemy.sql import func

#默认的字段名称为'count_1'
s = select([func.count(cookies.c.cookie_name)])
rp = connection.execute(s)
record = rp.first()
print(record.keys())
print(record.count_1)

# 使用label作为别名
s = select([func.count(cookies.c.cookie_name).label('inventory_count')])
rp = connection.execute(s)
record = rp.first()
print(record.keys())
print(record.inventory_count)

# 基本where条件使用
s = select([cookies]).where(cookies.c.cookie_name == 'chocolate chip')
rp = connection.execute(s)
record = rp.first()
print(record.items())

# where and条件
s = select([cookies]).where(cookies.c.cookie_name.like('%chocolate%')).where(cookies.c.quantity == 12)
rp = connection.execute(s)
for record in rp.fetchall():
	print(record.cookie_name)

s = cookies.select(limit=1)
for row in connection.execute(s):
	print(row)

# 字段值添加'SKU-'前缀
s = select([cookies.c.cookie_name, 'SKU-' + cookies.c.cookie_sku])
for row in connection.execute(s):
	print(row)

# 查询字段算术运算
s = select([cookies.c.cookie_name, cookies.c.quantity * cookies.c.unit_cost])
for row in connection.execute(s):
	print('{} - {}'.format(row.cookie_name, row.anon_1))

# cast类型转换
from sqlalchemy import cast

s = select([cookies.c.cookie_name, cast((cookies.c.quantity * cookies.c.unit_cost), Numeric(12, 2)).label('inv_cost')])
for row in connection.execute(s):
	print('{} - {}'.format(row.cookie_name, row.inv_cost))

s = select([cookies.c.cookie_name, (cookies.c.quantity * cookies.c.unit_cost).label('inv_cost')])
for row in connection.execute(s):
	print('{:<25} {:.2f}'.format(row.cookie_name, row.inv_cost))

# where and_,or_,not_
from sqlalchemy import and_, or_, not_

s = select([cookies]).where(and_(
	cookies.c.quantity > 23,
	cookies.c.unit_cost < 0.40
))
for row in connection.execute(s):
	print(row.cookie_name)

from sqlalchemy import and_, or_, not_

s = select([cookies]).where(or_(
	cookies.c.quantity.between(10, 50),
	cookies.c.cookie_name.contains('chip')
))
for row in connection.execute(s):
	print(row.cookie_name)

# 基本update
from sqlalchemy import update

u = update(cookies).where(cookies.c.cookie_name == "chocolate chip")
u = u.values(quantity=(cookies.c.quantity + 120))
result = connection.execute(u)
print(result.rowcount)

s = select([cookies]).where(cookies.c.cookie_name == "chocolate chip")
result = connection.execute(s).first()
for key in result.keys():
	print('{:>20}: {}'.format(key, result[key]))

# 基本delete
from sqlalchemy import delete

u = delete(cookies).where(cookies.c.cookie_name == "dark chocolate chip")
result = connection.execute(u)
print(result.rowcount)
s = select([cookies]).where(cookies.c.cookie_name == "dark chocolate chip")
result = connection.execute(s).fetchall()
print(len(result))
print(result)

# 多表关联查询
columns = [orders.c.order_id, users.c.username, users.c.phone, cookies.c.cookie_name, line_items.c.quantity,
           line_items.c.extended_cost]
cookiemon_orders = select(columns)
cookiemon_orders = cookiemon_orders.select_from(users.join(orders).join(line_items).join(cookies)).where(
	users.c.username == 'cookiemon')  # 注意: 这里通过主外键关联
result = connection.execute(cookiemon_orders).fetchall()
for row in result:
	print(row)

print(str(cookiemon_orders))

# 外关联
columns = [users.c.username, orders.c.order_id]
all_orders = select(columns)
all_orders = all_orders.select_from(users.outerjoin(orders))
result = connection.execute(all_orders).fetchall()
for row in result:
	print(row)

# group语句
columns = [users.c.username, func.count(orders.c.order_id)]
all_orders = select(columns)
all_orders = all_orders.select_from(users.outerjoin(orders)).group_by(users.c.username)
print(str(all_orders))
result = connection.execute(all_orders).fetchall()
for row in result:
	print(row)


def get_orders_by_customer(cust_name):
	columns = [orders.c.order_id, users.c.username, users.c.phone, cookies.c.cookie_name, line_items.c.quantity,
	           line_items.c.extended_cost]
	cust_orders = select(columns)
	cust_orders = cust_orders.select_from(users.join(orders).join(line_items).join(cookies)).where(
		users.c.username == cust_name)
	result = connection.execute(cust_orders).fetchall()
	return result


get_orders_by_customer('cakeeater')

# 字段动态扩展
def get_orders_by_customer(cust_name, shipped=None, details=False):
	columns = [orders.c.order_id, users.c.username, users.c.phone]
	joins = users.join(orders)
	if details:
		columns.extend([cookies.c.cookie_name, line_items.c.quantity, line_items.c.extended_cost])
		joins = joins.join(line_items).join(cookies)
	cust_orders = select(columns)
	cust_orders = cust_orders.select_from(joins).where(users.c.username == cust_name)
	# if shipped is not None:
	# 	cust_orders = cust_orders.where(orders.c.shipped == shipped)
	result = connection.execute(cust_orders).fetchall()
	return result


get_orders_by_customer('cakeeater')

get_orders_by_customer('cakeeater', details=True)

# get_orders_by_customer('cakeeater', shipped=True)
#
# get_orders_by_customer('cakeeater', shipped=False)
#
# get_orders_by_customer('cakeeater', shipped=False, details=True)

# 原生sql
if isOracle:
	result = connection.execute("select * from oracle_orders").fetchall()
if isMySql:
	result = connection.execute("select * from mysql_orders").fetchall()
print(result)

from sqlalchemy import text

stmt = select([users]).where(text("username='cookiemon'"))  #注意: oracle字符串使用单引号
print(connection.execute(stmt).fetchall())


