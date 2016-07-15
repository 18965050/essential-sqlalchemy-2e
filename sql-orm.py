from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (MetaData, Table, Numeric, Column, ForeignKey, create_engine)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship, backref

if __name__ == '__main__':
	isOracle = False
	isMySql = True

if isOracle:
	# oracle dll load
	import ctypes

	ctypes.WinDLL('D:/installed/instantclient_12_1/oci.dll')

Base = declarative_base()

if isOracle:
	from sqlalchemy import Sequence
	from sqlalchemy.dialects.oracle import *

	seqCookies = Sequence('seq_oracle_cookies', minvalue=1, maxvalue=9999999999, start=1,
	                      increment=1, metadata=Base.metadata)

	seqUsers = Sequence('seq_oracle_users', minvalue=1, maxvalue=9999999999, start=1,
	                    increment=1, metadata=Base.metadata)

	seqOrders = Sequence('seq_oracle_orders', minvalue=1, maxvalue=9999999999, start=1,
	                     increment=1, metadata=Base.metadata)

	seqLineItems = Sequence('seq_oracle_line_items', minvalue=1, maxvalue=9999999999, start=1,
	                        increment=1, metadata=Base.metadata)

	seqEmployees = Sequence('seq_oracle_employees', minvalue=1, maxvalue=9999999999, start=1,
	                        increment=1, metadata=Base.metadata)


	class Cookie(Base):
		__tablename__ = 'oracle_cookies'

		cookie_id = Column(NUMBER(10), seqCookies, primary_key=True)
		cookie_name = Column(VARCHAR2(50), index=True)
		cookie_recipe_url = Column(VARCHAR2(255))
		cookie_sku = Column(VARCHAR2(55))
		quantity = Column(NUMBER(5))
		unit_cost = Column(NUMBER(12, 2))


	class User(Base):
		__tablename__ = 'oracle_users'

		user_id = Column(NUMBER(10), seqUsers, primary_key=True)
		username = Column(VARCHAR2(15), nullable=False, unique=True)
		email_address = Column(VARCHAR2(255), nullable=False)
		phone = Column(VARCHAR2(20), nullable=False)
		password = Column(VARCHAR2(25), nullable=False)
		created_on = Column(TIMESTAMP(6), default=datetime.now)
		updated_on = Column(TIMESTAMP(6), default=datetime.now, onupdate=datetime.now)


	class Order(Base):
		__tablename__ = 'oracle_orders'
		order_id = Column(NUMBER(10), seqOrders, primary_key=True)
		user_id = Column(NUMBER(10), ForeignKey('oracle_users.user_id'))
		shipped = Column(NUMBER(1), default=False)
		user = relationship("User", backref=backref('oracle_orders', order_by=order_id))


	class LineItem(Base):
		__tablename__ = 'oracle_line_items'
		line_items_id = Column(NUMBER(10), seqLineItems, primary_key=True)
		order_id = Column(NUMBER(10), ForeignKey('oracle_orders.order_id'))
		cookie_id = Column(NUMBER(10), ForeignKey('oracle_cookies.cookie_id'))
		quantity = Column(NUMBER(10))
		extended_cost = Column(Numeric(12, 2))
		order = relationship("Order", backref=backref('oracle_line_items', order_by=line_items_id))
		cookie = relationship("Cookie", uselist=False)


	class Employee(Base):
		__tablename__ = 'oracle_employees'

		id = Column(NUMBER(10), seqEmployees, primary_key=True)
		manager_id = Column(NUMBER(10), ForeignKey('oracle_employees.id'))
		name = Column(VARCHAR2(255), nullable=False)

		manager = relationship("Employee", backref=backref('reports'), remote_side=[id])


	engine = create_engine('oracle://ins:instest@192.168.27.240:1521/ins', echo=True)

if isMySql:
	from sqlalchemy.dialects.mysql import *


	class Cookie(Base):
		__tablename__ = 'mysql_cookies'

		cookie_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
		cookie_name = Column(VARCHAR(50), index=True)
		cookie_recipe_url = Column(VARCHAR(255))
		cookie_sku = Column(VARCHAR(55))
		quantity = Column(SMALLINT(5))
		unit_cost = Column(NUMERIC(12, 2))


	class User(Base):
		__tablename__ = 'mysql_users'

		user_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
		username = Column(VARCHAR(15), nullable=False, unique=True)
		email_address = Column(VARCHAR(255), nullable=False)
		phone = Column(VARCHAR(20), nullable=False)
		password = Column(VARCHAR(25), nullable=False)
		created_on = Column(DATETIME(), default=datetime.now)
		updated_on = Column(DATETIME(), default=datetime.now, onupdate=datetime.now)


	class Order(Base):
		__tablename__ = 'mysql_orders'
		order_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
		user_id = Column(INTEGER(10), ForeignKey('mysql_users.user_id'))
		shipped = Column(TINYINT(1), default=False)
		user = relationship("User", backref=backref('mysql_orders', order_by=order_id))


	class LineItem(Base):
		__tablename__ = 'mysql_line_items'
		line_items_id = Column(INTEGER(10), primary_key=True, autoincrement=True)
		order_id = Column(INTEGER(10), ForeignKey('mysql_orders.order_id'))
		cookie_id = Column(INTEGER(10), ForeignKey('mysql_cookies.cookie_id'))
		quantity = Column(INTEGER(10))
		extended_cost = Column(NUMERIC(12, 2))
		order = relationship("Order", backref=backref('mysql_line_items', order_by=line_items_id))
		cookie = relationship("Cookie", uselist=False)


	class Employee(Base):
		__tablename__ = 'mysql_employees'

		id = Column(INTEGER(10), primary_key=True, autoincrement=True)
		manager_id = Column(INTEGER(10), ForeignKey('mysql_employees.id'))
		name = Column(VARCHAR(255), nullable=False)

		manager = relationship("Employee", backref=backref('reports'), remote_side=[id])


	engine = create_engine('mysql+pymysql://chaos:chaos@ins-test:3306/chaos_demo', echo=True)

connection = engine.connect()

if isOracle:
	connection.execute('''
	BEGIN
	    EXECUTE IMMEDIATE 'drop table ins.oracle_employees';
	    EXCEPTION WHEN OTHERS THEN NULL;
	END;
	''')

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
	    EXECUTE IMMEDIATE 'drop sequence ins.seq_oracle_employees';
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
	    EXECUTE IMMEDIATE 'drop sequence ins.seq_oracle_orders';
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
	connection.execute('drop table if exists mysql_employees')
	connection.execute('drop table if exists mysql_line_items')
	connection.execute('drop table if exists mysql_orders')
	connection.execute('drop table if exists mysql_users')
	connection.execute('drop table if exists mysql_cookies')

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# 普通insert
cc_cookie = Cookie(cookie_name='chocolate chip',
                   cookie_recipe_url='http://some.aweso.me/cookie/recipe.html',
                   cookie_sku='CC01',
                   quantity=12,
                   unit_cost=0.50)
session.add(cc_cookie)
session.commit()
print(cc_cookie.cookie_id)

dcc = Cookie(cookie_name='dark chocolate chip',
             cookie_recipe_url='http://some.aweso.me/cookie/recipe_dark.html',
             cookie_sku='CC02',
             quantity=1,
             unit_cost=0.75)
mol = Cookie(cookie_name='molasses',
             cookie_recipe_url='http://some.aweso.me/cookie/recipe_molasses.html',
             cookie_sku='MOL01',
             quantity=1,
             unit_cost=0.80)
session.add(dcc)
session.add(mol)
session.flush()  # 此时事务未提交,数据并没有真正插入表中

print(dcc.cookie_id)
print(mol.cookie_id)

# 批次insert
c1 = Cookie(cookie_name='peanut butter',
            cookie_recipe_url='http://some.aweso.me/cookie/peanut.html',
            cookie_sku='PB01',
            quantity=24,
            unit_cost=0.25)
c2 = Cookie(cookie_name='oatmeal raisin',
            cookie_recipe_url='http://some.okay.me/cookie/raisin.html',
            cookie_sku='EWW01',
            quantity=100,
            unit_cost=1.00)

session.bulk_save_objects([c1, c2])
session.commit()

print(c1.cookie_id)

# 查询所有
cookies = session.query(Cookie).all()
print(cookies)

for cookie in session.query(Cookie):
	print(cookie)

print(session.query(Cookie.cookie_name, Cookie.quantity).first())

# 升序排序
for cookie in session.query(Cookie).order_by(Cookie.quantity):
	print('{:3} - {}'.format(cookie.quantity, cookie.cookie_name))

# 降序排序
from sqlalchemy import desc

for cookie in session.query(Cookie).order_by(desc(Cookie.quantity)):
	print('{:3} - {}'.format(cookie.quantity, cookie.cookie_name))

# 限制结果条数方式一
query = session.query(Cookie).order_by(Cookie.quantity)[:2]
print([result.cookie_name for result in query])

# 限制结果条数方式二
query = session.query(Cookie).order_by(Cookie.quantity).limit(2)
print([result.cookie_name for result in query])

# 函数使用
from sqlalchemy import func

inv_count = session.query(func.sum(Cookie.quantity)).scalar()
print(inv_count)

# 别名
rec_count = session.query(func.count(Cookie.cookie_name)).first()
print(rec_count)

rec_count = session.query(func.count(Cookie.cookie_name) \
                          .label('inventory_count')).first()
print(rec_count.keys())
print(rec_count.inventory_count)

# where条件
record = session.query(Cookie).filter(Cookie.cookie_name == 'chocolate chip').first()
print(record)

record = session.query(Cookie).filter_by(cookie_name='chocolate chip').first()
print(record)

query = session.query(Cookie).filter(Cookie.cookie_name.like('%chocolate%'))
for record in query:
	print(record.cookie_name)

# 字段值添加'SKU-'前缀
results = session.query(Cookie.cookie_name, 'SKU-' + Cookie.cookie_sku).all()
for row in results:
	print(row)

# 类型转换
from sqlalchemy import cast

query = session.query(Cookie.cookie_name,
                      cast((Cookie.quantity * Cookie.unit_cost),
                           Numeric(12, 2)).label('inv_cost'))
for result in query:
	print('{} - {}'.format(result.cookie_name, result.inv_cost))

# where条件使用 and_, or_, not_
from sqlalchemy import and_, or_, not_

query = session.query(Cookie).filter(
	Cookie.quantity > 23,
	Cookie.unit_cost < 0.40
)
for result in query:
	print(result.cookie_name)

from sqlalchemy import and_, or_, not_

query = session.query(Cookie).filter(
	or_(
		Cookie.quantity.between(10, 50),
		Cookie.cookie_name.contains('chip')
	)
)
for result in query:
	print(result.cookie_name)

# 普通update方式一
query = session.query(Cookie)
cc_cookie = query.filter(Cookie.cookie_name == "chocolate chip").first()
cc_cookie.quantity = cc_cookie.quantity + 120
session.commit()
print(cc_cookie.quantity)

# 普通update方式二
query = session.query(Cookie)
query = query.filter(Cookie.cookie_name == "chocolate chip")
query.update({Cookie.quantity: Cookie.quantity - 20})  # 注意: 此时事务未提交

cc_cookie = query.first()
print(cc_cookie.quantity)

# 普通delete
query = session.query(Cookie)
query = query.filter(Cookie.cookie_name == "dark chocolate chip")
dcc_cookie = query.one()
session.delete(dcc_cookie)
session.commit()
dcc_cookie = query.first()
print(dcc_cookie)

query = session.query(Cookie)
query = query.filter(Cookie.cookie_name == "molasses")
query.delete()
mol_cookie = query.first()
print(mol_cookie)

cookiemon = User(username='cookiemon',
                 email_address='mon@cookie.com',
                 phone='111-111-1111',
                 password='password')
cakeeater = User(username='cakeeater',
                 email_address='cakeeater@cake.com',
                 phone='222-222-2222',
                 password='password')
pieperson = User(username='pieperson',
                 email_address='person@pie.com',
                 phone='333-333-3333',
                 password='password')
session.add(cookiemon)
session.add(cakeeater)
session.add(pieperson)
session.commit()

o1 = Order()
o1.user = cookiemon
session.add(o1)

cc = session.query(Cookie).filter(Cookie.cookie_name ==
                                  "chocolate chip").one()
line1 = LineItem(cookie=cc, order=o1, quantity=2, extended_cost=1.00)

pb = session.query(Cookie).filter(Cookie.cookie_name ==
                                  "peanut butter").one()
line2 = LineItem(quantity=12, extended_cost=3.00)
line2.cookie = pb
line2.order = o1

# o1.line_items.append(line1)
# o1.line_items.append(line2)
session.commit()

o2 = Order()
o2.user = cakeeater

cc = session.query(Cookie).filter(Cookie.cookie_name ==
                                  "chocolate chip").one()
line1 = LineItem(cookie=cc, order=o2, quantity=24, extended_cost=12.00)

oat = session.query(Cookie).filter(Cookie.cookie_name ==
                                   "oatmeal raisin").one()
line2 = LineItem(cookie=oat, order=o2, quantity=6, extended_cost=6.00)

# o2.line_items.append(line1)
# o2.line_items.append(line2)

session.add(o2)
session.commit()

# 多表查询
query = session.query(Order.order_id, User.username, User.phone,
                      Cookie.cookie_name, LineItem.quantity,
                      LineItem.extended_cost)
query = query.join(User).join(LineItem).join(Cookie)
results = query.filter(User.username == 'cookiemon').all()
print(results)

# group语句
query = session.query(User.username, func.count(Order.order_id))
query = query.outerjoin(Order).group_by(User.username)
for row in query:
	print(row)

# 自关联
marsha = Employee(name='Marsha')
fred = Employee(name='Fred')
marsha.reports.append(fred)
session.add(marsha)
session.commit()

for report in marsha.reports:
	print(report.name)

# 外关联
query = session.query(User.username, func.count(Order.order_id))
query = query.outerjoin(Order).group_by(User.username)
for row in query:
	print(row)


def get_orders_by_customer(cust_name):
	query = session.query(Order.order_id, User.username, User.phone,
	                      Cookie.cookie_name, LineItem.quantity,
	                      LineItem.extended_cost)
	query = query.join(User).join(LineItem).join(Cookie)
	results = query.filter(User.username == cust_name).all()
	return results


get_orders_by_customer('cakeeater')


# 查询字段动态扩展
def get_orders_by_customer(cust_name, shipped=None, details=False):
	query = session.query(Order.order_id, User.username, User.phone)
	query = query.join(User)
	if details:
		query = query.add_columns(Cookie.cookie_name, LineItem.quantity,
		                          LineItem.extended_cost)
		query = query.join(LineItem).join(Cookie)
	if shipped is not None:
		query = query.filter(Order.shipped == shipped)
	results = query.filter(User.username == cust_name).all()
	return results


print(get_orders_by_customer('cakeeater'))

print(get_orders_by_customer('cakeeater', details=True))

print(get_orders_by_customer('cakeeater', shipped=True))

print(get_orders_by_customer('cakeeater', shipped=False))

print(get_orders_by_customer('cakeeater', shipped=False, details=True))

# 原生sql
from sqlalchemy import text

query = session.query(User).filter(text("username='cookiemon'"))
print(query.all())

# session state
cc_cookie = Cookie(cookie_name='chocolate chip',
                   cookie_recipe_url='http://some.aweso.me/cookie/recipe.html',
                   cookie_sku='CC01',
                   quantity=12,
                   unit_cost=0.50)

from sqlalchemy import inspect

insp = inspect(cc_cookie)

for state in ['transient', 'pending', 'persistent', 'detached']:
	print('{:>10}: {}'.format(state, getattr(insp, state)))

session.add(cc_cookie)
for state in ['transient','pending','persistent','detached']:
	print('{:>10}: {}'.format(state, getattr(insp, state)))

session.commit()
for state in ['transient','pending','persistent','detached']:
	print('{:>10}: {}'.format(state, getattr(insp, state)))

session.expunge(cc_cookie)
for state in ['transient','pending','persistent','detached']:
	print('{:>10}: {}'.format(state, getattr(insp, state)))

session.add(cc_cookie)
cc_cookie.cookie_name = 'Change chocolate chip'
print(insp.modified)

for attr, attr_state in insp.attrs.items():
	if attr_state.history.has_changes():
		print('{}: {}'.format(attr, attr_state.value))
		print('History: {}\n'.format(attr_state.history))

try:
	results=session.query(Cookie).one()
except:
	print('We found too many cookies... is that even possible?')

try:
	results=session.query(Cookie).filter_by(cookie_name='notexists').one()
except:
	print('We found no cookies... is that even possible?')

lineItem = session.query(LineItem).first()
session.expunge(lineItem)
try:
	print(lineItem.order)
except:
	print('line item has detached from db')

from sqlalchemy.ext.automap import automap_base
Base = automap_base()
Base.prepare(engine, reflect=True)
print(Base.classes.keys())

if isMySql:
	Cookie=Base.classes.mysql_cookies

for cookie in session.query(Cookie).limit(10):
	print(cookie.cookie_id, cookie.cookie_name)