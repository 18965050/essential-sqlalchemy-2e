from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///:memory:')

Session = sessionmaker(bind=engine)

from datetime import datetime

from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method

Base = declarative_base()


class Cookie(Base):
	__tablename__ = 'cookies'

	cookie_id = Column(Integer, primary_key=True)
	cookie_name = Column(String(50), index=True)
	cookie_recipe_url = Column(String(255))
	cookie_sku = Column(String(55))
	quantity = Column(Integer())
	unit_cost = Column(Numeric(12, 2))

	@hybrid_property
	def inventory_value(self):
		return self.unit_cost * self.quantity

	@hybrid_method
	def bake_more(self, min_quantity):
		return self.quantity < min_quantity

	def __repr__(self):
		return "Cookie(cookie_name='{self.cookie_name}', " \
		       "cookie_recipe_url='{self.cookie_recipe_url}', " \
		       "cookie_sku='{self.cookie_sku}', " \
		       "quantity={self.quantity}, " \
		       "unit_cost={self.unit_cost})".format(self=self)


Base.metadata.create_all(engine)

print(Cookie.inventory_value < 10.00)

print(Cookie.bake_more(12))

session = Session()
cc_cookie = Cookie(cookie_name='chocolate chip',
                   cookie_recipe_url='http://some.aweso.me/cookie/recipe.html',
                   cookie_sku='CC01',
                   quantity=12,
                   unit_cost=0.50)
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
session.add(cc_cookie)
session.add(dcc)
session.add(mol)
session.flush()

print(dcc.inventory_value)
print(dcc.bake_more(12))

from sqlalchemy import desc
for cookie in session.query(Cookie).order_by(desc(Cookie.inventory_value)):
	print('{:>20} - {:.2f}'.format(cookie.cookie_name, cookie.inventory_value))

for cookie in session.query(Cookie).filter(Cookie.bake_more(12)):
    print('{:>20} - {}'.format(cookie.cookie_name, cookie.quantity))