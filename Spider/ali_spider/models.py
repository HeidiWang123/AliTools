from sqlalchemy import Integer, String, Date, Boolean
from sqlalchemy import Column, ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Product(Base):
    """docstring for Product."""

    __tablename__ = "products"

    id = Column('id', Integer, primary_key=True, autoincrement=False)
    style_no = Column('style_no', String)
    title = Column('title', String)
    keywords = Column('keywords', String)
    owner = Column('owner', String)
    modify_time = Column('modify_time', Date)
    is_trade_product = Column('is_trade_product', Boolean)
    is_window_product = Column('is_window_product', Boolean)

    rank = relationship("Rank")

class Keyword(Base):
    """docstring for Keyword."""

    __tablename__ = "keywords"

    value = Column('value', String, primary_key=True)
    repeat_keyword = Column('repeat_keyword', String)
    company_cnt = Column('company_cnt', Integer)
    srh_pv = Column('srh_pv', String)
    update = Column('update', Date)
    is_p4p_keyword = Column('is_p4p_keyword', Boolean)

    rank = relationship("Rank")

class Rank(Base):
    """docstring for Rank."""

    __tablename__ = "rank"

    id = Column('id', Integer, primary_key=True)
    keyword_value = Column('keyword_value', String, ForeignKey('keywords.value'))
    product_id = Column('product_id', Integer, ForeignKey('products.id'))
    ranking = Column('ranking', Integer)

    keyword = relationship('Keyword', back_populates="rank")
    product = relationship('Product', back_populates="rank")

class Database():
    session = None
    def __init__(self):
        self._init_db()

    def _init_db(self):
        if Database.session is not None:
            return

        engine = create_engine('sqlite:///./database/data.db', echo=True)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        Database.session = Session()

    def add_products(self, products):
        self.session.add_all(products)
        self.session.commit()

    def get_product_keywords(self):
        keywords = set()
        for keyword in self.session.query(Product.keywords):
            keywords.update(keyword.split(','))
        return keywords

    def close(self):
        self.session.commit()
        self.session.close()
