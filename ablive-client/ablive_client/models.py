from sqlalchemy import BigInteger, Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class AbliveDanmaku(Base):
    __table_args__ = {
        "mysql_charset": "utf8mb4",
        "schema": "ablive_dm",
    }
    __tablename__ = "undefined"

    ts = Column(Integer, nullable=False)
    uid = Column(BigInteger, nullable=False)
    uname = Column(String(32), nullable=False)
    liverid = Column(BigInteger, nullable=False)
    text = Column(String(127), nullable=False)
    _id = Column(Integer, primary_key=True, autoincrement=True)


class AbliveEntry(Base):
    __table_args__ = {
        "mysql_charset": "utf8mb4",
        "schema": "ablive_en",
    }
    __tablename__ = "undefined"

    ts = Column(Integer, nullable=False)
    uid = Column(BigInteger, nullable=False)
    uname = Column(String(32), nullable=False)
    liverid = Column(BigInteger, nullable=False)
    _id = Column(Integer, primary_key=True, autoincrement=True)


class AbliveGift(Base):
    __table_args__ = {
        "mysql_charset": "utf8mb4",
        "schema": "ablive_gf",
    }
    __tablename__ = "undefined"

    ts = Column(Integer, nullable=False)
    uid = Column(BigInteger, nullable=False)
    uname = Column(String(32), nullable=False)
    liverid = Column(BigInteger, nullable=False)
    gift_info = Column(String(32), nullable=False)
    gift_cost = Column(Numeric(7, 1), nullable=False)
    _id = Column(Integer, primary_key=True, autoincrement=True)


class AbliveSuperChat(Base):
    __table_args__ = {
        "mysql_charset": "utf8mb4",
        "schema": "ablive_sc",
    }
    __tablename__ = "undefined"

    ts = Column(Integer, nullable=False)
    uid = Column(BigInteger, nullable=False)
    uname = Column(String(32), nullable=False)
    liverid = Column(BigInteger, nullable=False)
    text = Column(String(127), nullable=False)
    sc_price = Column(Numeric(7, 1), nullable=False)
    _id = Column(Integer, primary_key=True, autoincrement=True)
