class Config(object):
    SECRET_KEY = '123546'


class ProdConfig(Config):
    SQLALCHEMY_DATABASE_URI = 'sqlite:///../findata/cse.db'


class DevConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///../findata/cse.db'