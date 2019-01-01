from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class CorpList(db.Model):
    __tablename__ = 'cselist'

    symbol = db.Column(db.String, primary_key=True)
    company = db.Column(db.String)
    industry = db.Column(db.String)
    list_date = db.Column(db.String)

    def to_json(self):
        return {
            'symbol': self.symbol,
            'company': self.company,
            'industry': self.industry,
            'list_date': self.list_date
        }


class CseStock(db.Model):
    __tablename__ = 'get'

    Date = db.Column(db.String(10), primary_key=True)
    Open = db.Column(db.Float)
    High = db.Column(db.Float)
    Low = db.Column(db.Float)
    Close = db.Column(db.Float)
    # Adj Close = db.Column(db.Float)
    Volume = db.Column(db.Float)

    def to_json(self):
        return {
            'Date': self.Date,
            'Open': self.Open,
            'High': self.High,
            'Low': self.Low,
            'Close': self.Close,
            'Volume': self.Volume
        }


