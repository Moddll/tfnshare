import datetime
from sqlalchemy import func
from flask import render_template, Blueprint
from flask import Blueprint, redirect, url_for
from flask import jsonify


from webapp.models import db, CorpList, CseStock

main_blueprint = Blueprint(
    'main',
    __name__,
    template_folder='../templates'
)


@main_blueprint.route('/')
def index():
    # corplist = CorpList.query.order_by(CorpList.list_date.desc())
    corplist = db.session.query(CorpList).order_by(CorpList.list_date.desc())
    temp = []
    for x in corplist:
        temp.append(x.to_json())
    # jsonify(object=temp) 方法参考 http://lazybios.com/2015/06/cover-sqlalchemy-result-to-json/

    return render_template(
        'home.html',
        corplist=temp,
    )


@main_blueprint.route('/p_cse')
def policy():
    csestock = db.session.query(CseStock).order_by(CseStock.Date.desc())
    temp1 = []
    for x in csestock:
        temp1.append(x.to_json())

    return render_template(
        'p_cse.html',
        csestock=temp1,
        # csestock=csestock,
    )



