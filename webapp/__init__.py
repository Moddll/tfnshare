from flask import Flask

from webapp.models import db
from webapp.controllers.main import main_blueprint


def create_app(object_name):
    """

    :argument object_name: the python path of the config object
    :return: app
    """

    app = Flask(__name__)
    app.config.from_object(object_name)

    db.init_app(app)

    app.register_blueprint(main_blueprint)

    return app


if __name__ == "__main__":
    app = create_app('project.config.DevConfig')
    app.run()


