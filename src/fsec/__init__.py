# pylint: disable=unused-argument


from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from config import APP_SETTINGS, get_active_config
import loggers

loggers.config()

conf = get_active_config()

# instantiate the extensions
db = SQLAlchemy()


def create_app(script_info=None):
    app = Flask(__name__)
    app.config.from_object(APP_SETTINGS)

    # set up extensions
    db.init_app(app)
    db.reflect(app=app)

    # with app.app_context():
    # db.Model.metadata.reflect(bind=db.engine, schema=conf.DATABASE_SCHEMA)

    # shell context for flask cli
    @app.shell_context_processor
    def ctx():  # pylint: disable=unused-variable
        return {"app": app, "db": db}

    return app

