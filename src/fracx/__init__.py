from flask import Flask
from flask_sqlalchemy import SQLAlchemy


import loggers
from config import APP_SETTINGS, get_active_config


loggers.config()

conf = get_active_config()

# instantiate the extensions
db = SQLAlchemy()


def create_app(script_info=None):
    app = Flask(__name__)
    app.config.from_object(APP_SETTINGS)

    # set up extensions
    db.init_app(app)

    # shell context for flask cli
    @app.shell_context_processor
    def ctx():
        return {"app": app, "db": db}

    return app
