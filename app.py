import logging
import argparse

from dash import html

import dash
import dash_bootstrap_components as dbc

from dash.long_callback import DiskcacheLongCallbackManager

## Diskcache
import diskcache
cache = diskcache.Cache("./cache")
long_callback_manager = DiskcacheLongCallbackManager(cache)

def main(args):
    logging.basicConfig(
        filename=args.log_path,
        format='%(asctime)s - %(filename)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    log = logging.getLogger()
    log.info("Start program")

    app = dash.Dash(__name__,
                external_stylesheets=[dbc.themes.BOOTSTRAP],
                suppress_callback_exceptions=True,
                update_title=None,
                use_pages=True,
                long_callback_manager=long_callback_manager)

    server = app.server

    navbar = dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink(f"{page['name']}", href=page["relative_path"])) for page in dash.page_registry.values() 
        ],
        brand="Stocks Portfolio",
        color="dark",
        dark=True, 
        className="mb-2",       
    )
    
    app.layout = html.Div([
        navbar,
        dash.page_container
    ])

    return  app, server


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Stocks")
    parser.add_argument("--log-path", type=str, default="stocks.log", help="Path to log file")
    parser.add_argument("--db-path", type=str, default="stocks.db", help="Path to database file")
    parser.add_argument("--port", type=int, default=5000, help="Port ")
    parser.add_argument("--addr", type=str, default="127.0.0.1", help="Server address")
    args = parser.parse_args()

    app, server = main(args)
    app.run(
        port=args.port,
        host=args.addr,
        debug=True
    )
