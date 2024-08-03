import dash
from dash import dcc, register_page, html, callback, Input, Output, State, dash_table, set_props
import dash_bootstrap_components as dbc
import logging
import polars as pl
from datetime import date

from libs.db import DB
from libs.stocks import Stocks
from libs.config import DATABASE_PATH

register_page(__name__, title='Stocks Management')

app = dash.get_app()
log = logging.getLogger()

layout = html.Div([
    html.H1('Stocks Management', style={"text-align": "center", "margin": "30px 0px 0px 0px"}),
    html.P('Add stocks to database', style={"margin": "0px 20px 0px 20px"}),
    html.Div([
        dbc.Row([
            dbc.Col([
                html.Div(
                    dbc.Stack([
                        dbc.Input(placeholder="Add stock to database", type="text", id="stocks-to-add"),
                        html.Br(),
                        dbc.Button("Add stock", color="dark", className="me-1", id="add-button"),
                        html.Br(),
                        dbc.Button("Update stock database", color="dark", className="me-1", id="update-button"),
                        html.Br(),
                        # html.Progress(id="progress-bar"),
                    ]),
                )
            ], width=2, style={"margin": "0px 20px 0px 20px"}),
            dbc.Col([
                html.Div(id="stocks-data-table"),
            ],
            width=2),
            dbc.Col([
                html.Div(
                    dbc.Stack([
                        dbc.Input(placeholder="Stock to portifolio", type="text", id="stocks-to-portifolio"),
                        html.Br(),
                        dbc.Input(placeholder="Number of stocks", type="text", id="number-of-stocks"),
                        html.Br(),
                        dbc.Input(placeholder="Price at buy", type="text", id="price-at-buy"),
                        html.Br(),
                        html.Span("Aquisition date: "),
                        dcc.DatePickerSingle(
                            id='stocks-date-picker',
                            date=date.today()
                        ),
                        html.Br(),
                        dbc.Button("Add stock", color="dark", className="me-1", id="add-to-portifolio-button"),
                        html.Br(),
                        dbc.Button("Delete selected stock", color="dark", className="me-1", id="delete-from-portifolio-button"),
                    ]),
                )
            ], width=2, style={"margin": "0px 20px 0px 20px"}),
            dbc.Col([
                html.Div(
                    dash_table.DataTable(
                        id='portifolio-table',
                        data=[],
                        page_size=20,
                        page_action='native',
                        filter_action='native',
                        sort_action='native',
                        style_table={'height': '900px'},
                        style_cell={
                            'minWidth': 95, 'maxWidth': 200, 'width': 95, 'textAlign': 'center'
                        },
                        row_selectable='single',
                    )
                ),
            ],
            width=5),
        ]),
    ])
])


@callback(
    Output("stocks-data-table", "children"), 
    Input("add-button", "n_clicks"),
    State("stocks-to-add", "value")
)
def add_stock_database(n, stocks_to_add):
    stocks = Stocks(DB(DATABASE_PATH))

    if not n is None:
        stocks_to_add = f"{stocks_to_add}.SA" if not stocks_to_add.endswith(".SA") else stocks_to_add
        stocks.add_stocks(stocks_to_add)

    stocks_list = stocks.list_stocks()
    df = pl.DataFrame(stocks_list, schema=[("Stocks in Database", pl.Utf8)])
    table = dash_table.DataTable(
        data=df.to_dicts(),
        columns=[{'id': c, 'name': c} for c in df.columns],
        style_table={'height': '900px'},
        style_cell={
            'minWidth': 95, 'maxWidth': 500, 'width': 95, 'textAlign': 'center'
        },
        page_size=20,
        page_action='native',
        filter_action='native',
        sort_action='native',
    )

    return table
    
@callback(
    Output("portifolio-table", "data"),
    Output("portifolio-table", "columns"),
    Input("add-to-portifolio-button", "n_clicks"),
    Input("delete-from-portifolio-button", "n_clicks"),
    State("stocks-to-portifolio", "value"),
    State("number-of-stocks", "value"),
    State("price-at-buy", "value"),
    State("stocks-date-picker", "date"),
    State("portifolio-table", "selected_rows")
)
def add_stocks_to_portifolio(n1, n2, stocks_to_portifolio, number_of_stocks, price_at_buy, date, selected_rows):
    stocks = Stocks(DB(DATABASE_PATH))
    if dash.callback_context.triggered[0]['prop_id'] == "delete-from-portifolio-button.n_clicks":
        if n2 is not None or selected_rows is not None:
            df = stocks.get_portifolio().reset_index(drop=True)
            ticker = df.loc[selected_rows[0], "Ticker"]
            stocks.delete_stock_from_portifolio(ticker)

    if dash.callback_context.triggered[0]['prop_id'] == "add-to-portifolio-button.n_clicks":
        if  n1 is not None and stocks_to_portifolio != "" and number_of_stocks  != "" and price_at_buy  != "" and date is not None:
            stocks_to_portifolio = f"{stocks_to_portifolio}.SA" if not stocks_to_portifolio.endswith(".SA") else stocks_to_portifolio
            date = f"{date} 00:00:00" # keep all dates in the same format
            stocks.insert_portifolio(stocks_to_portifolio, number_of_stocks, price_at_buy, date)

    df = stocks.get_portifolio()

    set_props("stocks-to-portifolio", {"value": ""})
    set_props("number-of-stocks", {"value": ""})
    set_props("price-at-buy", {"value": ""})
    return df.to_dicts() ,[{'id': c, 'name': c} for c in df.columns]

# @app.long_callback(
#     inputs=Input("update-button", "n_clicks"),
#     running=[
#         (
#             Output("progress-bar", "style"),
#             {"visibility": "hidden"},
#             {"visibility": "visible"},
#         ),
#     ],
#     progress=[
#         Output("progress-bar", "value"), Output("progress-bar", "max")
#     ],
# )
# def update_stocks_database(set_progress, n):
#     stocks = Stocks(DB(DATABASE_PATH))
#     if not n is None:
#         for i, size in stocks.update_stocks():
#             set_progress((str(i), str(size)))

#     return None
