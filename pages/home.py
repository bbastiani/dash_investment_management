from dash import register_page, html, dcc
from dash.dash_table import DataTable, FormatTemplate
from dash.dash_table.Format import Format, Scheme, Group, Symbol
import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import polars as pl

from libs.db import DB
from libs.stocks import Stocks
from libs.config import DATABASE_PATH

register_page(__name__, path='/')

money_format = Format(
    scheme=Scheme.fixed, 
    precision=2,
    group=Group.yes,
    groups=3,
    group_delimiter='.',
    decimal_delimiter=',',
    symbol=Symbol.yes, 
    symbol_prefix=u'R$'
)

def layout(**kwargs):
    stocks = Stocks(DB(DATABASE_PATH))
    df = stocks.get_portifolio()

    statistics = []

    for stock in df.iter_rows(named=True):
        stats = stocks.get_statistics_by_buy_date(stock['Ticker'], stock['Date'], stock['Price at Buy'], return_dict=True)
        stats["Price at Buy"] = stock['Price at Buy']
        stats["N Stocks"] = stock['Number of Stocks']
        stats["Buy Date"] = stock['Date'].strftime("%d/%m/%Y")
        stats["Div Yield"] = stats["Dividend_yield"] 
        stats["Close Price"] = stats["Close"]
        stats["Price %"] = stats["Price_variation"]
        stats["Ticker"] = stock['Ticker']
        statistics.append(stats)

    cols = ["Ticker", "Price %", "Close Price", "Price at Buy", "Dividends", "Div Yield", "N Stocks", "Buy Date"]
    formatters = {
        "Price %": FormatTemplate.percentage(2),
        "Div Yield": FormatTemplate.percentage(2),
        "Close Price": money_format,
        "Price at Buy": money_format,
        "Dividends": money_format,
    }
    data = [
        {k: v for k, v in s.items() if k in cols}
        for s in statistics
    ]
    cols = [
        dict(name=i, id=i, type="numeric", format=formatters[i])
        if i in formatters else {"name": i, "id": i}
        for i in cols 
    ]

    data_table_style = [
        {
            'if': {
                'filter_query': f'{{{col["id"]}}} < 0',
                'column_id': col['id']
            },
            'backgroundColor': 'white',
            'color': 'red'
        } for col in cols if "Price %" == col["id"]
    ]

    data_table_style.extend([
        {
            'if': {
                'filter_query': f'{{{col["id"]}}} > 0',
                'column_id': col['id']
            },
            'backgroundColor': 'white',
            'color': 'green'
        } for col in cols if "Price Variation" == col["id"]
    ])
    d_table = DataTable(
            id='portifolio-table',
            data=data,
            columns=cols,
            page_size=20,
            page_action='native',
            filter_action='native',
            sort_action='native',
            style_table={'height': '900px'},
            style_cell={
                'textAlign': 'center', 'fontSize':14
            },
            style_data_conditional=data_table_style,
        )
    # bar chart with dividends and price variation
    data = stocks.get_monthly_portifolio_statistics()
    data = data.sort("Date", descending=False)
    data = data.group_by_dynamic(
            "Date", every="1mo", period="1mo", closed="right"
        ).agg(pl.sum("Dividends"), pl.sum("Price Variation Diff")).select("Date", "Dividends", "Price Variation Diff")

    fig = go.Figure(data=[
        go.Bar(x=data['Date'], y=data['Dividends'], name="Dividends"),
        go.Bar(x=data['Date'], y=data['Price Variation Diff'], name="Price Variation")
    ])

    fig.update_layout(
        margin=dict(l=40, r=40, t=10, b=10),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=0.02,
            xanchor="right",
            x=1
        )
    )

    return html.Div(
        dbc.Row([
            dbc.Col([
                d_table
            ], width=6),
            dbc.Col([
                dcc.Graph(figure=fig, style={'height': '500px'})
            ], width=6)
        ], style={"margin": "20px 20px 20px 20px"})
    )