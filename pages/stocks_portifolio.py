from dash import dcc, register_page, html, callback, Input, Output, State, dash_table
import dash_bootstrap_components as dbc

from libs.db import DB
from libs.stocks import Stocks
from libs.config import DATABASE_PATH

register_page(__name__, title='Stocks Portifolio')

def layout(**kwargs):
    stocks = Stocks(DB(DATABASE_PATH))
    df = stocks.get_portifolio()

    cards = []
    for stock in df.iter_rows(named=True):
        stats = stocks.get_statistics_by_buy_date(stock['Ticker'], stock['Date'], return_dict=True)
        num_stocks = stock['Number of Stocks']
        quote_variation = stats['Close'] / stock['Price at Buy'] * 100 - 100
        gain = (stats['Close'] - stock['Price at Buy']) * num_stocks
        cards.append(dbc.Card(dbc.CardBody([
            html.H4(stock['Ticker'], style={'float': 'left'}),
            html.H4(f"{quote_variation:.1f}%", style={"color": "red" if quote_variation < 0 else "green", "font-weight": "bold", 'float': 'right'}),
            html.Br(),
            html.Br(),
            html.Br(),
            html.Span("Number of Stocks:"), html.Span(f" {stock['Number of Stocks']}", style={"font-weight": "bold", "margin-right": "0rem", 'float': 'right'}),
            html.Br(),
            html.Span("Price at Buy:"), html.Span(f" R$ {stock['Price at Buy']:.2f}", style={"font-weight": "bold", "margin-right": "0rem", 'float': 'right'}),
            html.Br(),
            html.Span("Price:"), html.Span(f"R$ {stats['Close']:.2f}", style={"font-weight": "bold", "margin-right": "0rem", 'float': 'right'}),
            html.Br(),
            html.Span("Buy Date:"), html.Span(f" {stock['Date'].strftime('%d/%m/%Y')}", style={"font-weight": "bold", "margin-right": "0rem", 'float': 'right'}),
            html.Br(),
            html.Span("Loose/Gain:"), html.Span(f"R$ {gain:.2f}", style={"color": "red" if gain < 0 else "green", "font-weight": "bold", "margin-right": "0rem", 'float': 'right'}),
            html.Br(),
            html.Span("Dividend Yield:"), html.Span(f"{stats['Dividend_yield']*100:.2f}%", style={"color": "red" if stats['Dividend_yield'] < 0 else "green", "font-weight": "bold" , "margin-right": "0rem", 'float': 'right'}),
            html.Br(),
            html.Span("Dividends:"), html.Span(f"R$ {stats['Dividends']*num_stocks:.2f}", style={"font-weight": "bold", "margin-right": "0rem", 'float': 'right'}),
        ])))

    num_rows = len(cards) // 6 + 1
    num_cols = 6

    cards = [cards[i*num_cols:(i+1)*num_cols] for i in range(num_rows)]
    cards = [dbc.Row([dbc.Col(card, width=2) for card in row], style={"margin": "0px 20px 0px 20px"}) for row in cards]
    # insert a space between rows
    num_spaces = num_rows // 2 + 1
    
    for i in range(1, num_spaces + 1):
        if i == 1:
            cards.insert(i, html.Br())
        else:
            cards.insert(i + 2, html.Br())

    return html.Div(id='portifolio-content', children=cards)