from dash import dcc, register_page, html, callback, Input, Output
import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import polars as pl

from libs.db import DB
from libs.stocks import Stocks
from libs.config import DATABASE_PATH

register_page(__name__, title='Historical Data')

def layout(**kwargs):
    stocks = Stocks(DB(DATABASE_PATH))
    return html.Div([
    html.H1('Stock Historical Quotes', style={"text-align": "center", "margin": "30px 0px 0px 0px"}),
    html.P('Select the stock and the period to display the historical chart.', style={"margin": "0px 20px 0px 20px"}),
    html.Div([
        dbc.Row([
            dbc.Col(
                dbc.Stack(
                    [
                        html.Div(id='stocks-card'),
                        dcc.Dropdown(
                            options=[s[0] for s in stocks.list_stocks()],
                            value="PETR3.SA",
                            # multi=True,
                            id="stocks-dropdown"
                        ),
                        dcc.Dropdown(
                            options=["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "ytd", "max"],
                            value='1y',
                            id='stocks-period'
                        ),
                        html.P('Avg Mean:', style = {'display': 'flex', "margin":"8px"}),
                        dcc.Checklist(
                            options=["15d", "30d", "60d"],
                            value=[],
                            inline=True,
                            id='avg-mean',
                            labelStyle= {"margin":"8px"}, style = {'display': 'flex'}
                        ),
                        html.P('Adjusted chart:', style = {'display': 'flex', "margin":"8px"}),
                        dcc.RadioItems(
                            options=["Yes", "No"],
                            value='No',
                            inline=True,
                            id='adjusted-show',
                            labelStyle= {"margin":"8px"}, style = {'display': 'flex'}
                        ),
                        html.P('Stock forecast:', style = {'display': 'flex', "margin":"8px"}),
                        dcc.RadioItems(
                            options=["Yes", "No"],
                            value='No',
                            inline=True,
                            id='forecast-show',
                            labelStyle= {"margin":"8px"}, style = {'display': 'flex'}
                        ),
                    ],
                    gap=1,
                    style={"margin": "0px 20px 0px 20px"}
                ),
                width=2
            ),
            dbc.Col(
                dbc.Stack([
                    html.Div(id='stocks-chart'),
                    html.Div(id='adjusted-chart'),
                ]),
                width=10
            )   
        ]),
    ]),
])


@callback(
    Output('stocks-chart', 'children'),
    Output('stocks-card', 'children'),
    Input('stocks-dropdown', 'value'),
    Input('stocks-period', 'value'),
    Input('avg-mean', 'value'),
)
def update_stocks_chart(dropdown, period, avg_mean):
    stocks = Stocks(DB(DATABASE_PATH))
    df = stocks.get_stocks(dropdown, period)
    statistics = stocks.get_statistics_by_period(dropdown, period)
    fig = go.Figure(data=[go.Candlestick(x=df['Date'],
            open=df['Open'], high=df['High'],
            low=df['Low'], close=df['Close'],
            name=dropdown)
        ])
    for avg in avg_mean:
        df_rolling = df.sort('Date').rolling(
            index_column='Date',
            period=avg
        ).agg(pl.col("Close").mean().alias("Close"))

        fig.add_trace(
            go.Scatter(x=df_rolling['Date'], y=df_rolling['Close'], mode='lines', name=avg)
        )
    
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
    chart = dcc.Graph(figure=fig, style={'height': '600px'})
    quote_variation = statistics['Close']/statistics['Open']*100-100
    card = dbc.Card(
        [
            # dbc.CardImg(src="/static/images/placeholder286x180.png", top=True),
            dbc.CardBody(
                [
                    html.H4(dropdown, style={'float': 'left'}),
                    html.H4(f"{quote_variation:.1f}%", style={"color": "red" if quote_variation < 0 else "green", "font-weight": "bold", 'float': 'right'}),
                    html.Br(),
                    html.Br(),
                    html.Span("Dividend Yield:"), html.Span(f"{statistics['Dividend_yield']*100:.2f}%", style={"color": "red" if statistics['Dividend_yield'] < 0 else "green", "font-weight": "bold" , 'float': 'right'}),
                    html.Br(),
                    html.Span("Dividends:"), html.Span(f"R$ {statistics['Dividends']:.2f}", style={"font-weight": "bold", 'float': 'right'}),
                    html.Br(),
                    html.Span("Open Price:"), html.Span(f"R$ {statistics['Open']:.2f}", style={"font-weight": "bold", 'float': 'right'}),
                    html.Br(),
                    html.Span("Close Price:"), html.Span(f"R$ {statistics['Close']:.2f}", style={"font-weight": "bold", 'float': 'right'}),
                ]
            ),
        ],
    )

    return chart, card

@callback(
    Output('adjusted-chart', 'children'),
    Input('stocks-dropdown', 'value'),
    Input('stocks-period', 'value'),
    Input('adjusted-show', 'value'),
)
def update_dividends_chart(dropdown, period, radio):
    if radio == "Yes":
        stocks = Stocks(DB(DATABASE_PATH))
        df = stocks.get_stocks(dropdown, period)

        fig = go.Figure(data=[go.Scatter(x=df['Date'], y=df['Close'], name=dropdown, mode='lines')])
        fig.add_trace(go.Scatter(x=df['Date'], y=df['Adj Close'], name='Adjusted Price', mode='lines'))
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
        # fig.add_trace(go.Scatter(x=df['Date'], y=df['Dividends'].cum_sum(), name='Dividends Gain', mode='lines'))
        return dcc.Graph(figure=fig, style={'height': '600px'})
    else:
        return None