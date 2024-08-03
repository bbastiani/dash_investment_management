from dash import register_page, html
from dash.dash_table import DataTable, FormatTemplate
from dash.dash_table.Format import Format, Scheme, Group, Symbol

from libs.db import DB
from libs.stocks import Stocks
from libs.config import DATABASE_PATH

from tqdm import tqdm
import logging

register_page(__name__, title='Stocks Portifolio')

log = logging.getLogger()

def layout(**kwargs):
    stocks = Stocks(DB(DATABASE_PATH))
    all_stocks = stocks.list_stocks()

    statistics = []

    progress_bar = tqdm(all_stocks)
    for stock in progress_bar:
        log.info(str(progress_bar))
        stock = stock[0]
        stats = stocks.get_statistics_all_periods(stock, ["1y", "2y", "5y"], True)

        statistics.append({
            "ticker": stock,
            "dividends": {s['Period']:s['Dividends'] for s in stats},
            "dividend_yield": {s['Period']:s['Dividend_yield'] for s in stats},
            "price": {s['Period']:s['Close'] for s in stats},
            "price_variation": {s['Period']:s['Price_variation']/100 for s in stats},
        })

    cols = []
    for k,v in statistics[0].items():
        if isinstance(v, dict):
            for k1,v1 in v.items():
                if k == "dividends"  or k == "price":
                    cols.append(dict(name=[k, k1], id=f"{k}_{k1}", type="numeric", format=Format(
                                                                                        scheme=Scheme.fixed, 
                                                                                        precision=2,
                                                                                        group=Group.yes,
                                                                                        groups=3,
                                                                                        group_delimiter='.',
                                                                                        decimal_delimiter=',',
                                                                                        symbol=Symbol.yes, 
                                                                                        symbol_prefix=u'R$')))
                elif k == "price_variation" or k == "dividend_yield":
                    cols.append(dict(name=[k, k1], id=f"{k}_{k1}", type="numeric", format=FormatTemplate.percentage(2)))
                else:
                    cols.append({"name": [k, k1], "id": f"{k}_{k1}"})
                
        else:
            cols.append({"name": k, "id": k})

    data = []
    for s in statistics:
        d = {}
        for k, v in s.items():
            if isinstance(v, dict):
                for k1, v1 in v.items():
                        d[f"{k}_{k1}"] = v1
            else:
                d[k] = v
        data.append(d)

    data_table_style = [
        {
            'if': {
                'filter_query': f'{{{col["id"]}}} < 0',
                'column_id': col['id']
            },
            'backgroundColor': 'white',
            'color': 'red'
        } for col in cols if "price_variation" in col["id"]
    ]

    data_table_style.extend([
        {
            'if': {
                'filter_query': f'{{{col["id"]}}} > 0',
                'column_id': col['id']
            },
            'backgroundColor': 'white',
            'color': 'green'
        } for col in cols if "price_variation" in col["id"]
    ])


    return html.Div(
            DataTable(
                data=data,
                columns=cols,
                style_table={'height': '900px'},
                style_cell={
                    'minWidth': 95, 'maxWidth': 500, 'width': 95, 'textAlign': 'center'
                },
                merge_duplicate_headers=True,
                style_data_conditional=data_table_style,
                filter_action="native",
                sort_action="native",
                page_action="native",
                page_size=50,
            )
        )
