# Policy:  Check Unusual Daily Volume
import dash

from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_table

import pandas as pd

from stock.tfnstock import RwDatabase
from stock.tfnstock import FormatStockData

# set database
db = '../findata/cse.db'
dbc = RwDatabase(db)
file = '../stock/policy/madata.csv'

# Get Company List
sqlflt = "select * from companylist;"
lstCompany = dbc.read_sqldata(sqlflt)
symbol_array = lstCompany['symbol']

dp_options_list = []

for i, row in lstCompany.iterrows():
    # {'label': 'company', 'value': 'symbol'}
    mydict={}
    mydict['label'] = row['company']
    mydict['value'] = row['symbol']
    dp_options_list.append(mydict)

# Get moving average data from database
# sqlflt = "select * from daily_ma  ORDER BY v_change DESC, volume ASC;"
# data = dbc.read_sqldata(sqlflt)

#  Get moving average data from csv file
data = pd.read_csv(file, sep='\t', encoding='utf-8', index_col=False)

# Cleaning, sort and format dataframe
data = data[data.change > 0]
data = data.sort_values(['v_change', 'change'], ascending=[False, False])
data['volume'] = data['volume'].map('{:,.0f}'.format)
data['v_change'] = data['v_change'].map('{:,.0f}'.format)
data['v_ma5'] = data['v_ma5'].map('{:,.0f}'.format)
data['v_ma10'] = data['v_ma10'].map('{:,.0f}'.format)
data['v_ma20'] = data['v_ma20'].map('{:,.0f}'.format)


# Dash App

app = dash.Dash(__name__)

dash_head = html.Div([
    html.H2('Daily Volumes Strategy'),
            dcc.Markdown(''' --- '''),
            # html.Div([
            #     html.H3('Enter a stock symbol:', style={'paddingRight': '30px'}),
            #     dcc.Dropdown(
            #               id='my_ticker_symbol',
            #               options=dp_options_list,
            #               value=['SPY'],
            #               multi=True,
            #               style={'display': 'inline-block', 'fontSize': 24, 'width': 75}
            #     ),
            # ], style={'display': 'inline-block', 'verticalAlign': 'top', 'width': '30%'})
], className='row')

dash_table = html.Table([
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in data.columns],
        style_cell={'padding': '5px'},
        style_header={
            'backgroundColor': 'white',
            'fontWeight': 'bold',
            'fontsize': '18px'
        },
        style_cell_conditional=[
            {
                'if': {'column_id': 'date'},
                # 'width': '15px',
                'textAlign': 'left'
            },
            {
                'if': {'column_id': 'company'},
                'textAlign': 'left'
            },
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(220,238,244)'
            }
        ],
        data=data.to_dict("rows"),
        n_fixed_rows=1,
        # editable=True,
        # filtering=True,
        sorting=True,
        # sorting_type="multi",
        row_selectable="single",
        # row_deletable=True,
        selected_rows=[],
        style_table={
            'maxHeight': '290',
        },
        content_style='fit',
        style_as_list_view=True,
    ),
    html.Div(id='datatable-interactivity-container'),
    dcc.Markdown(''' --- ''')
], className='table')

app.layout = html.Div([dash_head, dash_table], className='container')


@app.callback(
    Output('datatable-interactivity-container', "children"),
    [Input('table', "derived_virtual_data"),
     Input('table', "derived_virtual_selected_rows")])
def update_graph(rows, derived_virtual_selected_rows):
    # if derived_virtual_selected_rows is None:
    #     derived_virtual_selected_rows = []

    if rows is None:
        pass
        # return
        # dff = '' # data
    elif derived_virtual_selected_rows:
        selected_symbol = rows[derived_virtual_selected_rows[0]]['symbol']
        plot_title = rows[derived_virtual_selected_rows[0]]['company']
        # Get stock data from database
        madata = pd.DataFrame()
        formatdata = FormatStockData(db)
        madata = formatdata.ma_stockdata(selected_symbol)
        madata['color_label'] = 'green'
        madata.loc[madata.close < madata.open, 'color_label'] = 'red'
        madata.loc[madata.close >= madata.open, 'color_label'] = 'green'

    trace1 = {
        'x': madata.date,
        'open': madata.open,
        'high': madata.high,
        'low':  madata.low,
        'close': madata.close,
        'name': 'price',
        'increasing': {'line': {'color': 'green'}},
        'decreasing': {'line': {'color': 'red'}},
        'type': 'candlestick'
    }

    trace2 = {
        'x': madata.date,
        'y': madata.ma5,
        'xaxis': 'x',
        'yaxis': 'y',
        'name': '5-day Moving Average',
        'type': 'line'
    }

    trace3 = {
        'x': madata.date,
        'y': madata.ma20,
        'xaxis': 'x',
        'yaxis': 'y',
        'name': '20-day Moving Average',
        'marker': {'color': '#33bbff', 'width': 3, 'dash': "5px"},
        'type': 'line'
    }

    trace4 = {
        'x': madata.date,
        'y': madata.volume,
        'xaxis': 'x',
        'yaxis': 'y2',
        'name': 'Volume',
        'marker': {'color': madata.color_label},
        'type': 'bar'
    }

    plot_data = [trace1, trace2, trace3, trace4]

    return html.Div(
        [

            html.H3(plot_title),
            dcc.Markdown(''' --- '''),
            dcc.Graph(
                id='stockgraph',
                figure={
                    "data": plot_data,
                    "layout": {
                        # "title": plot_title,
                        'showlegend': False,
                        'paper_bgcolor': 'white',
                        'plot_bgcolor': 'rgba(0, 0, 0, 0)',
                        "grid": {
                            "rows": 2,
                            "columns": 1,
                            "subplots": [['xy'], ['xy2']],
                            "roworder": 'top to bottom'
                        },
                        "xaxis": {
                            "automargin": True,
                            'showgrid': True,
                            'showline': True,
                            'zeroline': True,
                            'gridcolor': '#bdbdbd',
                            'linecolor': 'black',  # '#636363',
                            'linewidth': 2,
                            'rangeslider': {'visible': False},
                            'rangeselector': {
                                'buttons': [
                                    {'count': 1, 'label': '1m', 'step': 'month', 'stepmode': 'backward'},
                                    {'count': 3, 'label': '3m', 'step': 'month', 'stepmode': 'backward'},
                                    {'count': 6, 'label': '6m', 'step': 'month', 'stepmode': 'backward'},
                                    {'count': 1, 'label': '1y', 'step': 'year', 'stepmode': 'backward'},
                                    {'count': 3, 'label': '3y', 'step': 'year', 'stepmode': 'backward'},
                                    {'count': 5, 'label': '5y', 'step': 'year', 'stepmode': 'backward'},
                                    {'step': 'all'}
                                ]
                            },
                        },
                        "yaxis": {'gridcolor': '#bdbdbd', 'side': 'right', 'showgrid': True, "automargin": True, 'autorange': True, 'domain': [0.19, 0.95]},
                        "yaxis2": {'gridcolor': '#bdbdbd', 'side': 'right', "automargin": True, 'autorange': True, 'domain': [0, 0.185]},
                        "height": 500,
                        "margin": {"t": 0, "l": 15, "r": 15},
                    },
                },
            )
        ]
    )


if __name__ == "__main__":

    # print(madata1.tail())
    app.run_server(debug=True)
