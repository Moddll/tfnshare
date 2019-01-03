# Policy:  Check Unusual Daily Volume
import dash

from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from pandas_datareader import data as web
from datetime import datetime

import pandas as pd

from stock.tfnstock import RwDatabase
from stock.tfnstock import FormatStockData

# set database
db = '../findata/cse.db'
dbc = RwDatabase(db)

# Get Company List
sqlflt = "select * from companylist;"
lstCompany = dbc.read_sqldata(sqlflt)
symbol_array = lstCompany['symbol']

# Dash App

app = dash.Dash(__name__)

dp_options_list = []

for i, row in lstCompany.iterrows():
    # {'label': 'company', 'value': 'symbol'}
    mydict={}
    mydict['label'] = row['company']
    mydict['value'] = row['symbol']
    dp_options_list.append(mydict)


app.layout = html.Div([

    html.H1('CSE Stock Tickers'),
    dcc.Markdown(''' --- '''),
    # html.Div([], style=()),
    html.Div([html.H3('Enter a stock symbol:', style={'paddingRight': '30px'}),
        dcc.Dropdown(
            id='my-dropdown',
            options=dp_options_list,
            value='TGIF',
        )
    ], style={'display': 'inline-block', 'verticalAlign': 'top', 'width': '30%'}),
    html.Div([html.H3('Enter Start/End Date:'),
        dcc.DatePickerRange(id='my_date_picker',
                            min_date_allowed=datetime(2015, 1, 1),
                            max_date_allowed=datetime.today(),
                            start_date=datetime(2018, 1, 1),
                            end_date=datetime.today()
                            )
    ], style={'display': 'inline-block'}),
    html.Div(
        className="col-sm",
        children=
            html.Div([
                html.Button(id='submit-button',
                    className='btn btn-success',
                    n_clicks=0,
                    children='Submit',
                    style={'fontSize': 24, 'marginLeft': '30px'}
                )
            ], style={'display': 'inline-block'}),

    ),
    dcc.Graph(id='my-graph'),
    dcc.Markdown(''' --- ''')
], className='container-fluid')


@app.callback(Output('my-graph', 'figure'), [Input('my-dropdown', 'value')])
def update_graph(selected_dropdown_value):
    # Get stock data from database
    data = pd.DataFrame()
    formatdata = FormatStockData(db)
    data = formatdata.ma_stockdata(selected_dropdown_value)

    return {
        'data': [{
            'x': data.date,
            'y': data.close
        }]
    }


if __name__ == "__main__":

    print(dp_options_list)
    app.run_server(debug=True)
