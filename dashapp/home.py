import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

# Configure navbar menu
navbar = html.Div([
    html.Ul([
            html.Li([
                    dcc.Link('First', href='/first')
                    ], className='active'),
            html.Li([
                    dcc.Link('Second', href='/second')
                    ]),
            html.Li([
            dcc.Link('Third', href='/third')
                    ]),
            ], className='nav navbar-nav')
], className='navbar navbar-inverse navbar-static-top', role='navigation')

body = html.Div(
    [
        html.Div([
            html.H2("hello world!")
            ], className='wrapper'),

        html.Section([
            html.Ul([
                html.Li([
                    "Header"
                ], className='header')
            ], className='sidebar-menu')
        ])
    ],
    className="hold-transition skin-blue sidebar-mini"
)

# external_stylesheets = [dbc.themes.CERULEAN]
# external_stylesheets = ['https://fonts.googleapis.com/css?family=Source+Sans+Pro:300,400,600,700,300italic,400italic,600italic"']
app = dash.Dash(__name__)

app.layout = html.Div([navbar, body])

if __name__ == "__main__":
    app.run_server(port=8051)
