import pandas as pd
import webbrowser
import re

import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input,Output
import dash_table as dt

import plotly.graph_objects as go
import plotly.express as px



app = dash.Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP])
project_name = None

def load_data():
    
    call_dataset_name = 'call_data.csv'
    service_dataset_name = 'service_data.csv'
    device_dataset_name = 'device_data.csv'
    
    global call_data
    call_data = pd.read_csv(call_dataset_name)
    
    global service_data
    service_data = pd.read_csv(service_dataset_name)
    
    global device_data
    device_data = pd.read_csv(device_dataset_name)
    
    global start_date_list
    temp_list = sorted(call_data["date"].dropna().unique().tolist())
    start_date_list = [{'label': str(i),'value':str(i)} for i in temp_list]
    
    global end_date_list
    end_date_list = [{'label': str(i),'value':str(i)} for i in temp_list]
    
    global report_type_list
    temp_list = ["Hourly", "Daywise", "Weekly"]
    report_type_list = [{'label': str(i), 'value':str(i)} for i in temp_list]

def open_browser():
    webbrowser.open_new('http://127.0.0.1:8010/')
    
def create_app_ui():
    main_layout = html.Div(
        [
            dbc.Row([
                dbc.Col(html.H1('CDR Analysis with Insights', id='main-title'),md=6),
                dbc.Col(
                     dcc.Tabs(id='tabs',value='call',parent_className="custom-tabs", className="custom-tabs-container",children=[
                         dcc.Tab(id='call-analytics',label='Call Analytics',value='call',className="custom-tab",selected_className='custom-tab--selected',children=[
                             html.Br(),
                             html.Br(),
                             dbc.Row([
                                 dbc.Col([
                                     dcc.Dropdown(
                                         id='start-date-dropdown',
                                         options=start_date_list,
                                         placeholder ='Select start date',
                                         value = "2019-06-20"
                                         )
                                     ],md=3),
                                 
                                  dbc.Col([
                                     dcc.Dropdown(
                                         id='end-date-dropdown',
                                         options=end_date_list,
                                         placeholder ='Select end date',
                                         value = "2019-06-25"
                                         )
                                     ],md=3),
                                  
                                   dbc.Col([
                                     dcc.Dropdown(
                                         id='group-dropdown',
                                         placeholder ='Select group',
                                         multi = True
                                         )
                                     ],md=3),
                                 
                                    dbc.Col([
                                     dcc.Dropdown(
                                         id='report-type-dropdown',
                                         placeholder ='Select report type',
                                         options=report_type_list,
                                         value = 'Hourly'
                                         )
                                     ],md=3),
                                 ])
                             ]),
                         dcc.Tab(id='device-analytics', label='Device Analytics', value='device',className="custom-tab",selected_className='custom-tab--selected',children=[
                             html.Br(),
                             dbc.Row([
                                 dbc.Col(
                                     dcc.Dropdown(
                                 id ='device-date-dropdown',
                                 options = start_date_list,
                                 multi = True,
                                 placeholder = 'Select date'
                                 ),
                                  md=5)
                                 ], className="remove-margin")
                             
                             ]),
                         dcc.Tab(id='service-analytics', label='Service Analytics',value='service',className="custom-tab",selected_className='custom-tab--selected',children=[
                             html.Br(),
                             dbc.Row([
                                 dbc.Col(
                                     dcc.Dropdown(
                                 id ='service-date-dropdown',
                                 options = start_date_list,
                                 multi = True,
                                 placeholder = 'Select date'
                                 ),
                                  md=5)
                                 ], className="remove-margin")
                             ])
                ]),
                md=6, className="tabs-columns"
                    )
                ],className="remove-margin",style={"background-color":'#039cde'}),
          html.Br(),
          dcc.Loading (html.Div(id='tabs-content'))
        ]
        )
    return main_layout

@app.callback(
    Output('group-dropdown','options'),
    [
      Input('start-date-dropdown','value'),
      Input ('end-date-dropdown','value')
     ]
    )
def update_groups(start_date,end_date):
    reformed_data = call_data[(call_data["date"]>=start_date) & (call_data["date"]<=end_date)]
    group_list = reformed_data["Group"].unique().tolist()
    group_list = [{'label':m, 'value':m} for m in group_list]
    return group_list

def create_card(title, content, color):
    card = dbc.Card(
           dbc.CardBody(
               [
                   html.H4(title),
                   html.Br(),
                   html.H2(content),
            
                ]
               ),
           color = color, inverse = True, style={'height':'82%','width':'25rem'}
        )
    return card


def count_devices(data):
    print(data)
    # various devices used for VoIP calls
    device_dict = {
        "Polycom" : 0,
        "Windows" : 0,
        "iphone" : 0,
        "Android" : 0,
        "Mac" : 0,
        "Yealink" : 0,
        "Aastra": 0,
        "Others": 0
        }
    
    reformed_data  = data["UserDeviceType"].dropna().reset_index()
    
    for var in reformed_data["UserDeviceType"]:
        if re.search("Polycom",var):
            device_dict["Polycom"]+= 1
        elif re.search("Windows", var):
            device_dict["Windows"]+= 1
        elif re.search("iPhone|iOS",var):
            device_dict["iphone"]+= 1
        elif re.search("Android",var):
            device_dict["Android"]+= 1
        elif re.search("Mac",var):
            device_dict["Mac"]+= 1
        elif re.search("Yealink",var):
            device_dict["Yealink"]+= 1
        elif re.search("Aastra",var):
            device_dict["Aastra"]+= 1
        else:
            device_dict["Others"]+= 1
            
    final_data = pd.DataFrame()
    final_data["Device"] = device_dict.keys()
    final_data["Count"] = device_dict.values()
        
    return final_data
    
    
  
    
@app.callback(
    Output('tabs-content','children'),
    [
       Input('tabs', "value"),
       Input('start-date-dropdown', 'value'),
       Input('end-date-dropdown', 'value'),
       Input('group-dropdown', 'value'),
       Input('report-type-dropdown', 'value'),
       Input('device-date-dropdown', 'value'),
       Input('service-date-dropdown', 'value'),
     ]
    )
def update_app_ui(tab,start_date, end_date, group, report_type, device_date, service_date):
    if tab=='call':
        
        #============Filter data based on dropdown selection===========================#
        call_analytics_data = call_data[(call_data['date']>=start_date) & (call_data['date']<=end_date)]
        
        if group != [] and group!=None:
            call_analytics_data = call_data[call_data['Group'].isin(group)]
            
        graph_data = call_analytics_data
        
        if report_type == "Hourly":
            graph_data = graph_data.groupby("hourly_range")["Call_Direction"].value_counts().reset_index(name = "count")
            x = "hourly_range"
            
            content = call_analytics_data["hourly_range"].value_counts().idxmax()
            title =  "Busiest Hour"
            
        
        elif report_type == 'Daywise':
            graph_data = graph_data.groupby('date')['Call_Direction'].value_counts().reset_index(name='count')
            x = 'date'
            
            content = call_analytics_data['date'].value_counts().idxmax()
            title = 'Busiest Day'
            
        else:
            graph_data = graph_data.groupby('weekly_range')['Call_Direction'].value_counts().reset_index(name='count')
            x = 'weekly_range'
            
            content = call_analytics_data['weekly_range'].value_counts().idxmax()
            title = 'Busiest WeekDay'
            
        # Graph Section
        figure = px.area(graph_data,
                         x = x,
                         y = "count",
                         color = 'Call_Direction',
                         hover_data = ["Call_Direction", "count"],
                         )
        
        figure.update_traces(mode= 'lines+markers')
        figure.update_layout( margin = dict(l=20, r=20, t=25, b=20)),

        
        # card section
        total_calls = call_analytics_data['Call_Direction'].count()
        card_1 = create_card("Total Calls", total_calls, "#2a80b9")
        
        incoming_calls = call_analytics_data[call_analytics_data['Call_Direction']=='Incoming']['Call_Direction'].count()
        card_2 = create_card("Incoming Calls",incoming_calls, "#15a085")
        
        outgoing_calls = call_analytics_data[call_analytics_data['Call_Direction']=='Outgoing']['Call_Direction'].count()
        card_3 = create_card("Outgoing Calls", outgoing_calls, "#f39c13")
        
        missed_calls = call_analytics_data[call_analytics_data['Missed Calls'] == 3]['Missed Calls'].count()
        card_4 = create_card("Missed Calls", missed_calls, "#e74c3d")
        
        max_duration = call_analytics_data["duration"].max()
        card_5 = create_card("Max Duration", f'{max_duration} min' , "#35495f" )
        
        card_6 = create_card(title, content , "#8e44ad")
        
        card_row_1 = dbc.Row([dbc.Col(card_1,md=4), dbc.Col(card_2,md=4), dbc.Col(card_3,md=4)], className='remove-margin')
        card_row_2 = dbc.Row([dbc.Col(card_4,md=4), dbc.Col(card_5,md=4), dbc.Col(card_6, md=4)], className='remove-margin')   
            
        
        card_div = html.Div([card_row_1,html.Br(),card_row_2], style={'padding-left':'5%'})
        
        # Data table section
        datatable_data = call_analytics_data.groupby(['Group','UserID','UserDeviceType'])['Call_Direction'].value_counts().unstack(fill_value=0).reset_index() 
        
        if call_analytics_data[call_analytics_data["Missed Calls"]==19]["Missed Calls"].count()!=0: 
            datatable_data["Missed Calls"] = call_analytics_data.groupby(['Group','UserID','UserDeviceType'])["Missed Calls"].value_counts().unstack()[3]
         
        else:
            datatable_data["Missed Calls"] = 0
        
        datatable_data["Total_call_duration"] = call_analytics_data.groupby(['Group','UserID','UserDeviceType'])["duration"].sum().tolist()
        
        datatable = dt.DataTable(
            id='table',
            columns=[{"name":i, "id":i} for i in datatable_data.columns],
            data = datatable_data.to_dict('records'), # returns the dataframe in the form of list of records,
            page_current = 0,
            page_size = 5,
            page_action ='native',
            style_header={
                'backgroundColor': '#a4adf8',
                'fontWeight': 'bold'
                },
            )
        
        
        return [
                  card_div,
                  dcc.Graph(figure=figure),
                  html.Br(),
                  datatable
              ]

#=========================DEVICE=========================================#
    elif tab=='device':
        if device_date is None or device_date==[]:
            device_analytics_data = count_devices(device_data)
        else:
            device_analytics_data = count_devices(device_data[device_data["DeviceEventDate"].isin(device_date)])
        
        print(device_analytics_data)
        fig1 = px.pie(device_analytics_data, names = "Device", values="Count", color="Device", hole =.3)
        fig1.update_layout(
            autosize = True,
            margin = dict(l=0,r=0,t=25,b=20)
            )
        fig1.update_layout({
          'plot_bgcolor': '#daecff',
            'paper_bgcolor': '#ffffff',
                })
        
        
        fig = px.bar(device_analytics_data, y='Count', x='Device', text='Count')
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title={"text" : "Histogram of various devices", 'x' : 0.5, 'y' : 0.93})
        fig.update_layout({
            'plot_bgcolor': '#daecff',
            'paper_bgcolor': '#ffffff',       
                })
       
        
        device_layout = dbc.Row([
            dbc.Col(
                dcc.Graph(figure = fig1),md=5
                ),
             dbc.Col(
                dcc.Graph(figure = fig),md=7
                ),
            
            ],className="remove-margin")
        return [html.Br(),device_layout]
    
    
#========================= SERVICE ======================================#
    elif tab=='service':
        if service_date is None or service_date==[]:
           service_analytics_data = service_data["FeatureName"].value_counts().reset_index(name="Count") 
          
        else:
            service_analytics_data = service_data[service_data["FeatureEventDate"].isin(service_date)]["FeatureName"].value_counts().reset_index(name = "Count")
            
        fig = px.pie(service_analytics_data , names = "index", values= "Count", color="index")
        fig.update_layout(
            autosize = True,
            margin = dict(l=0,r=0,t=25,b=20)
            )
        fig.update_layout({
             'plot_bgcolor': '#daecff',
            'paper_bgcolor': '#ffffff',   
           
                })
       
        fig2 = px.bar(service_analytics_data, y='Count', x='index', text='Count',labels={'index':'Services'})
        fig2.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig2.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title={"text" : "Histogram of various services", 'x' : 0.5, 'y' : 0.93})
        fig2.update_layout({
            'plot_bgcolor': '#daecff',
            'paper_bgcolor': '#ffffff',       
                })
       
        
        service_layout = dbc.Row([
            dbc.Col(
                dcc.Graph(figure = fig),md=5
                ),
             dbc.Col(
                dcc.Graph(figure = fig2),md=7
                ),
            
            ],className="remove-margin")
        return [html.Br(),service_layout]
    
def main():
    load_data()
    open_browser()
    
    global project_name
    project_name = "CDR Analysis with Insights"
    
    global app
    app.title = project_name
    app.layout = create_app_ui()
    app.run_server(host='127.0.0.1', port=8010)
    
    print("This would be executed only after script is closed")
    app = None
    project_name = None
    
    global call_data, device_data, service_data, start_date_list, end_date_list, report_type_list
    call_data = None
    device_data = None
    service_data = None
    start_date_list = None
    end_date_list = None
    report_type_list = None
    

if __name__ == '__main__':
    main()

    
    
    
    
    
    
    
    
    
    
    