from flask import Flask, render_template, request
import neo4j
import datetime
import plotly.graph_objects as go
import plotly.express as px
import re
import pandas as pd
from plotly.offline import plot

app = Flask(__name__, static_folder="image")

def connect_db():
    driver = neo4j.GraphDatabase.driver(uri="neo4j://0.0.0.0:7687", auth=("neo4j", "970708Guo"))
    session = driver.session(database="neo4j")
    return session



@app.route("/")
def homepage():
    return render_template('Homepage.html')


# data management part
@app.route("/datamanage/<business_id>",methods = ['GET','POST'])
def add(business_id = ""):
    session = connect_db()
    if request.method == 'POST':
        return render_template('add_review.html')
    if request.method == 'GET':  # get method
        user_id = request.args.get('user_id')
        text = request.args.get('text')
        stars = request.args.get('stars')

        if text == None:
            return render_template('add_review.html')

        query1 = '''
        MATCH (n:User)
        WHERE n.id = $id
        RETURN n
        '''
        result = session.run(query1, parameters={'id':user_id})
        today = datetime.date.today()


        if len(result.data()) == 0:
            query2 = '''
                CREATE (u:User {name: 'Guest'})
                RETURN id(u) as u_id
                '''
            mid = session.run(query2)
            a = mid.data()[0]
            mid = a['u_id']

        else:
            query6 = '''
                MATCH (n:User)
                WHERE n.id = $id
                RETURN id(n) as u_id
                '''
            mid = session.run(query6, parameters={'id':user_id})

            a = mid.data()[0]

            mid = a['u_id']

        query3 = '''MATCH (b:Business)
                            MATCH (r:Review)
                            WHERE b.id = $business_id AND id(r) = $r_id
                            CREATE (r) - [:UNDER] -> (b)
                            '''
        query4 = '''
                CREATE (r:Review {text: $text,stars:$stars})
                RETURN id(r) as review_id
                '''

        r_id = session.run(query4, parameters={'text': text,
                                               'stars': stars})
        r_id = r_id.data()[0]
        query5 = '''MATCH (u:User)
                    MATCH (r:Review)
                    WHERE id(u) = $mid AND id(r) = $r_id
                    CREATE (u) - [:IS_AUTHOR_OF {date: $date}] -> (r)
                    '''
        session.run(query5, parameters={'mid': mid,
                                        'r_id': r_id['review_id'],
                                        'date': today})
        session.run(query3, parameters={'business_id': business_id,
                                        'r_id': r_id['review_id']})

    return render_template('add_review.html')


@app.route("/delete_review/<review_id>",methods = ['GET','DELETE'])
def delete(review_id):
    review_id = int(review_id)
    session = connect_db()
    query = '''
            MATCH (n2) - [r2:IS_AUTHOR_OF] -> (r:Review) - [r1:UNDER] -> (n1)
            WHERE id(r) = $review_id
            DELETE r1
            DELETE r2
            DELETE (r)
            
        '''

    try:
        session.run(query, parameters={'review_id': review_id})
        result = "Success"
    except:
        result = None
    return render_template('delete_review.html',data = result)

@app.route("/update_review/<review_id>",methods = ['GET','POST'])
def update_review(review_id):
    session = connect_db()
    review_id = int(review_id)
    flag = "Wait for your update"
    if request.method == 'POST':
        return render_template('update_review.html')
    if request.method == 'GET':
        text = request.args.get('text')
        stars = request.args.get('stars')


        if text is not None:
            query = '''
                        MATCH (r:Review)
                        WHERE id(r) = $review_id
                        SET r.text = $text ,r.stars = $stars
                        RETURN r
                '''
            try:
                session.run(query, parameters={'review_id': review_id,
                                                        'text': text,
                                                        'stars': stars})




                flag = "Update successfully"
            except:
                flag = "Update failed"
        else:
            flag = "Wait for your update"


    return render_template('update_review.html',data = flag)

@app.route("/updatebusiness/<business_id>",methods = ['GET','POST'])
def update_business(business_id):
    session = connect_db()
    if request.method == "POST":
        return render_template('update_business.html')

    if request.method == "GET":
        city = request.args.get('city')
        state = request.args.get('state')
        address = request.args.get('address')
        open = request.args.get('inlineRadioOptions')

        if open != None:
            if open == "option1":
                open = 'True'
            else:
                open = 'False'
            query1 = '''
                    MATCH (b:Business) -[r] - (c:City)
                    WHERE b.id = $business_id
                    RETURN c.city_name as city,c.state as state,b.address as address
            '''

            result1 = session.run(query1, parameters={'business_id': business_id})
            data1 = result1.data()
            data1 = data1[0]


            if city == '':
                city = data1['city']
            if state == '':
                state = data1['state']
            if address == '':
                address = data1['address']


            query2 = '''
                    MATCH (b:Business) - [r] -> (c:City)
                    WHERE b.id = $business_id
                    DELETE r
            '''
            session.run(query2, parameters={'business_id': business_id})

            query3 = '''
                    MATCH (b:Business)
                    WHERE b.id = $business_id
                    SET b.city = $city, b.state = $state, b.address = $address, b.open = $open
            '''


            query4 = '''
                    MATCH (c:City)
                    WHERE c.city_name = $city AND c.state = $state
                    RETURN c
            '''

            query5 = '''
                    CREATE (c:City {city_name: $city, state: $state})     
            '''
            query6 = '''
                                MATCH(b:Business)
                                MATCH(c:City)
                                WHERE b.id = $business_id AND c.city_name = $city AND c.state = $state
                                CREATE (b) - [:LOCATE] -> (c)
                                    
                        '''

            result2 = session.run(query4, parameters={'city': city,
                                                  'state':state})

            data2 = result2.data()

            if len(data2) == 0:
                session.run(query5, parameters={'city': city,
                                                  'state':state})
            session.run(query3, parameters={'business_id':business_id,
                                            'city': city,
                                            'state':state,
                                            'address':address,
                                            'open':open})

            session.run(query6, parameters={'business_id':business_id,
                                            'city': city,
                                            'state':state})

    return render_template('update_business.html')


@app.route("/addbusiness",methods = ['GET','POST'])
def add_business():
    session = connect_db()
    if request.method == "POST":
        return render_template('add_business.html')
    if request.method == "GET":

        name = request.args.get('name')
        city = request.args.get('city')
        state = request.args.get('state')
        address = request.args.get('address')
        open = request.args.get('inlineRadioOptions')

        if name != None:

            if open == "option1":
                openvalue = 'True'
            else:
                openvalue = 'False'


            query1 = '''
                        CREATE (b:Business {name: $name, city:$city, state: $state, open:$openvalue,address:$address})
                        RETURN id(b) as id
                '''

            id = session.run(query1, parameters={'name': name,
                                                     'city': city,
                                                     'state': state,
                                                     'openvalue': openvalue,
                                                 'address':address})

            data = id.data()

            data = data[0]
            new_id = data['id']


            query2 = '''
                        MATCH (b:Business)
                        WHERE id(b) = $id
                        SET b.id = toString(id(b))
                        RETURN id(b)
                '''
            session.run(query2, parameters={'id': new_id})
            query3 = '''
                                    MATCH (c: City)
                                    WHERE c.city_name = $city AND c.state = $state
                                    RETURN c
                            '''

            result = session.run(query3, parameters={'city': city,
                                                     'state':state})

            count = result.data()


            if not count:
                query4 = '''
                        CREATE (c:City{city_name:$city ,state: $state})
                '''
                session.run(query4, parameters={'city': city,
                                                'state':state})
            query5 = '''
            MATCH (b:Business) 
            MATCH (c:City)
            WHERE id(b) = $id AND c.city_name = $city AND c.state = $state
            CREATE (b) -[:UNDER] -> (c)
            '''
            session.run(query5, parameters={'id':new_id,
                                                'city': city,
                                                'state':state})



    return render_template('add_business.html')

@app.route("/deletebusiness/<business_id>",methods = ['GET','DELETE'])
def delete_business(business_id):


    session = connect_db()
    query1 = '''MATCH (n) -[r]->(b:Business) 
    WHERE b.id = $business_id
    DELETE r
    '''
    query2 = '''MATCH (b:Business) -[r]-(n)
        WHERE b.id = $business_id
        DELETE r
        '''
    query3 = '''MATCH (b:Business)
            WHERE b.id = $business_id
            DELETE b
            '''
    try:
        session.run(query1, parameters={'business_id': business_id})
        session.run(query2, parameters={'business_id': business_id})
        session.run(query3, parameters={'business_id': business_id})

        flag = "Success"
    except:
        flag = None
    return render_template('delete_business.html',data = flag)



# search function
@app.route("/search",methods = ['GET'])
def search_business():

    if request.method == 'GET':  # get method
        bu = request.args.get('business_name')
        city = request.args.get('city')
    if not bu:
        bu = ''
    if not city:
        city = ''


    session= connect_db()
    query = '''
    MATCH (b: Business) -[r] -> (c:City)
    WHERE b.name =~$business AND c.city_name =~$city
    RETURN b.name as name,b.address as address,b.open as open,b.id as id
    '''
    result = session.run(query, parameters={'business':'.*' + bu + '.*','city': '.*' + city + '.*'})

    data = result.data()
    if len(data) > 100:
        data = data[:100]

    return render_template('Search_business.html', data= data)





@app.route("/search/review/<business_id>",methods = ['GET'])
def search_review(business_id):
    session = connect_db()
    query = '''
        MATCH (u:User) - [r1: IS_AUTHOR_OF] ->(n:Review) - [r2: UNDER] -> (b: Business)
        WHERE b.id = $business_id
        RETURN u.name as user_name, r1.date as date, n.text as text,n.stars as stars,toString(id(n)) as review_id
        '''
    result = session.run(query, parameters={'business_id': business_id})
    data = result.data()
    final = [business_id]
    final.append(data)
    return render_template('review_check.html', data= final)


# data analysis part
@app.route("/analysisbusiness/<business_id>")
def plot_function_business(business_id):
    session = connect_db()

    query1 = '''
    MATCH (r:Review) -[r1] -> (b:Business)
            WHERE b.id = $business_id AND r.stars in [0,1,2,3,4,5]
            RETURN AVG(r.stars) as avg,count(r1) as count, b.id as id,b.name as name
    '''

    query2 = '''
            MATCH (r:Review) -[r1] -> (b:Business)
            WHERE b.id = $business_id AND r.stars in [0,1,2,3,4,5]
            RETURN r.id as id, r.stars as stars
    '''

    result1 = session.run(query1, parameters={'business_id': business_id})
    result2 = session.run(query2, parameters={'business_id': business_id})


    data1 = result1.data()
    data2 = result2.data()


    if data1 and data2:

        average_stars = data1[0]['avg']
        total_count = data1[0]['count']

        stars = []
        for i in data2:
            stars.append(i['stars'])
        index = list(range(len(stars)))

        graphs2 = []
        graphs2.append(
            go.Scatter(x=index, y=stars, mode='markers', opacity=0.8, marker_size=[i * 6 for i in stars],
                       name='Scatter'))

        layout2 = {
            'title': data1[0]['name']  + " Distribution" ,
            'xaxis_title': 'Review',
            'yaxis_title': 'Stars',
            'height': 600,
            'width': 800,
        }
        fig = plot({'data': graphs2, 'layout': layout2}, output_type='div')



    query3 = '''
    MATCH (r:Review) -[r1] -> (b:Business)
    RETURN AVG(r.stars) as avg,count(r1) as count, b.id as id,b.name as name
    ORDER BY count DESC
    '''

    result3 = session.run(query3)

    data3 = result3.data()

    new_df = pd.DataFrame(data3[:10])
    new_name = list(new_df.name)
    new_count = list(new_df['count'])



    graphs1 = []


    graphs1.append(
        go.Bar(x=new_name, y=new_count))

    layout1 = {
        'title': "Most popular business",
        'xaxis_title': 'Name',
        'yaxis_title': 'Review Counts',
        'height': 600,
        'width': 800,
    }
    fig1 = plot({'data': graphs1, 'layout': layout1}, output_type='div')









    return render_template('Analysis.html', **locals())




# Advanced function
@app.route("/advanced")
def recommand():
    session = connect_db()
    query = '''
            MATCH (r:Review) - [r1] -> (b:Business)- [r2:LOCATE] -> (c)
            WHERE r.stars in [0,1,2,3,4,5]
            RETURN AVG(r.stars) as scores, b.id as id,b.name as name,b.address as address, r2.location as location
            '''

    result = session.run(query)
    token = 'pk.eyJ1IjoiZ3VveTIwIiwiYSI6ImNsYjY1a3RveTA0MDYzd3BoMGYxZWFqN3IifQ.LMgx6tN5dWAXE99dHIPtZA'
    data = result.data()
    lat = []
    lon = []
    text = []
    for i in data:
        try:
            rule = re.compile(r'[(](.*?)[)]', re.S)
            d = re.findall(rule, i['location'])
            d = d[0].split()
            lon.append(float(d[0]))
            lat.append(float(d[1]))
            text.append(i['name'] + ": " + i['address'] + '; ' + str(round(i['scores'],1))+ ' scores')
        except:
            continue

    dict = {"lat": lat, "lon": lon, "text": text}
    df = pd.DataFrame(dict)
    graph = []
    graph.append(go.Scattermapbox(mode='markers',
                                  lon=df.lon,
                                  lat=df.lat,
                                  hovertext=df.text,
                                  hoverinfo='text',
                                  marker_symbol='marker',
                                  marker_size=10
                                  ))

    layout = {'mapbox': {'accesstoken': token,'center':{'lat':33.581867,'lon':-112.241596},'zoom':11.8},'height':800}

    fig = plot({'data': graph, 'layout': layout}, output_type='div')
    return render_template('Advanced.html', **locals())


