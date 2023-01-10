import neo4j
import pandas as pd


user = pd.read_csv('data/yelp_training_set_user.csv')
user = pd.DataFrame(user)


business = pd.read_csv('data/yelp_training_set_business.csv')
business = pd.DataFrame(business)



review = pd.read_csv('data/yelp_training_set_review.csv')
review = pd.DataFrame(review)





def connect_db():
    driver = neo4j.GraphDatabase.driver(uri="neo4j://0.0.0.0:7687", auth=("neo4j", "970708Guo"))
    session = driver.session(database="neo4j")
    return session


def wipe_out_db(session):
    # wipe out database by deleting all nodes and relationships

    # similar to SELECT * FROM graph_db in SQL
    query = "match (node)-[relationship]->() delete node, relationship"
    session.run(query)

    query = "match (node) delete node"
    session.run(query)



session = connect_db()

wipe_out_db(session)
print('DB initialize')

cities = []
states = []



for i,row in business.iterrows():
    if row["business_city"] not in cities:
        cities.append(row["business_city"])
        states.append(row["business_state"])




for i in range(len(cities)):
    session.run('''
          CREATE (c:City {city_name:$name,state:$state})
          ''',parameters = {'name': cities[i],'state': states[i]} )

print("cities finish")

for index, row in user.iterrows():
    session.run('''
      CREATE (u:User {id:$user_id, name:$reviewer_name,cool:$reviewer_cool,funny:$reviewer_funny, average_star:$reviewer_average_stars,useful:$reviewer_useful })
    ''', parameters = {'user_id': row['user_id'],
                       'reviewer_name': row['reviewer_name'],
                       'reviewer_cool': row['reviewer_cool'],
                       'reviewer_funny': row['reviewer_funny'],
                       'reviewer_average_stars': row['reviewer_average_stars'],
                       'reviewer_useful': row['reviewer_useful']})




for index, row in business.iterrows():
    session.run('''
      CREATE (b:Business {id:$business_id ,
      name:$business_name,
      address:$business_full_address,
      open:$business_open, 
      category:$business_categories})
     ''', parameters = {'business_id':row['business_id'],
                        'business_name':row['business_name'],
                        'business_full_address':row['business_full_address'],
                        'business_open':row['business_open'],
                        'business_categories':row['business_categories']})
    session.run('''
          MATCH (b1:Business)
          MATCH (c1:City)
          WHERE b1.id = $business_id AND c1.city_name = $city_name
          CREATE (b1) - [:LOCATE {location: $location}] ->(c1)
         ''', parameters={'business_id': row['business_id'],
                          'city_name': row['business_city'],
                          'location': row['business_location']})


print("Business finish")

i = 0

for index, row in review.iterrows():
    i += 1
    session.run('''
      CREATE (r:Review {id: $review_id, text: $text, stars: $stars, useful:$reviewer_useful})
     ''', parameters = {'review_id': row['review_id'],
                      'text': row['text'],
                      'stars': row['stars'],
                      'reviewer_useful':row['reviewer_useful']})

    session.run('''
                  MATCH (u1:User)
                  MATCH (r1:Review)
                  WHERE u1.id = $user_id AND r1.id = $review_id
                  CREATE (u1) - [:IS_AUTHOR_OF {date: $date}] ->(r1)
                 ''', parameters={'user_id': row['user_id'],
                                  'review_id': row['review_id'],
                                  'date': row['date']})

    session.run('''
                          MATCH (b1:Business)
                          MATCH (r1:Review)
                          WHERE b1.id = $business_id AND r1.id = $review_id
                          CREATE (r1) - [:UNDER] ->(b1)
                         ''', parameters={'business_id': row['business_id'],
                                          'review_id': row['review_id']})
    if i % 100 == 0:
        print(i)
    if i >= 10000:
        break

print("Reviews finish")




