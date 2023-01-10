import neo4j
import pandas as pd

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

query1 = '''
        LOAD CSV WITH HEADERS FROM 'file:///yelp_training_set_review.csv' AS line
        MERGE (:City {name:line.business_city})
        '''
result = session.run(query1)
print(result)

