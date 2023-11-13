import neo4j
from createKG import GraphDatabaseManager

def searchTeacher(teachername):
    query = """
            MATCH (n:Teacher {teachername: $name}), (m:College)
            MATCH (n)-[r:BelongTO]->(m)
            RETURN n,m.collegeid,m.collegename
            """
    parameters = {"name":teachername}
    graph_db_manager = GraphDatabaseManager("bolt://localhost:7687", "neo4j", "12345678")
    result = graph_db_manager.execute_query(query,parameters)
    # Retrieve data
    if result.peek() is not None:
        # Retrieve data
        data = [record.data() for record in result]
    else:
        data = []
    # Process the data if needed
    processed_data = []
    for record in data:
        teacher_data = record.get("n", {})
        college_id = record.get("m.collegeid")
        college_name = record.get("m.collegename")

        # Process the data as needed
        processed_data.append({
            "teacher_data": teacher_data,
            "college_id": college_id,
            "college_name": college_name
        })
    return processed_data



if __name__ == '__main__':
    print(searchTeacher('李小龙'))