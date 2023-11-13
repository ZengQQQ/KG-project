import os.path
from neo4j import GraphDatabase
import pandas as pd
import tqdm

class GraphDatabaseManager:
    def __init__(self, uri, user, password):
        self.uri = uri
        self._user = user
        self._password = password
        self._driver = GraphDatabase.driver(self.uri, auth=(self._user, self._password))

    def connect(self):
        self._driver = GraphDatabase.driver(self.uri, auth=(self._user, self._password))

    def close(self):
        if self.driver is not None:
            self.driver.close()

    def execute_query(self, query, parameters=None):
        with self.driver.session() as session:
            return  session.run(query, parameters)

    def create_teacher(self):
        csv_file_path = os.path.abspath("./import/shiti-teacher1.csv")
        df = pd.read_csv(csv_file_path, encoding='utf-8')
        df.fillna("暂无", inplace=True)
        for index, row in df.iterrows():
            teacherid = row['teacherid']
            teachername = row['teachername']
            Email = row['Email']
            Gender = row['Gender']
            Duty = row['Duty']
            Nation = row['Nation']
            Course = row['Course']
            Direction = row['Direction']
            achievement = row['achievement']
            Requirement = row['Requirement']
            Level = row['Level']
            query1 = """
                            MERGE (p:Teacher {
                                teacherid: $teacherid,
                                teachername: $teachername,
                                Email: $Email,
                                Gender: $Gender,
                                Duty: $Duty,
                                Nation: $Nation,
                                Course: $Course,
                                Direction: $Direction,
                                achievement:$achievement,
                                Requirement:$Requirement,
                                Level:$Level
                            })
                        """
            parameters = {"teacherid":teacherid,"teachername":teachername,"Email":Email,"Gender":Gender,
                          "Duty":Duty,"Nation":Nation,"Course":Course,"Direction":Direction,"achievement":achievement,
                          "Requirement":Requirement,"Level":Level}
            self.execute_query(query1,parameters)

    def create_xueyuan(self):
        csv_file_path = os.path.abspath("./import/xueyuan.csv")
        df = pd.read_csv(csv_file_path, encoding='utf-8')
        df.fillna("暂无", inplace=True)
        for index, row in df.iterrows():
            collegeid = row['collegeid']
            collegename = row['collegename']
            query2 = """
                            MERGE (p:College {
                                collegeid: $collegeid,
                                collegename: $collegename
                            })
                        """
            parameters = {"collegeid":collegeid,"collegename":collegename}
            self.execute_query(query2,parameters)

    def create_school(self):
        csv_file_path = os.path.abspath("./import/xuexiao.csv")
        df = pd.read_csv(csv_file_path, encoding='utf-8')
        df.fillna("暂无", inplace=True)
        for index, row in df.iterrows():
            schoolid = row['schoolid']
            schoolname = row['schoolname']
            query3 = """
                            MERGE (p:School {
                                schoolid: $schoolid,
                                schoolname: $schoolname
                            })
                        """
            parameters = {"schoolid":schoolid,"schoolname":schoolname}
            self.execute_query(query3,parameters)

    def create_guanxi_xueyuan_laoshi(self):
        csv_file_path = os.path.abspath("./import/guanxi.csv")
        df = pd.read_csv(csv_file_path, encoding='utf-8')
        df.fillna("暂无", inplace=True)
        for index, row in df.iterrows():
            teacherid = row['teacherid']
            collegeid = row['collegeid']
            guanxi = row['guanxi']
            query4 = """
                            MATCH (from:Teacher {teacherid: $teacherid}), (to:College {collegeid: $collegeid}) 
                            MERGE (from)-[r:BelongTO {desc: $guanxi}]->(to)
                        """
            parameters = {"teacherid":teacherid,"collegeid":collegeid,"guanxi":guanxi}
            self.execute_query(query4,parameters)

    def create_guanxi_xueyuan_xuexiao(self):
        csv_file_path = os.path.abspath("./import/guanxi1.csv")
        df = pd.read_csv(csv_file_path, encoding='utf-8')
        df.fillna("暂无", inplace=True)
        for index, row in df.iterrows():
            schoolid = row['schoolid']
            collegeid = row['collegeid']
            guanxi1 = row['guanxi1']
            query5 = """
                            MATCH (from:School {schoolid: $schoolid}), (to:College {collegeid: $collegeid}) 
                            MERGE (from)-[r:Include {desc: $guanxi1}]->(to)
                        """
            parameters = {"schoolid":schoolid,"collegeid":collegeid,"guanxi1":guanxi1}
            self.execute_query(query5,parameters)


# 示例用法
if __name__ == "__main__":
    # 替换为您的 Neo4j 数据库的 URI、用户名和密码
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "12345678"

    # 创建 GraphDatabaseManager 实例
    graph_db_manager = GraphDatabaseManager(neo4j_uri, neo4j_user, neo4j_password)

    try:
        # 连接到数据库
        graph_db_manager.create_teacher()
        graph_db_manager.create_xueyuan()
        graph_db_manager.create_school()
        graph_db_manager.create_guanxi_xueyuan_laoshi()
        graph_db_manager.create_guanxi_xueyuan_xuexiao()


    finally:
        # 关闭数据库连接
        graph_db_manager.close()
