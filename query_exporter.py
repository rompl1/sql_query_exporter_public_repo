# Export Prometheus metrics from SQL queries
# By Roman Peliusenko 
# v1.0

class Metric():
    def __init__(self, name, query):
        self.metric_name = name
        self.metric_query = query
    def query_sql(self):
        import pymssql
        conn = pymssql.connect(server="",database="",port=1433,user="",password="")
        cursor = conn.cursor(as_dict=True)
        cursor.execute(self.metric_query)
        list=[]
        for row in cursor:
            value=row.pop(self.metric_name)
            reformat_row=",".join('{}="{}"'.format(*i) for i in row.items())
            reformat_row = "{" + reformat_row + "}"
            line = (f'{self.metric_name}{reformat_row} {value}')
            list.append(line)
        conn.close()
        prometheus_metrics = "\n".join(list)
        return prometheus_metrics

from flask import Flask
import json

app = Flask(__name__)

@app.route('/sql_query_exporter', methods=['GET'])
def sql_query_exporter():
    
    metric1 = Metric('assort_count_testa','select top 100 ShopCode,COUNT(*) as assort_count_testa,cast(IsInAssortment as varchar) as IsInAssortment from [somedb].dbo.sometable (nolock) group by ShopCode,IsInAssortment order by COUNT (*) desc')
    metric2 = Metric('assort_count_testa2','select top 100 ShopCode,COUNT(*) as assort_count_testa2,cast(IsInAssortment as varchar) as IsInAssortment from [somedb].dbo.sometable (nolock) group by ShopCode,IsInAssortment order by COUNT (*) asc')
    final_list = metric1.query_sql() + "\n" + metric2.query_sql() + "\n"
    return final_list 

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=someport)
