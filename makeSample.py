import psycopg2
from psycopg2 import sql


class MakeSample:
    def sample_bernoulli(self, schema, top_point_cloud, base_point_cloud, sample):

        imported_table_name_top = top_point_cloud + "_top"
        imported_table_name_base = base_point_cloud + "_base"

        table_name_top = top_point_cloud + "_sample_top"
        table_name_base = base_point_cloud + "_sample_base"

        print(top_point_cloud)
        print(base_point_cloud)

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()

        drop_table_top = sql.SQL("""DROP TABLE IF EXISTS {}.{};""").format(
            *map(sql.Identifier, (schema, table_name_top)))
        drop_table_base = sql.SQL("""DROP TABLE IF EXISTS {}.{};""").format(
            *map(sql.Identifier, (schema, table_name_base)))
        cur.execute(drop_table_top)
        cur.execute(drop_table_base)

        cur.execute("""DROP TABLE IF EXISTS {}.{};""".format(schema, table_name_top))
        cur.execute("""DROP TABLE IF EXISTS {}.{};""".format(schema, table_name_base))
        cur.close()
        conn.commit()
        conn.close()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        print('tables dropped')
        query1 = sql.SQL("""CREATE TABLE {}.{} AS 
            (SELECT * FROM {}.{} TABLESAMPLE BERNOULLI (10));""").format(
            *map(sql.Identifier, (schema, table_name_top, schema, imported_table_name_top)))
        query2 = sql.SQL("""CREATE TABLE {}.{} AS 
            (SELECT * FROM {}.{} TABLESAMPLE BERNOULLI (10));""").format(
            *map(sql.Identifier, (schema, table_name_base, schema, imported_table_name_base)))
        print('query created')
        cur.execute(query1)
        cur.execute(query2)

        # print(query1.as_string(conn))

        print('next')
        print('complete')
        cur.close()
        conn.commit()
        conn.close()