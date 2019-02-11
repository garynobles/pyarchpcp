import psycopg2
from psycopg2 import sql
from datetime import datetime
from scipy.spatial.distance import cdist


class GetPointDistance:
    def calculate_nearest_top(self, schema, top_point_cloud, base_point_cloud):

        # schema = 'pc_processing'
        # top_point_cloud = 'pc_201407060711'
        # base_point_cloud = 'pc_201407061221'

        table_name_top = top_point_cloud + "_dist_top"
        sample_top = top_point_cloud + "_sample_top"
        sample_base = base_point_cloud + "_sample_base"

        # @@@@@@@@@ creation of a temporary holdiong table, this should be revised @@@@@@@@@
        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        print("connected")
        cur.execute("""DROP TABLE IF EXISTS {}."pc_dist_top_tmp" CASCADE;""".format(schema))

        query1 = sql.SQL("""CREATE TABLE {}.{} (
                        id serial NOT NULL,
                        x double precision,
                        y double precision,
                        z double precision,
                        geom geometry,
                        dist double precision);""").format(
            *map(sql.Identifier, (schema, "pc_dist_top_tmp")))
        cur.execute(query1)

        cur.close()
        conn.commit()
        # @@@@@@@@@ END @@@@@@@@@

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        print("connected")
        cur.execute("""DROP TABLE IF EXISTS {}.{} CASCADE;""".format(schema, table_name_top))

        cur.execute("""
                CREATE TABLE {}.{} (
                        id serial NOT NULL,
                        x double precision,
                        y double precision,
                        z double precision,
                        geom geometry,
                        dist double precision);
                """.format(schema, table_name_top))
        cur.close()
        conn.commit()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()

        print("connected4")
        cur.execute("""SELECT x,y,z FROM {}.{};""".format(schema, sample_top))
        T = cur.fetchall()

        print("connected5")

        cur.execute("""SELECT x,y,z FROM {}.{};""".format(schema, sample_base))
        B = cur.fetchall()
        cur.close()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        start_time = datetime.now()
        print(start_time)
        xi = [i[0] for i in T]
        yi = [i[1] for i in T]
        zi = [i[2] for i in T]
        XB = B
        insert_params = []
        for i in range(len(T)):
            XA = [T[i]]
            disti = cdist(XA, XB, metric='euclidean').min()
            insert_params.append((xi[i], yi[i], zi[i], disti))
            print("Top: " + str(i) + " of " + str(len(T)))
        cur.executemany("INSERT INTO pc_processing.pc_dist_top_tmp (x,y,z,dist) values (%s, %s, %s, %s)",
                        insert_params)
        conn.commit()
        cur.close()
        # @@@@@@@@@ creation of a temporary holdiong table, this should be revised @@@@@@@@@
        # conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        # cur = conn.cursor()
        print("connected")

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()

        query2 = sql.SQL("""INSERT INTO {}.{} (x, y, z, dist)
            (SELECT x, y, z, dist FROM {}."pc_dist_top_tmp");""").format(
            *map(sql.Identifier, (schema, table_name_top, schema)))
        print('query created')
        cur.execute(query2)
        # print(query2.as_string(conn))

        cur.close()
        conn.commit()

        end_time = datetime.now()
        print("Runtime: ", end='')
        print(end_time - start_time)
        print("Finished")
        print('Complete')


    def calculate_nearest_base(self, schema, top_point_cloud, base_point_cloud):
        # schema = 'pc_processing'
        # top_point_cloud = 'pc_201407060711'
        # base_point_cloud = 'pc_201407061221'

        table_name_base = base_point_cloud + "_dist_base"

        sample_top = top_point_cloud + "_sample_top"
        sample_base = base_point_cloud + "_sample_base"

        # @@@@@@@@@ creation of a temporary holdiong table, this should be revised @@@@@@@@@
        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        print("connected")
        cur.execute("""DROP TABLE IF EXISTS {}."pc_dist_base_tmp" CASCADE;""".format(schema))

        query1 = sql.SQL("""CREATE TABLE {}.{} (
                        id serial NOT NULL,
                        x double precision,
                        y double precision,
                        z double precision,
                        geom geometry,
                        dist double precision);""").format(
            *map(sql.Identifier, (schema, "pc_dist_base_tmp")))
        cur.execute(query1)

        cur.close()
        conn.commit()
        # @@@@@@@@@ END @@@@@@@@@

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        print("connected")
        cur.execute("""DROP TABLE IF EXISTS {}.{} CASCADE;""".format(schema, table_name_base))

        cur.execute("""
                CREATE TABLE {}.{} (
                        id serial NOT NULL,
                        x double precision,
                        y double precision,
                        z double precision,
                        geom geometry,
                        dist double precision);
                """.format(schema, table_name_base))
        cur.close()
        conn.commit()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()

        print("connected4")
        cur.execute("""SELECT x,y,z FROM {}.{};""".format(schema, sample_top))
        T = cur.fetchall()

        print("connected5")

        cur.execute("""SELECT x,y,z FROM {}.{};""".format(schema, sample_base))
        B = cur.fetchall()

        cur.close()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        start_time = datetime.now()
        print(start_time)
        xi = [i[0] for i in B]
        yi = [i[1] for i in B]
        zi = [i[2] for i in B]
        XB = T

        insert_params = []
        for i in range(len(B)):
            XA = [B[i]]
            disti = cdist(XA, XB, metric='euclidean').min()
            insert_params.append((xi[i], yi[i], zi[i], disti))
            print("Base: " + str(i) + " of " + str(len(B)))
        cur.executemany("INSERT INTO pc_processing.pc_dist_base_tmp (x,y,z,dist) values (%s, %s, %s, %s)",
                        insert_params)
        conn.commit()
        cur.close()

        # @@@@@@@@@ creation of a temporary holdiong table, this should be revised @@@@@@@@@
        print("connected")

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()

        query2 = sql.SQL("""INSERT INTO {}.{} (x, y, z, dist)
            (SELECT x, y, z, dist FROM {}."pc_dist_base_tmp");""").format(
            *map(sql.Identifier, (schema, table_name_base, schema)))
        print('query created')
        cur.execute(query2)
        # print(query2.as_string(conn))

        cur.close()
        conn.commit()
        # @@@@@@@@@ END @@@@@@@@@

        end_time = datetime.now()
        print("Runtime: ", end='')
        print(end_time - start_time)
        print("Finished")
        print('Complete')