import psycopg2
from psycopg2 import sql





class GetPointCloud:
    def top(self, top_point_cloud, schema):
        print("Started")
        schema_name = schema
        table_name1 = top_point_cloud
        table_name2 = top_point_cloud + "_top"
        print("creating tables")

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()

        query1 = sql.SQL("""DROP TABLE IF EXISTS {}.{} CASCADE;""").format(
            *map(sql.Identifier, (schema_name, table_name2)))
        cur.execute(query1)

        query2 = sql.SQL("""
            CREATE TABLE {}.{} (
                    id serial NOT NULL,
                    x double precision,
                    y double precision,
                    z double precision,
                    geom geometry,
                    dist double precision);
        """).format(*map(sql.Identifier, (schema_name, table_name2)))
        cur.execute(query2)

        cur.close()
        conn.commit()
        conn.close()
        print("tables created")

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        print("inserting data")
        try:
            query = sql.SQL("""INSERT INTO {}.{} (x,y,z)
                        SELECT
                            st_x(PC_EXPLODE(pa)::geometry) as x,
                            st_y(PC_EXPLODE(pa)::geometry) as y,
                            st_z(PC_EXPLODE(pa)::geometry) as z
                        from "public".{} order by x,y,z;""").format(
                *map(sql.Identifier, (schema_name, table_name2, table_name1)))

            cur.execute(query)

            conn.commit()
        except IOError:
            print('error not loaded')
        finally:
            cur.close()
            conn.close()

            conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
            cur = conn.cursor()
            cur.execute(
                """UPDATE pc_processing.{} SET geom=ST_GeomFromText('POINT('||x||' '||y||' '||z||')',32635);""".format(
                    table_name2))
            conn.commit()
            cur.close()
            print('complete')

    def base(self, base_point_cloud, schema):
        print("Started")
        schema_name = schema
        table_name1 = base_point_cloud
        table_name2 = base_point_cloud + "_base"
        print("creating tables")

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()

        query1 = sql.SQL("""DROP TABLE IF EXISTS {}.{} CASCADE;""").format(
            *map(sql.Identifier, (schema_name, table_name2)))
        cur.execute(query1)

        query2 = sql.SQL("""
            CREATE TABLE {}.{} (
                    id serial NOT NULL,
                    x double precision,
                    y double precision,
                    z double precision,
                    geom geometry,
                    dist double precision);
        """).format(*map(sql.Identifier, (schema_name, table_name2)))
        cur.execute(query2)

        cur.close()
        conn.commit()
        conn.close()
        print("tables created")

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        print("inserting data")
        query = sql.SQL("""INSERT INTO {}.{} (x,y,z)
                    SELECT
                        st_x(PC_EXPLODE(pa)::geometry) as x,
                        st_y(PC_EXPLODE(pa)::geometry) as y,
                        st_z(PC_EXPLODE(pa)::geometry) as z
                    from "public".{} order by x,y,z;""").format(
            *map(sql.Identifier, (schema_name, table_name2, table_name1)))

        cur.execute(query)

        conn.commit()
        cur.close()
        conn.close()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute(
            """UPDATE pc_processing.{} SET geom=ST_GeomFromText('POINT('||x||' '||y||' '||z||')',32635);""".format(
                table_name2))
        conn.commit()


        cur.close()
        print('complete')
