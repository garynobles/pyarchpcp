import pptk
import psycopg2
import numpy as np
# import vtk
import pandas as pd


class ViewPointCloud:
    def view_point_clouds_imported(self, top_point_cloud, base_point_cloud):
        import pandas as pd
        table_name_top = top_point_cloud + "_top"
        table_name_base = base_point_cloud + "_base"
        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute("""SELECT (x - 580000),(y - 4275000),(z), 255,0,0 FROM pc_processing.{} TABLESAMPLE BERNOULLI (100);""".format(table_name_top))
        T = cur.fetchall()
        cur.close()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute("""SELECT (x - 580000),(y - 4275000),(z), 0,255,0 FROM pc_processing.{} TABLESAMPLE BERNOULLI (100);""".format(table_name_base))
        B = cur.fetchall()
        cur.close()

        data = T + B

        pd = pd.DataFrame(data)
        pd.columns = ['x', 'y', 'z','r', 'g', 'b']
        points = pd
        P = points

        v = pptk.viewer(P[['x', 'y', 'z']])
        v.attributes(P[['r', 'g', 'b']] / 255)
        v.set(point_size=0.001)

    def view_point_clouds_dist(self, top_point_cloud, base_point_cloud):
        import pandas as pd
        # top_point_cloud = 'pc_201406151141'
        # base_point_cloud = 'pc_201407031017'
        schema = 'pc_processing'
        table_name_top = top_point_cloud + '_dist_top'
        table_name_base = base_point_cloud + '_dist_base'
        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute("""SELECT (x - 580000),(y - 4275000),(z),dist, 255,0,0 FROM pc_processing.{}
         TABLESAMPLE BERNOULLI (100);""".format(table_name_top))
        T = cur.fetchall()
        cur.close()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute("""SELECT (x - 580000),(y - 4275000),(z),dist, 0,255,0 FROM {}.{};""".format(
            schema, table_name_base))
        B = cur.fetchall()
        cur.close()

        data = T + B

        pd = pd.DataFrame(data)
        pd.columns = ['x', 'y', 'z', 'i', 'r', 'g', 'b']
        points = pd
        P = points

        v = pptk.viewer(P[['x', 'y', 'z']])
        v.attributes(P[['r', 'g', 'b']] / 255., P['i'])
        v.set(point_size=0.001)

    def view_point_clouds_filtered(self, top_point_cloud, base_point_cloud):
        import pandas as pd

        # top_point_cloud = 'pc_201407060711'
        # base_point_cloud = 'pc_201407061221'

        table_name_top = top_point_cloud + '_filtered_top'
        table_name_base = base_point_cloud + '_filtered_base'

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute("""SELECT (x - 580000),(y - 4275000),(z), 33,179,210 FROM pc_processing.{}
         TABLESAMPLE BERNOULLI (100);""".format(table_name_top))
        T = cur.fetchall()
        cur.close()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute("""SELECT (x - 580000),(y - 4275000),(z), 33,179,210 FROM pc_processing.{}
         TABLESAMPLE BERNOULLI (100);""".format(table_name_base))
        B = cur.fetchall()
        cur.close()

        data = T + B

        pd = pd.DataFrame(data)
        pd.columns = ['x', 'y', 'z', 'r', 'g', 'b']
        points = pd
        P = points

        v = pptk.viewer(P[['x', 'y', 'z']])
        v.attributes(P[['r', 'g', 'b']] / 255)
        v.set(point_size=0.001)
