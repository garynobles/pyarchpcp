import os
import psycopg2
from psycopg2 import sql
import pandas as pd
from open3d import *
import pyntcloud
from pyntcloud import PyntCloud
from pyntcloud import *

#

#
# nb = 20
# ratio = 2

class Filters:
    def statistical_filter_top(self, nb, ratio, show, top_point_cloud):

        # nb = 100
        # ratio = 2
        # show = 1
        # top_point_cloud ='pc_201407060711'

        table_name_top = top_point_cloud + '_dist_top'
        print(table_name_top)



        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute("""SELECT x,y,z FROM pc_processing.{} WHERE dist >0.035;""".format(table_name_top))  # 0.035
        data = cur.fetchall()

        # some temporary translation is required :(
        x = [i[0] - 580000 for i in data]
        y = [i[1] - 4275000 for i in data]
        z = [i[2] for i in data]

        points = pd.DataFrame({'x': x,'y': y,'z': z})

        points = PyntCloud(points)


        if os.path.isfile("tmp/top/" + table_name_top + "_1.pcd"):
            print('file exists')
            file3d = ("tmp/top/" + table_name_top)
            # add in open3d reader
            points = read_point_cloud(file3d + "_1.pcd")
            file3d = ("tmp/top/" + table_name_top + "_2")
        else:
            print('file exists not')
            file3d = str("tmp/top/" + table_name_top)
            points.to_file(file3d + '.ply')
            points = read_point_cloud(file3d + '.ply')



        # draw_geometries([points])
        # pcd = points
        voxel_down_pcd = points

        def display_inlier_outlier(cloud, ind):
            inlier_cloud = select_down_sample(cloud, ind)
            outlier_cloud = select_down_sample(cloud, ind, invert=True)

            print("Showing outliers (red) and inliers (gray): ")
            outlier_cloud.paint_uniform_color([1, 0, 0])
            inlier_cloud.paint_uniform_color([0.8, 0.8, 0.8])
            draw_geometries([inlier_cloud, outlier_cloud])

        # print("Every 5th points are selected")
        # uni_down_pcd = uniform_down_sample(pcd, every_k_points = 5)
        # draw_geometries([uni_down_pcd])

        print("Statistical outlier removal")
        result, ind = statistical_outlier_removal(voxel_down_pcd, nb_neighbors=nb, std_ratio=ratio)
        display_inlier_outlier(voxel_down_pcd, ind)
        if show == 1:
            draw_geometries([result])
        else:
            print('no show')
        print(type(result))
        print("writting pointcloud")
        write_point_cloud(file3d + '_1.pcd', result)
        print("pointcloud written")
        # return result

    def statistical_filter_base(self, nb, ratio, show, base_point_cloud):

        table_name_base = base_point_cloud + '_dist_base'
        print(table_name_base)

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute("""SELECT x,y,z FROM pc_processing.{} WHERE dist >0.035 ;""".format(table_name_base))  # 0.035
        data = cur.fetchall()

        # some temporary translation is required :(
        x = [i[0] - 580000 for i in data]
        y = [i[1] - 4275000 for i in data]
        z = [i[2] for i in data]

        points = pd.DataFrame({'x': x, 'y': y, 'z': z})

        points = PyntCloud(points)

        if os.path.isfile("tmp/base/" + table_name_base + "_1.pcd"):
            print('file exists')
            file3d = ("tmp/base/" + table_name_base)
            # add in open3d reader
            points = read_point_cloud(file3d + "_1.pcd")
            file3d = ("tmp/base/" + table_name_base + "_2")
        else:
            print('file exists not')
            file3d = str("tmp/base/" + table_name_base)
            print(file3d)
            points.to_file(file3d + '.ply')
            points = read_point_cloud(file3d + '.ply')

        # draw_geometries([points])
        pcd = points
        voxel_down_pcd = points

        def display_inlier_outlier(cloud, ind):
            inlier_cloud = select_down_sample(cloud, ind)
            outlier_cloud = select_down_sample(cloud, ind, invert=True)

            print("Showing outliers (red) and inliers (gray): ")
            outlier_cloud.paint_uniform_color([1, 0, 0])
            inlier_cloud.paint_uniform_color([0.8, 0.8, 0.8])
            draw_geometries([inlier_cloud, outlier_cloud])

        # print("Every 5th points are selected")
        # uni_down_pcd = uniform_down_sample(pcd, every_k_points = 5)
        # draw_geometries([uni_down_pcd])

        print("Statistical outlier removal")
        result, ind = statistical_outlier_removal(voxel_down_pcd, nb_neighbors=nb, std_ratio=ratio)
        display_inlier_outlier(voxel_down_pcd, ind)
        if show == 1:
            draw_geometries([result])
        else:
            print('no show')
        print(type(result))
        print("writting pointcloud")
        write_point_cloud(file3d + '_1.pcd', result)
        print("pointcloud written")
        # return result

    def save_point_cloud(self, schema, top_point_cloud, base_point_cloud):

        # top_point_cloud = 'pc_201407060711'
        # base_point_cloud = 'pc_201407061221'
        # schema = 'pc_processing'

        #
        # top_point_cloud = 'pc_201407061221'
        # base_point_cloud = 'pc_201407071201'

        table_name_top = top_point_cloud + '_filtered_top'
        table_name_base = base_point_cloud + '_filtered_base'

        filename_top = top_point_cloud + '_dist_top'
        filename_base = base_point_cloud + '_dist_base'

        path_top = ("tmp/top/" + filename_top + "_2_1.pcd")
        path_base = ("tmp/base/" + filename_base + "_2_1.pcd")

        top = read_point_cloud(path_top)
        base = read_point_cloud(path_base)

        t = np.asarray(top.points)
        b = np.asarray(base.points)

        # @@@@@@@@@ creation of a temporary holdiong table, this should be revised @@@@@@@@@
        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        print("connected")
        cur.execute("""DROP TABLE IF EXISTS pc_processing."pc_tmp_top" CASCADE;""".format(schema))
        print("connected2")
        query1 = sql.SQL("""CREATE TABLE {}.{} (
                        id serial NOT NULL,
                        x numeric,
                        y numeric,
                        z numeric,
                        geom geometry,
                        dist double precision);""").format(
            *map(sql.Identifier, (schema, "pc_tmp_top")))
        cur.execute(query1)
        print("connected")
        cur.execute("""DROP TABLE IF EXISTS {}."pc_tmp_base" CASCADE;""".format(schema))

        query2 = sql.SQL("""CREATE TABLE {}.{} (
                        id serial NOT NULL,
                        x numeric,
                        y numeric,
                        z numeric,
                        geom geometry,
                        dist double precision);""").format(
            *map(sql.Identifier, (schema, "pc_tmp_base")))
        cur.execute(query2)

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
                        x numeric,
                        y numeric,
                        z numeric,
                        dist numeric,
                        --x double precision,
                        --y double precision,
                        --z double precision,
                        geom geometry);
                """.format(schema, table_name_top))
        cur.close()
        conn.commit()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        print("connected")
        cur.execute("""DROP TABLE IF EXISTS {}.{} CASCADE;""".format(schema, table_name_base))
        cur.execute("""
                CREATE TABLE {}.{} (
                	    id serial NOT NULL,
                        --# x double precision,
                        --# y double precision,
                        --# z double precision,
                        x numeric,
                        y numeric,
                        z numeric,
                        dist numeric,
                        geom geometry);
                """.format(schema, table_name_base))
        cur.close()
        conn.commit()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()



        xi = [i[0] + 580000 for i in t]
        yi = [i[1] + 4275000 for i in t]
        zi = [i[2] for i in t]

        for i in range(len(xi)):
            cur.execute("""INSERT INTO pc_processing.pc_tmp_top (x,y,z) values (%s, %s, %s)""",(xi[i], yi[i], zi[i]))
            print(i)

        cur.close()
        conn.commit()

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()

        xi = [i[0] + 580000 for i in b]
        yi = [i[1] + 4275000 for i in b]
        zi = [i[2] for i in b]

        for i in range(len(xi)):
            cur.execute("""INSERT INTO pc_processing.pc_tmp_base (x,y,z) values (%s, %s, %s)""",(xi[i], yi[i], zi[i]))
            print(i)

        cur.close()
        conn.commit()



        # @@@@@@@@@ creation of a temporary holdiong table, this should be revised @@@@@@@@@
        # conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        # cur = conn.cursor()
        print("connected")

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()

        query3 = sql.SQL("""INSERT INTO {}.{} (x, y, z, dist)
            (SELECT x, y, z, dist FROM {}."pc_tmp_top");""").format(
            *map(sql.Identifier, (schema, table_name_base, schema)))
        print(query2.as_string(conn))
        print('query created')
        cur.execute(query3)
        # print(query2.as_string(conn))

        query4 = sql.SQL("""INSERT INTO {}.{} (x, y, z, dist)
            (SELECT x, y, z, dist FROM {}."pc_tmp_base");""").format(
            *map(sql.Identifier, (schema, table_name_base, schema)))
        print(query2.as_string(conn))
        print('query created')
        cur.execute(query4)
        # print(query2.as_string(conn))


        cur.close()
        conn.commit()
        # @@@@@@@@@ END @@@@@@@@@

        print('complete')



