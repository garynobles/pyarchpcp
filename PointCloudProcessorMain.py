
# imports
import os
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5 import uic
import psycopg2

# local classes
import getPointCloud
import makeSample
import filterPointCloud
import viewPointCloud
import getPointDistance

en_voice_id = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\TTS_MS_EN-GB_HAZEL_11.0"
ru_voice_id = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\TTS_MS_EN-US_ZIRA_11.0"
david = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\TTS_MS_EN-US_DAVID_11.0"
brian = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\IVONA 2 Voice Brian22"
amy = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\IVONA 2 Voice Amy22"

voice = brian
speed = 50

schema = 'pc_processing'
top_point_cloud_gbl = ''
base_point_cloud_gbl = ''


error1 = "My memory banks are overloaded!"
greeting1 = ""
loaded = " loaded."
loaded2 = " has been loaded."
pc_loaded = "The point cloud has been loaded."
processing = "Processing complete for photobatch "
convexhull = "Redundant points removed."

# convexhull = "Erroneous points removed."

class MyWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(MyWindow, self).__init__()
        uic.loadUi('pyqtdesigner.ui', self)

        # top_point_cloud = None
        # base_point_cloud = None

        self.setWindowTitle('Pointcloud Processing 0.0.1')
        # self.btnTop.clicked.connect(self.top)
        # self.btnBase.clicked.connect(self.base)


        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE 
			   table_schema = 'public' AND 
			   table_name != 'geography_columns' AND
			   table_name != 'spatial_ref_sys' AND
			   table_name != 'raster_columns' AND
			   table_name != 'raster_overviews' AND
			   table_name != 'pointcloud_formats' AND
			   table_name != 'pointcloud_columns' AND
			   table_name != 'pcpatches' AND
			   table_name != 'geometry_columns'
               """)
        #
        # cur.execute("""select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';""")
        # tablelist = []
        # for table in cur.fetchall():
        #     tablelist.append(table)
        #
        # cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        tables = cur.fetchall()
        flat_list = [item for sublist in tables for item in sublist]

        cur.close()
        conn.close()
        self.comboTop.addItems(flat_list)
        self.comboBase.addItems(flat_list)

        # self.comboTop.addItems([
        #     'pc_201406151141',
        #     'pc_201406220908',
        #     'pc_201407031017',
        #     'pc_201407060711',
        #     'pc_201407061221'])
        # self.comboBase.addItems([
        #     'pc_201406220908',
        #     'pc_201407031017',
        #     'pc_201407060711',
        #     'pc_201407061221', 'pc_201407071201'])



        self.comboTop.activated.connect(self.top)
        self.comboBase.activated.connect(self.base)

        self.btnSample.clicked.connect(self.sample_bernoulli)

        self.btnNearDistTop.clicked.connect(self.near_dist_top)
        self.btnNearDistBase.clicked.connect(self.near_dist_base)
        self.btnConvexHull.clicked.connect(self.convex_hull)

        # self.spDistValueTop.clicked.connect(self.dist_value_top)
        # self.spDistValueBase.clicked.connect(self.dist_value_base)

        self.btnToGeom.clicked.connect(self.calculate_geometry)

        self.btnStatFilterTop.clicked.connect(self.stat_filter_top)
        self.btnResetFilterTop.clicked.connect(self.reset_filter_stat_top)

        self.btnStatFilterBase.clicked.connect(self.stat_filter_base)
        self.btnResetFilterBase.clicked.connect(self.reset_filter_stat_base)

        self.btnViewFilteredPointCloud.clicked.connect(self.view_filtered_point_clouds)

        self.btnSavePointCloud.clicked.connect(self.save_point_cloud)
        self.btnViewBase.clicked.connect(self.view_base)
        self.btnViewTop.clicked.connect(self.view_top)

        self.btnViewPointCloudsImported.clicked.connect(self.view_point_clouds_imported)
        self.btnViewPointCloudsDist.clicked.connect(self.view_point_clouds_dist)

        print(schema)

        # import pyttsx3
        # engine = pyttsx3.init()
        # en_voice_id = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\TTS_MS_EN-GB_HAZEL_11.0"
        # us_voice_id = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\TTS_MS_EN-US_ZIRA_11.0"
        #
        # # Use female English voice
        # engine.setProperty('voice', en_voice_id)
        # rate = engine.getProperty('rate')
        # engine.setProperty('rate', rate - 0)
        # engine.say("Welcome")
        # engine.runAndWait()





    def download(self):
        self.completed = 0

        while self.completed < 100:
            self.completed += 0.01
            self.progressBar.setValue(self.completed)

    def write(string):
        pass



    def top(self, index):

        top_point_cloud = self.comboTop.itemText(index)
        self.statusBar().showMessage(top_point_cloud + ' selected')

        global top_point_cloud_gbl
        top_point_cloud_gbl = top_point_cloud

        if self.chkTop.isChecked() == True:
            self.statusBar().showMessage('Importing Top PointCloud as: ' + top_point_cloud)
            print(top_point_cloud)
            getPointCloud.GetPointCloud.top(self, top_point_cloud, schema)
            self.statusBar().showMessage('Complete')

            import pyttsx3
            engine = pyttsx3.init()
            rate = engine.getProperty('rate')
            engine.setProperty('rate', rate - speed)
            engine.setProperty('voice', amy)
            try:
                engine.say(loaded)
            except IOError:
                print('error')
            finally:
                engine.runAndWait()
        else:
            print('Import disabled')




    def base(self, index):
        base_point_cloud = self.comboBase.itemText(index)

        global base_point_cloud_gbl
        base_point_cloud_gbl = base_point_cloud

        if self.chkBase.isChecked() == True:
            self.statusBar().showMessage('Importing Base PointCloud as: ' + base_point_cloud)
            print(base_point_cloud)
            getPointCloud.GetPointCloud.base(self, base_point_cloud, schema)
            self.statusBar().showMessage('Complete')

            import pyttsx3
            engine = pyttsx3.init()
            rate = engine.getProperty('rate')
            engine.setProperty('rate', rate - speed)
            engine.setProperty('voice', brian)
            try:
                engine.say(loaded)
            except IOError:
                print('error')
            finally:
                engine.runAndWait()
        else:
            pass

    def convex_hull(self):
        top_point_cloud = top_point_cloud_gbl
        base_point_cloud = base_point_cloud_gbl

        query = """
        DELETE FROM pc_processing.pc_201406151141_top top
        WHERE st_within(top.geom,
           (SELECT st_convexhull(st_collect(geom)) as geom
            FROM pc_processing.pc_201407060711_base
            )
        ) = false;
        """


        import pyttsx3
        engine = pyttsx3.init()
        rate = engine.getProperty('rate')
        engine.setProperty('rate', rate - speed)
        engine.setProperty('voice', amy)
        try:
            engine.say(convexhull)
        except IOError:
            print('error')
        finally:
            engine.runAndWait()


    def pre_clean(self):
        # This is needed to find the convex hull of the partnering pointcloud and remove points
        # outside of this and vice versa
        pass

    def sample_bernoulli(self):
        sample = self.spSample.value()
        top_point_cloud = top_point_cloud_gbl
        base_point_cloud = base_point_cloud_gbl

        print(sample)

        self.statusBar().showMessage('Importing Top PointCloud as: ' + top_point_cloud + ' & ' + base_point_cloud + ' Sample: ' + str(sample)+'%')
        makeSample.MakeSample.sample_bernoulli(self, schema, top_point_cloud, base_point_cloud, sample)
        self.statusBar().showMessage('complete')

    def near_dist_top(self):
        top_point_cloud = top_point_cloud_gbl
        base_point_cloud = base_point_cloud_gbl
        self.statusBar().showMessage('processing nearest points of ' + top_point_cloud + ' with ' + base_point_cloud)
        getPointDistance.GetPointDistance.calculate_nearest_top(self, schema, top_point_cloud, base_point_cloud)
        self.statusBar().showMessage('complete')



        import pyttsx3
        engine = pyttsx3.init()
        rate = engine.getProperty('rate')
        engine.setProperty('rate', rate - speed)
        engine.setProperty('voice', voice)
        try:
            engine.say(processing + top_point_cloud)
        except IOError:
            print('error')
        finally:
            engine.runAndWait()

    def view_top(self):

        import pyttsx3
        engine = pyttsx3.init()
        rate = engine.getProperty('rate')
        engine.setProperty('rate', rate - speed)
        engine.setProperty('voice', voice)
        try:
            engine.say(processing + " " + top_point_cloud_gbl)
        except IOError:
            print('error')
        finally:
            engine.runAndWait()

    def view_base(self):

        import pyttsx3
        engine = pyttsx3.init()
        rate = engine.getProperty('rate')
        engine.setProperty('rate', rate - speed)
        engine.setProperty('voice', voice)
        try:
            engine.say(processing + " " + base_point_cloud_gbl)
        except IOError:
            print('error')
        finally:
            engine.runAndWait()

    def near_dist_base(self):
        top_point_cloud = top_point_cloud_gbl
        base_point_cloud = base_point_cloud_gbl
        getPointDistance.GetPointDistance.calculate_nearest_base(self, schema, top_point_cloud, base_point_cloud)

        message2 = "Processing complete for photobatch" + base_point_cloud
        import pyttsx3
        engine = pyttsx3.init()
        en_voice_id = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\TTS_MS_EN-GB_HAZEL_11.0"
        # us_voice_id = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Speech\Voices\Tokens\TTS_MS_EN-US_ZIRA_11.0"

        # Use female English voice
        engine.setProperty('voice', en_voice_id)
        # rate = engine.getProperty('rate')
        # engine.setProperty('rate', rate - 0)
        engine.say(message2)
        engine.runAndWait()


    def dist_value_top(self):
        pass

    def dist_value_base(self):
        pass


    def calculate_geometry(self):
        # getPointDistance.GetPointDistance.to_geom(self)

        # top_point_cloud = 'pc_201407060711'
        # base_point_cloud = 'pc_201407061221'

        top_point_cloud = top_point_cloud_gbl
        base_point_cloud = base_point_cloud_gbl

        top = top_point_cloud + '_filtered_top'
        base = base_point_cloud + '_filtered_base'

        self.statusBar().showMessage('Generating Geometry...')

        conn = psycopg2.connect("dbname=kap_pointcloud host=localhost user=postgres password=Gnob2009")
        cur = conn.cursor()
        cur.execute(
            """UPDATE pc_processing.{} SET geom=ST_GeomFromText('POINT('||x||' '||y||' '||z||')',32635);""".format(top))
        cur.execute(
            """UPDATE pc_processing.{} SET geom=ST_GeomFromText('POINT('||x||' '||y||' '||z||')',32635);""".format(base))
        conn.commit()
        cur.close()
        print('Geometry Updated')
        self.statusBar().showMessage('Ready')




    def stat_filter_top(self):
        print('stat filter clicked')
        top_point_cloud = top_point_cloud_gbl
        print('pc= ' + top_point_cloud)
        nb = self.spNbTop.value()
        ratio = self.spRatioTop.value()
        # show = self.pcFiltered.isChecked()
        print('stop 1')
        if self.pcFilteredTop.isChecked() == True:
            show = 1
            print(show)
        else:
            show = 2
            print(show)
        print('stop 2')
        print(ratio)
        print('stop 3')
        filterPointCloud.Filters.statistical_filter_top(self, nb, ratio, show, top_point_cloud)
        print('stop 4')
        self.statusBar().showMessage('complete')

    def reset_filter_stat_top(self):
        file3d = top_point_cloud_gbl

        file1 = "tmp/top/" + file3d + "_dist_top.ply"
        file2 = "tmp/top/" + file3d + "_dist_top_1.pcd"
        file3 = "tmp/top/" + file3d + "_dist_top_2_1.pcd"

        if os.path.isfile(file1):
            os.remove(file1)
            print(file1)
        else:
            pass

        if os.path.isfile(file2):
            os.remove(file2)
            print(file2)
        else:
            pass

        if os.path.isfile(file3):
            os.remove(file3)
            print(file3)
        else:
            pass


        print("Filter Reset!")

    def stat_filter_base(self):
        print('stat filter clicked')
        base_point_cloud = base_point_cloud_gbl
        print('pc=' + base_point_cloud + ' test')
        nb = self.spNbBase.value()
        ratio = self.spRatioBase.value()
        # show = self.pcFiltered.isChecked()

        if self.pcFilteredBase.isChecked() == True:
            show = 1
            print(show)
        else:
            show = 2
            print(show)

        print(ratio)
        filterPointCloud.Filters.statistical_filter_base(self, nb, ratio, show, base_point_cloud)

        self.statusBar().showMessage('complete')

    def reset_filter_stat_base(self):
        file3d = base_point_cloud_gbl

        file1 = "tmp/base/" + file3d + "_dist_base.ply"
        file2 = "tmp/base/" + file3d + "_dist_base_1.pcd"
        file3 = "tmp/base/" + file3d + "_dist_base_2_1.pcd"

        if os.path.isfile(file1):
            os.remove(file1)
            print(file1)
        else:
            pass

        if os.path.isfile(file2):
            os.remove(file2)
            print(file2)
        else:
            pass

        if os.path.isfile(file3):
            os.remove(file3)
            print(file3)
        else:
            pass


        print("Filter Reset!")


    def save_point_cloud(self):
        print('save pointcloud clicked')
        top_point_cloud = top_point_cloud_gbl
        base_point_cloud = base_point_cloud_gbl
        filterPointCloud.Filters.save_point_cloud(self, schema, top_point_cloud, base_point_cloud)

    def list_point_clouds(self):
        pass





    def view_filtered_point_clouds(self):
        top_point_cloud = top_point_cloud_gbl
        base_point_cloud = base_point_cloud_gbl
        viewPointCloud.ViewPointCloud.view_point_clouds_filtered(self, top_point_cloud, base_point_cloud)




    def view_point_clouds_imported(self):
        top_point_cloud = top_point_cloud_gbl
        base_point_cloud = base_point_cloud_gbl
        viewPointCloud.ViewPointCloud.view_point_clouds_imported(self, top_point_cloud, base_point_cloud)

    def view_point_clouds_dist(self):
        top_point_cloud = top_point_cloud_gbl
        base_point_cloud = base_point_cloud_gbl
        viewPointCloud.ViewPointCloud.view_point_clouds_dist(self, top_point_cloud, base_point_cloud)



if __name__ == '__main__':
    import sys



    app = QtWidgets.QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec_())


