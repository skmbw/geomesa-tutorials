"""
    auth: bear
    date: 2016-04-13
    使用说明:使用之前必须安装好jnius,在安装jnius之前一定要确定java环境已经配好
    使用这个只需要把jar包拷贝到当前文件夹下，然后配置PACKAGE_CONFIGS

   通过import就可以直接使用 例如:

     import java

     java.python中的引用名

     也可以不用java这个名字，全凭个人喜好
"""

# 若要使用新的java类都需要配置这里
# PACKAGE_CONFIGS = [{'name': 'python中的引用名', 'package': '要导入java类必须是jar包里面有的 例如: com.***.***'}]
PACKAGE_CONFIGS = [{'name': 'jniusTest', 'package': 'com.xueershangda.JniusTest'}]

import os

global JAVA_INITED
JAVA_INITED = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + '/'


def IsSubString(SubStrList, Str):
    '''
    #判断字符串Str是否包含序列SubStrList中的每一个子字符串
    #>>>SubStrList=['F','EMS','txt']
    #>>>Str='F06925EMS91.txt'
    #>>>IsSubString(SubStrList,Str)#return True (or False)
    '''

    flag = True
    for substr in SubStrList:
        if not (substr in Str):
            flag = False
    return flag


def GetFileList(FindPath, FlagStr=[]):
    '''
    #获取目录中指定的文件名
    #>>>FlagStr=['F','EMS','txt'] #要求文件名称中包含这些字符
    #>>>FileList=GetFileList(FindPath,FlagStr) #
    '''

    import os

    FileList = []
    FileNames = os.listdir(FindPath)
    if len(FileNames) > 0:
        for fn in FileNames:
            if len(FlagStr) > 0:
                # 返回指定类型的文件名
                if IsSubString(FlagStr, fn):
                    fullfilename = os.path.join(FindPath, fn)
                    FileList.append(fullfilename)
            else:
                # 默认直接返回所有文件名
                fullfilename = os.path.join(FindPath, fn)
                FileList.append(fullfilename)
    # 对文件名排序
    if len(FileList) > 0:
        FileList.sort()
    return FileList


def getjars():
    return GetFileList(BASE_DIR, ['jar'])


def __importJars():
    global JAVA_INITED

    JAVA_INITED = True
    for jar in getjars():
        add_classpath(jar)
    from jnius import autoclass
    # import java

    for info in PACKAGE_CONFIGS:
        func = autoclass(info.get('package'))
        jniusTest = func()
        print(jniusTest.tos("3245"))
        ArrayList = autoclass("java.util.ArrayList")
        list = ArrayList()
        list.add('aa');
        print(list.get(0))
        # setattr(java, info.get('name'), func)


def add_classpath(path):
    """
    Appends items to the classpath for the JVM to use.
    Replaces any existing classpath, overriding the CLASSPATH environment variable.
    """

    import jnius_config
    # JDK 1.6以后好像可以不配置CLASSPATH了，现在为了这个配置上了。
    if ':' + path in os.environ['CLASSPATH']:
        return
    # os.environ['CLASSPATH'] += ':' + path
    jnius_config.add_classpath(path)


if not JAVA_INITED:
    __importJars()

if __name__ == "__main__":
    import java
    print(java.tos("22"))
