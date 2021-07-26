"""
Windows文件系统下修改训练集路径下文件名,将所有文件加上上一级目录名前缀,并使用'-'符号进行拼接
"""
import os
path = input("请输入需要修改文件名的文件路径(以'\'为分隔符)：")

# 获取该目录下所有文件名，存入列表
fileList = os.listdir(path)
# 获取当前文件夹名
temp = path.split('\\')
folderName = temp[len(temp)-1]

n = 0
# 对所有文件名进行遍历
for file in fileList:
    # 判断是否是文件
    if os.path.isfile(os.path.join(path, file)) == True:
        # 获取旧文件名
        oldFileName = path+os.sep+file
        # 设置新的文件名
        newFileName = path+os.sep+folderName+"-"+file
        # 修改文件名
        os.rename(oldFileName, newFileName)

print("Change finished!")
