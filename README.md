# MapReduce_1
处理学生成绩表，获取各班成绩最高（或前几）的学生
    涉及自定义分区（各班学生的数据使用同一个Reducer处理）、排序（降序）、分组（各班学生的数据在同一组）

学生表如下：
![学生表](https://github.com/JackFong/MapReduce_1/blob/main/picture/student%E8%A1%A8.png)


结果如下：
![结果表](https://github.com/JackFong/MapReduce_1/blob/main/picture/%E7%BB%93%E6%9E%9C.png)

过程分析：
