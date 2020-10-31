# MapReduce_1
处理学生成绩表，获取各班成绩最高（或前几）的学生。
涉及自定义分区（各班学生的数据使用同一个Reducer处理）、排序（降序）、分组

学生表、结果如下：



![学生表](https://github.com/JackFong/MapReduce_1/blob/main/picture/student%E8%A1%A8.png)
![结果表](https://github.com/JackFong/MapReduce_1/blob/main/picture/%E7%BB%93%E6%9E%9C.png)


过程分析：

自定义StudentBean类，声明变量并序列化、定义成绩降序的排序规则、指定toString输出

Map阶段：

k_In： 一行文本数据末尾到开头的偏移量（起始为0）

v_In： 一行文本数据 <18001,Bryant,88.1>

k_Out： 需要判断（分区、排序、分组）的自定义类变量<班级号,null,成绩> 不让姓名做为k值

v_Out： 同v_In


Shuffle阶段：

分区：利用班级号除以最大整形值再取模，保证相同班级的学生数据在同一分区，否则无法进行排序

排序： 根据自定义类的排序规则进行排序

分组： 由于k已经是班级号+成绩，默认规则是班级号和成绩完全相同作为同一组，因此自定义分组将只要班级号相同的数据分到同一组


Reducer阶段：

k2_In: 自定义类变量<班级号,null,成绩>

v2_In：一行文本数据 <18001,Bryant,88.1>

目标结果只需要处理后的值

K2_Out： 同v2_In

v2_Out： NullWritable（只充当占位符）

定义循环结构，要求只需要成绩最高的学生数据，更改条件可获取更多数据（前几名）


最后：

创建job任务
