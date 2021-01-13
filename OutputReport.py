
# scientific environment
import numpy as np
import pandas as pd

# %matplotlib inline
# plt.style.use('ggplot')
# pd.set_option('display.min_row', 15)
# pd.set_option('display.max_columns', 50)
# pd.set_option('display.max_colwidth', 200)

# useful magic & package

# %load_ext autoreload
# %autoreload 2
# import ipywidgets
import itertools
import re
from datetime import datetime

# associative analysis
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import association_rules

# draw purchase path
from collections import deque
from graphviz import Digraph

# Excel输出
import xlsxwriter


from DataProcess import DataProcess
from OrderPattern import OrderPattern
from RebuyAnalysis import RebuyAnalysis
from AssociativeAnalysis import AssociativeAnalysis
from UserProfile import UserProfile
from PurchasePath import PurchasePath

################# 0 - 订单规律探索 #################
s = DataProcess('数据/测试数据.xlsx')
s.main()
####################################################

writer = pd.ExcelWriter(path='报表测试/报表测试.xlsx', engine='xlsxwriter')
wb = writer.book
wb.formats[0].set_font_name("Microsoft YaHei UI")
bold = wb.add_format({'bold': True})
pct_format = wb.add_format({'num_format': '0.0%'})

################# 1 - 订单规律探索 #################

module1 = OrderPattern(s.df, s.df_sub)

######## 1. 订单的月度/周内/日度分布 ########

a, b, c = module1.order_time_dist()

# 月度
srow = 9
a.to_excel(writer, sheet_name='1-订单规律探索', startrow=srow, startcol=1, index=False, float_format="%.1f")
ws1 = writer.sheets['1-订单规律探索']

ws1.write(f'B{srow-1}', '1-订单分布', bold)
ws1.write(f'B{srow}', '表1 订单的月度分布')

# 周内
srow += a.shape[0] + 3
ws1.write(f'B{srow}', '表2 订单的周内分布')
b.to_excel(writer, sheet_name=ws1.name, startrow=srow, startcol=1, index=True)

# 日度
srow += b.shape[0] + len(b.columns[0]) + 3
ws1.write(f'B{srow}', '图1 订单的每日分布')
c['dt'] = c['dt'].astype('str') # 把日期调成字符串格式
c.to_excel(writer, sheet_name='附1-订单日度分布', index=False)
wss1 = writer.sheets['附1-订单日度分布']
chart1 = wb.add_chart({'type': 'line'})
chart1.add_series({'categories': [wss1.name, 1, 0, c.shape[0], 0],
                  'values': [wss1.name, 1, 1, c.shape[0], 1]
                 })
chart1.set_x_axis({'name': 'date'})
chart1.set_y_axis({'name': 'sales'})
chart1.set_size({'width': 1200, 'height': 300})
ws1.insert_chart(f'B{srow + 1}', chart1)

######## 2. 订单价值分布 ########

a, b = module1.order_value_dist()

srow += 18
ws1.write(f'B{srow}', '2-订单价值分布', bold)

# 月份
srow += 1
ws1.write(f'B{srow}', '表3 订单价值分布（区分月份）')
a.to_excel(writer, sheet_name=ws1.name, startrow=srow, startcol=1, index=True)

# 区分CRM人群
srow += a.shape[0] + len(a.columns[0]) + 3
ws1.write(f'B{srow}', '表4 订单价值分布（区分CRM人群）' )
b.to_excel(writer, sheet_name=ws1.name, startrow=srow, startcol=1, index=True, float_format="%.3f")

srow += b.shape[0] + len(b.columns[0]) + 4

##### 3.各类别商品销售情况 #####

a, b = module1.channel_sales()
ws1.write(f'B{srow}', '3-各类别商品销售情况', bold)

# 全部类别
srow += 1
ws1.write(f'B{srow}', '表5 各类别销售情况')
a.to_excel(writer, sheet_name=ws1.name, startrow=srow, startcol=1, index=False, float_format="%.3f")

# 零售和餐饮销售额指数
srow += a.shape[0] + 3
ws1.write(f'B{srow}', '表6 各类别销售额指数变化（以第一个月为100）')
b.to_excel(writer, sheet_name=ws1.name, startrow=srow, startcol=1, index=True, float_format="%.0f")

srow += b.shape[0] + len(b.columns[0]) + 4

##### 4.餐饮人群和零售人群数量变化 #####

# a = module1.uv_type()
# ws1.write(f'B{srow}', '4-餐饮人群和零售人群数量变化', bold)

# srow += 1
# ws1.write(f'B{srow}', '表7 餐饮人群和零售人群数量变化比较')
# a.to_excel(writer, sheet_name=ws1.name, startrow=srow, startcol=1, index=False)

# srow += a.shape[0] + 4

##### 5. 消费者下单次数分布 #####
a, b = module1.order_num_dist()
ws1.write(f'B{srow}', '4-消费者累计下单次数分布及各次下单情况', bold)

srow += 1
ws1.write(f'B{srow}', '表7 消费者累计下单次数分布')
a.to_excel(writer, sheet_name=ws1.name, startrow=srow, startcol=1, index=True, float_format="%.3f")

srow += a.shape[0] + len(a.columns[0]) + 3
ws1.write(f'B{srow}', '表8 消费者各次下单订单数和总金额')
b.to_excel(writer, sheet_name=ws1.name, startrow=srow, startcol=1, index=True, float_format="%.0f")

srow += b.shape[0] + 1 + 4

#####  目录   #####
ws1.write('A2', '目录')
ws1.write('B2', '1-订单分布')
ws1.write('B3', '2-订单价值分布')
ws1.write('B4', '3-各类别商品销售情况')
ws1.write('B5', '4-消费者累计下单次数分布及各次下单情况')

######################################################

################# 2 - 品类复购分析 #################

module2 = RebuyAnalysis(s.df, s.df_sub)

##### 1. 各品类下单次数及复购率 #####
a = module2.rebuy_result(field='category')
srow = 7
a.to_excel(writer, sheet_name='2-品类复购分析', startrow=srow, startcol=1, index=False, float_format="%.3f")
ws2 = writer.sheets['2-品类复购分析']
ws2.write(f'B{srow - 1}', '1-各品类下单次数及复购率', bold)
ws2.write(f'B{srow}', '表1 各品类复购情况')

srow += a.shape[0] + 4

##### 2. 各品类复购周期分布 #####

a = module2.cate_rebuy_interval_dist()
ws2.write(f'B{srow}', '2-各品类复购周期分布', bold)

srow += 1
ws2.write(f'B{srow}', '表2-1 各品类复购周期分布')
a.to_excel(writer, sheet_name='2-品类复购分析', startrow=srow, startcol=1, index=True, float_format="%.3f")
srow += a.shape[0] + 3

#####  目录   #####
ws2.write('A2', '目录')
ws2.write('B2', '1-各品类下单次数及复购率')
ws2.write('B3', '2-各品类复购周期分布')

######################################################


################# 3 - 单品复购分析 #################

##### 1. 所有单品复购情况 #####
a = module2.rebuy_result(field='sku_id')

srow = 7
a.to_excel(writer, sheet_name='3-单品复购分析', startrow=srow, startcol=1, index=False, float_format="%.3f")
ws3 = writer.sheets['3-单品复购分析']
ws3.write(f'B{srow - 1}', '1-所有单品复购情况', bold)
ws3.write(f'B{srow}', '表1 所有单品复购情况')

srow += a.shape[0] + 4

##### 2. 爆发系数计算 #####
n_days = 2
a, b = module2.promo_outbreak_coeff(n=n_days)
ws3.write(f'B{srow}', '2-促销活动爆发系数', bold)

srow += 1
ws3.write(f'B{srow}', f'表2 促销类型爆发系数（往前推{n_days}天）')
a.to_excel(writer, sheet_name=ws3.name, startrow=srow, startcol=1, index=True, float_format="%.1f")
srow += a.shape[0] + 3

ws3.write(f'B{srow}', f'表3 促销活动爆发系数（往前推{n_days}天）')
b['起始日期'] = b['起始日期'].astype('str')
b.to_excel(writer, sheet_name=ws3.name, startrow=srow, startcol=1, index=True, float_format="%.1f")
srow += b.shape[0]  + 4

#####  目录   #####
ws3.write('A2', '目录')
ws3.write('B2', '1-所有单品复购情况')
ws3.write('B3', '2-促销活动爆发系数')

######################################################

################# 4 - 订单维度关联分析 #################

module3 = AssociativeAnalysis(s.df, s.df_sub)

##### 1. 品类关联分析 #####
srow = 16
a = module3.cate_associate_res(basket='order', min_sup=0.02, min_conf=0.2, min_lift=1)
a.to_excel(writer, sheet_name='4-订单维度关联分析', startrow=srow, startcol=1, index=False, float_format="%.3f")

ws4 = writer.sheets['4-订单维度关联分析']
ws4.write(f'B{srow - 1}', '1-不同品类之间的关联购买', bold)
ws4.write(f'B{srow}', '表1 不同品类之间的关联购买分析')

srow += a.shape[0] + 4

##### 2. 单品关联分析 #####
ws4.write(f'B{srow-1}', '2-不同单品之间的关联购买', bold)
ws4.write(f'B{srow}', '表2 不同单品之间的关联购买分析')
a = module3.sku_associate_res(basket='order', min_sup=0.01, min_conf=0.2, min_lift=1)
a.to_excel(writer, sheet_name=ws4.name, startrow=srow, startcol=1, index=False)

#####  目录   #####
ws4.write('A2', '目录')
ws4.write('B2', '0-概念说明')
ws4.write('B3', '1-同一订单中，不同品类之间的关联购买')
ws4.write('B4', '2-同一订单中，不同单品之间的关联购买')
# 概念说明
ws4.write('B7', '0-概念说明', bold)
ws4.write('B8', '支持度')
ws4.write('C8', '商品A和商品B同时出现在购物篮中的概率。 计算公式 = 同时含有商品A和商品B的订单数/总订单数')
ws4.write('B9', '置信度')
ws4.write('C9', '购买商品A的人中购买商品B的比例。计算公式 = 同时购买商品A和商品B的订单数/购买商品A的订单数')
ws4.write('C10', '商品A对商品B的置信度水平越高，购买商品A的顾客再购买B商品的可能性就越高')
ws4.write('B11', '提升度')
ws4.write('C11', '商品之间的亲密关系，也称兴趣度，反映了商品A的出现对于商品B被购买的影响程度。计算公式 = P(购买商品A且购买商品B) / [P(购买商品A) * P(购买商品B)]')
ws4.write('C12', '提升度越大，商品A和商品B之间的关联程度就越强。如果提升度为1，则顾客对于商品A和商品B的购买行为是完全独立的')

######################################################

################# 5 - 用户维度关联分析 #################

##### 1. 品类关联分析 #####
srow = 15
a = module3.cate_associate_res(basket='user', min_sup=0.01, min_conf=0.25, min_lift=1)
a.to_excel(writer, sheet_name='5-用户维度关联分析', startrow=srow, startcol=1, index=False, float_format="%.3f")

ws5 = writer.sheets['5-用户维度关联分析']
ws5.write(f'B{srow - 1}', '1-同一用户，不同品类之间的关联购买', bold)
ws5.write(f'B{srow}', '表1 不同品类之间的关联购买分析')

srow += a.shape[0] + 4

##### 2. 单品关联分析 #####
ws5.write(f'B{srow - 1}', '2-不同单品之间的关联购买', bold)
ws5.write(f'B{srow}', '表2 不同单品之间的关联购买分析')
a = module3.sku_associate_res(basket='user', min_sup=0.01, min_conf=0.25, min_lift=1)
a.to_excel(writer, sheet_name=ws5.name, startrow=srow, startcol=1, index=False)

#####  目录   #####
ws5.write('A2', '目录')
ws5.write('B2', '0-概念说明')
ws5.write('B3', '1-同一用户，不同品类之间的关联购买')
ws5.write('B4', '2-同一用户，不同单品之间的关联购买')

ws5.write('B7', '0-概念说明', bold)
ws5.write('B8', '支持度')
ws5.write('C8', '商品A和商品B同时出现在购物篮中的概率。 计算公式 = 同时含有商品A和商品B的订单数/总订单数')
ws5.write('B9', '置信度')
ws5.write('C8', '购买商品A的人中购买商品B的比例。计算公式 = 同时购买商品A和商品B的订单数/购买商品A的订单数')
ws5.write('C9', '商品A对商品B的置信度水平越高，购买商品A的顾客再购买B商品的可能性就越高')
ws5.write('B10', '提升度')
ws5.write('C10', '商品之间的亲密关系，也称兴趣度，反映了商品A的出现对于商品B被购买的影响程度。计算公式 = P(购买商品A且购买商品B) / [P(购买商品A) * P(购买商品B)]')
ws5.write('C11', '提升度越大，商品A和商品B之间的关联程度就越强。如果提升度为1，则顾客对于商品A和商品B的购买行为是完全独立的')

######################################################

# writer.save()

################# 6 - 地域及RFM分层 #################
module4 = UserProfile(s.df, s.df_sub)

##### 1. CRM和非CRM人群对比 #####
srow = 10
a = module4.crm_analysis()
a.to_excel(writer, sheet_name='6-地域及RFM分层', startrow=srow, startcol=1, index=False)

ws6 = writer.sheets['6-地域及RFM分层']
ws6.write(f'B{srow - 1}', '1-CRM和非CRM人群对比', bold)
ws6.write(f'B{srow}', '表1 CRM和非CRM人群下单情况对比')

srow += a.shape[0] + 4

##### 2. RFM分层 #####
a, (r_ref, f_ref, m_ref, end_dt) = module4.rfm_analysis()
ws6.write(f'B{srow - 1}', '2-RFM分层', bold)
ws6.write(f'B{srow}', '阈值设置：')
ws6.write(f'B{srow + 1}', '最近一次购买距今时间（平均值）/R')
ws6.write(f'C{srow + 1}', f'{int(r_ref)}天')
ws6.write(f'D{srow + 1}', f'基于时间尾端{end_dt}计算')
ws6.write(f'B{srow + 2}', '历史购买总次数（平均值）/F')
ws6.write(f'C{srow + 2}', f'{int(f_ref)}次')
ws6.write(f'B{srow + 3}', '历史购买总金额（平均值）/M')
ws6.write(f'C{srow + 3}', f'{int(m_ref)}元')

srow += 5
ws6.write(f'B{srow}', '表2 消费者RFM分层情况') # 可能需要设置一下表格宽度 ！！！
a.to_excel(writer, sheet_name=ws6.name, startrow=srow, startcol=1, index=True, float_format="%.3f")
srow += a.shape[0] + len(a.columns[0]) + 6

##### 3. 地域分布 #####

ws6.write(f'B{srow - 2}', '3-地域分布', bold)
ws6.write(f'B{srow - 1}', '3.1-地域购买情况', bold)
ws6.write(f'B{srow}', '表3 各省购买情况')

a = module2.rebuy_result(field='province')
a.to_excel(writer, sheet_name=ws6.name, startrow=srow, startcol=1, index=False, float_format="%.3f")
srow += a.shape[0] + 3

ws6.write(f'B{srow}', '表4 各城市级别购买情况')
a = module2.rebuy_result(field='tier')
a.to_excel(writer, sheet_name=ws6.name, startrow=srow, startcol=1, index=False, float_format="%.3f")
srow += a.shape[0] + 5

##### 4. 地域品类喜好 #####
ws6.write(f'B{srow - 1}', '3.2-地域品类喜好', bold)
a, b = module4.province_cate_favor()

ws6.write(f'B{srow}', '表5 店铺总体品类订单数占比')
a.to_excel(writer, sheet_name=ws6.name, startrow=srow, startcol=1, index=False, float_format="%.3f")
srow += a.shape[0] + 3

ws6.write(f'B{srow}', '表6 各省品类订单数占比')
b.to_excel(writer, sheet_name=ws6.name, startrow=srow, startcol=1, index=True, float_format="%.3f")

#####  目录   #####
ws6.write('A2', '目录')
ws6.write('B2', '1-CRM和非CRM人群对比')
ws6.write('B3', '2-RFM分层')
ws6.write('B4', '3-地域分布')
ws6.write('B5', '    3.1-地域购买情况')
ws6.write('B6', '    3.2-地域品类喜好')

######################################################

writer.save()

##### 购买路径 #####
module5 = PurchasePath(s.df, s.df_sub)
module5.main()
