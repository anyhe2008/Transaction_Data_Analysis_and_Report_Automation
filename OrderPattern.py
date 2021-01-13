
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

class OrderPattern:
    """
    1. 订单的月度/周内/每日分布
    2. 订单价值分布
    3. 各类别商品销售情况
    4. 零售人群和餐饮人群数量变化
    5. 消费者下单次数分布
    """
    def __init__(self, df, df_sub):
        self.df = df
        self.df_sub = df_sub

    def order_time_dist(self):
        """
        订单月度分布
        """
        # 月度分布
        res_monthly = self.df.groupby('month').agg(
            订单数 = ('order_id', 'count'),
            总销售额 = ('order_value', 'sum'),
            订单平均价值 = ('order_value', 'mean'),
            每单购买件数 = ('goods_num', 'mean'),
        ).astype({'总销售额':'int64', '订单平均价值':'int64'}).reset_index()
        # 周内分布【区分CRM人群】
        res_dayofweek = self.df[(self.df['promo_type'] == '平日')
                                ].groupby(['is_crm', 'day_of_week', 'dt']).agg(订单数=('order_id', 'count'),
                                                                          销售额=('order_value','sum')).reset_index()
        res_dayofweek = res_dayofweek.groupby(['is_crm', 'day_of_week']).agg(
            频次=('day_of_week', 'count'),
            总订单数=('订单数', 'sum'),
            总销售额=('销售额', 'sum'),
            日均订单数=('订单数', 'mean'),
           日均销售额=('销售额', 'mean')
        ).astype('int64')
        res_dayofweek = res_dayofweek.unstack(0).stack(0).unstack(-1)
        # 日度分布 数据量太多，只导出图，不导出表格
        res_daily = self.df.groupby('dt').agg(销售额 = ('order_value', 'sum')).reset_index()

        return res_monthly, res_dayofweek, res_daily

    def order_value_dist(self):
        bins = [0, 18, 28, 39, 50, 60, 80, 100, 130, 200, 20000]
        df_tmp = self.df.copy(deep=True)
        df_tmp['订单价值区间'] = pd.cut(df_tmp['order_value'], bins=bins, right=False, include_lowest=True)
        res_month = df_tmp.groupby(['month', '订单价值区间']).agg(订单数=('order_id', 'count')).unstack(0)
        res_month[('订单数', 'Total')] = res_month.sum(axis=1)
        # 区分CRM人群
        res_crm = df_tmp.groupby(['is_crm', '订单价值区间']).agg(订单数=('order_id', 'count')).reset_index()
        res_crm['占比'] = res_crm.groupby(['is_crm']).transform(lambda x: x / x.sum())
#         res_crm['占比'] = res_crm['占比'].apply(lambda x: '{:.1%}'.format(x))
        res_crm = res_crm.set_index(['is_crm', '订单价值区间']).unstack(0).stack(0).unstack(-1)
        res_crm = res_crm[[( 'CRM', '订单数'),( 'CRM',  '占比'),('非CRM', '订单数'), ('非CRM',  '占比')]]
        return res_month, res_crm

    def channel_sales(self):
       # 只取单类别订单
        df_one_channel = self.df_sub[self.df_sub['channel_num'] == 1].drop_duplicates('order_id')
        res_channel_ttl = df_one_channel.groupby('channel').agg(
            订单数 = ('order_id', 'count'),
            销售额 = ('order_value', 'sum'),
            每单购买件数 = ('goods_num', 'mean'),
            订单平均价值 = ('order_value', 'mean')
        )
        res_channel_ttl['订单数占比'] = res_channel_ttl['订单数'] / sum(res_channel_ttl['订单数'])
        res_channel_ttl['GMV占比'] = res_channel_ttl['销售额'] / sum(res_channel_ttl['销售额'])
        cols = ['订单数', '订单数占比', '销售额', 'GMV占比', '每单购买件数', '订单平均价值']
        res_channel_ttl = res_channel_ttl[cols].reset_index()
        res_channel_ttl[['销售额', '订单平均价值']] = res_channel_ttl[['销售额', '订单平均价值']].astype('int64')

        # 销售额指数
        res_channel_monthly = df_one_channel.groupby(['channel', 'month']).agg(销售额=('order_value', 'sum'))
        res_channel_monthly = res_channel_monthly.unstack(0)
        for col_name in res_channel_monthly.columns: ## 后面试试有没有更简单的方法！！！
            a, channel = col_name
            res_channel_monthly[('销售额指数', channel)] = res_channel_monthly[col_name] / res_channel_monthly[col_name][0] * 100

        return res_channel_ttl, res_channel_monthly

    def uv_type(self):
        # 计算各月的累计餐饮人群、零售人群、交叉人群
        res_uv_type = pd.DataFrame(columns=['month', '餐饮人群', '餐饮人群占比', '零售人群', '零售人群占比', '交叉人群', '交叉人群占比', '总人数'])
        start_month = min(self.df['month'])
        end_month = max(self.df['dt'])
        for mon in pd.date_range(start=start_month, end=end_month, freq='M'):
            mon = str(mon.strftime('%Y-%m-01'))
            tmp = self.df_sub[(self.df_sub['channel'].isin(['RT', 'FS'])) & (self.df_sub['month'] <= mon)].drop_duplicates(['user_id', 'channel'])
            tmp = tmp.groupby(['user_id']).agg(channel_num=('channel', 'nunique'), channel=('channel', 'max'))
            total_uv = tmp.shape[0]
            fs_uv = tmp[(tmp['channel'] == 'FS') & (tmp['channel_num'] == 1)].shape[0]
            rt_uv = tmp[(tmp['channel'] == 'RT') & (tmp['channel_num'] == 1)].shape[0]
            cross_uv = tmp[tmp['channel_num'] == 2].shape[0]
            res_uv_type = res_uv_type.append({
                'month': mon,
                '餐饮人群': fs_uv,
                '餐饮人群占比': fs_uv / total_uv,
                '零售人群': rt_uv,
                '零售人群占比': rt_uv / total_uv,
                '交叉人群': cross_uv,
                '交叉人群占比': cross_uv / total_uv,
                '总人数': total_uv
            }, ignore_index=True)
        return res_uv_type

    def order_num_dist(self):
        # 累计下单次数分布
        max_order_num = self.df['nth_order'].max() + 1
        res_order_num = self.df[['user_id', 'nth_order', 'is_crm']].groupby(['user_id', 'is_crm']).agg(
            累计下单次数 = ('nth_order', 'max')
        ).reset_index()
        res_order_num['累计下单次数分布'] = pd.cut(res_order_num['累计下单次数'], bins=[0, 1, 2, 3, 4, 5, 6, max_order_num], labels=[1, 2, 3, 4, 5, 6, '7次及以上'])
        res_order_num = res_order_num.groupby(['is_crm', '累计下单次数分布']).agg(人数=('user_id', 'count')).unstack(0)
        res_order_num[('人数', '合计')] = res_order_num.sum(axis=1)
        for col_name in res_order_num.columns: ## 后面试试有没有更简单的方法！！！
            a, b = col_name
            res_order_num[('占比', b)] = res_order_num[col_name] / res_order_num[col_name].sum()

        # 各次下单订单数和总金额
        res_nth_order = self.df.groupby(['nth_order', 'is_crm']).agg(
            订单数 = ('order_id', 'count'),
            总金额 = ('order_value', 'sum')
        ).reset_index()
        res_nth_order['第n次下单'] = pd.cut(res_nth_order['nth_order'], bins=[0, 1, 2, 3, 4, 5, 6, 210], labels=[1, 2, 3, 4, 5, 6, '7及以上'])
        res_nth_order = res_nth_order.groupby(['第n次下单', 'is_crm']).agg(
            订单数 = ('订单数', 'sum'),
            总金额 = ('总金额', 'sum')
        )
        res_nth_order['订单平均价值'] = (res_nth_order['总金额'] / res_nth_order['订单数'])
        res_nth_order = res_nth_order.stack().unstack(0).unstack(0).stack(-1)

        return res_order_num, res_nth_order
