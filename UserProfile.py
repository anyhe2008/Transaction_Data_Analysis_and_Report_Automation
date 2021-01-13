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

class UserProfile:
    def __init__(self, df, df_sub):
        self.df = df
        self.df_sub = df_sub
        self.df_sort = df.sort_values(['user_id', 'order_time'])

    def crm_analysis(self):
        res_crm = self.df.groupby('user_id').agg(
            is_crm = ('is_crm', 'max'),
            order_value = ('order_value', 'sum'),
            order_count = ('order_id', 'count'),
            is_rebuy = ('nth_order', 'max')
        ).reset_index()
        res_crm['is_rebuy'] = res_crm['is_rebuy'] > 1
        res_crm = res_crm.groupby('is_crm').agg(
            总人数 = ('user_id', 'count'),
            总订单数 = ('order_count', 'sum'),
            人均订单数 = ('order_count', 'mean'),
            人均累计购买金额 = ('order_value', 'mean'),
            复购率 = ('is_rebuy', 'mean')
        )
        return res_crm

    def rfm_analysis(self, r_ref=None, f_ref=None, m_ref=None, output_file=True):
        """
        parameters:
        - r_ref: 最近一次购买距今时间/R
        - f_ref: 历史购买总次数/F
        - m_ref: 历史购买总金额/M
        - output_file: 是否导出每个用户的RFM标签
        注：如果r_ref/f_ref/m_ref为None，则采取该时间段内的平均值
        """

        def rfm_tag(cols): # 给用户打上RFM标签
            r, f, m = cols[0], cols[1], cols[2] # recency, frequency, money
            if r <= r_ref and f > f_ref and m > m_ref:
                return '重要价值客户'
            elif r > r_ref and f > f_ref and m > m_ref:
                return '重要保持客户'
            elif r <= r_ref and f <= f_ref and m > m_ref:
                return '重要发展客户'
            elif r > r_ref and f <= f_ref and m > m_ref:
                return '重要挽留客户'
            elif r <= r_ref and f > f_ref and m <= m_ref:
                return '潜力客户'
            elif r <= r_ref and f <= f_ref and m <= m_ref:
                return '有推广价值新客'
            elif r > r_ref and f > f_ref and m <= m_ref:
                return '一般维持客户'
            elif r > r_ref and f <= f_ref and m <= m_ref:
                return '流失客户'

        df_rfm = self.df_sort.groupby('user_id').agg(
            is_crm=('is_crm', 'max'),
            order_num=('order_id', 'count'),
            order_value=('order_value', 'sum'),
            recent_order_dt=('dt', 'last'),
        ).reset_index()

        end_dt = pd.Timestamp(max(self.df['dt']))
        df_rfm['recency'] = (end_dt + pd.DateOffset(1) - df_rfm['recent_order_dt']).dt.days

        # 打标签
        if not r_ref or not f_ref or not m_ref:
            r_ref = df_rfm['recency'].mean()
            f_ref = df_rfm['order_num'].mean()
            m_ref = df_rfm['order_value'].mean()
        df_rfm['RFM标签'] = df_rfm[['recency', 'order_num', 'order_value']].apply(rfm_tag, axis=1)
        df_rfm[["最近一次购买距今时间/R", "历史购买总次数/F", "历史购买总金额/M"]] = (df_rfm[['recency', 'order_num', 'order_value']] > [r_ref, f_ref, m_ref])
        df_rfm.replace({False: '低于平均值', True:'高于平均值'}, inplace=True)

        res_rfm = df_rfm.groupby(['RFM标签', "最近一次购买距今时间/R", "历史购买总次数/F", "历史购买总金额/M", 'is_crm']).agg(人数=('user_id', 'count'))
        res_rfm = res_rfm.unstack(-1)
        idx = [
            ('重要价值客户', '低于平均值', '高于平均值', '高于平均值'),
            ('重要保持客户', '高于平均值', '高于平均值', '高于平均值'),
            ('重要发展客户', '低于平均值', '低于平均值', '高于平均值'),
            ('重要挽留客户', '高于平均值', '低于平均值', '高于平均值'),
            ('潜力客户', '低于平均值', '高于平均值', '低于平均值'),
            ('有推广价值新客', '低于平均值', '低于平均值', '低于平均值'),
            ('一般维持客户', '高于平均值', '高于平均值', '低于平均值'),
            ('流失客户', '高于平均值', '低于平均值', '低于平均值')
        ]
        res_rfm = res_rfm.reindex(index=idx)
        res_rfm[('人数', '合计')] = res_rfm.sum(axis=1)
        res_rfm[('占比', 'CRM')] = res_rfm[('人数', 'CRM')] / res_rfm[('人数', 'CRM')].sum()
        res_rfm[('占比', '非CRM')] = res_rfm[('人数', '非CRM')] / res_rfm[('人数', '非CRM')].sum()
        res_rfm[('占比', '合计')] = res_rfm[('人数', '合计')] / res_rfm[('人数', '合计')].sum()

        end_dt = end_dt.strftime('%Y-%m-%d')
        if output_file:
            df_rfm[['user_id', 'is_crm', 'recency', 'order_num', 'order_value', 'RFM标签']].to_excel(f"报表测试/消费者RFM标签-截止至{end_dt}.xlsx", index=False)

        return res_rfm,(r_ref, f_ref, m_ref, end_dt)

    def province_cate_favor(self):
        # 店铺总体
        df_tmp = self.df_sub.loc[self.df_sub['category'].isin(['新奥尔良料', '番茄酱', '世界风味酱', '锅物'])].drop_duplicates(['category', 'order_id'])
        res_store = df_tmp.groupby('category').agg(订单数=('order_id', 'count')).reset_index()
        res_store['订单占比'] = res_store['订单数'] / self.df.shape[0]

        # 各省情况
        tmp = self.df[self.df['province'].notnull()].groupby('province').agg(订单总数=('order_id', 'count'))
        res_province = df_tmp.groupby(['province', 'category']).agg(订单占比=('order_id', 'count')).unstack(-1).div(tmp['订单总数'], axis=0)
        res_province['订单总数'] = tmp['订单总数']
        return res_store, res_province
