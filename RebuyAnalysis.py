
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

class RebuyAnalysis:
    def __init__(self, df, df_sub):
        self.df = df
        self.df_sub = df_sub
        self.df_sub_sorted = df_sub.sort_values(['user_id', 'order_time'])
        self.store_uv = df_sub['user_id'].nunique()
        self.promo_file = '数据/味好美活动日历.xlsx'

    def compute_rebuy_interval(self, df):
        df = df.drop_duplicates('order_id')
        df = df.assign(next_dt = df.groupby('user_id')['dt'].shift(-1, axis=0))
        df['rebuy_interval'] = (df['next_dt'] - df['dt']).dt.days.astype('Int64')
        return df

    def rebuy_index(self, df):
        """
        本品复购，df只是本品的订单数据
        本品可以是品类、sku、省份、城市级别
        """
        df = df.drop_duplicates('order_id')
        order_num = df.shape[0] # 订单总数
        aov = int(df['order_value'].mean()) # 订单平均价值
        rebuy_interval_median = np.nan # 复购周期中位数
        if df[df['rebuy_interval'].notnull()].shape[0] > 0:
            rebuy_interval_median = int(df['rebuy_interval'].median())
        buy_uv = df['user_id'].nunique() # 购买人数
        rebuy_uv = df.loc[df['next_dt'].notnull(), 'user_id'].nunique() #本品复购人数
        rebuy_rate = '{:.4f}'.format(rebuy_uv / buy_uv)

        rebuy_dict = {'订单总数': order_num, '订单平均价值':aov, '下单人数': buy_uv, '下单人数占比': buy_uv/self.store_uv,
                      '复购人数': rebuy_uv, '复购率': rebuy_rate, '复购周期中位数': rebuy_interval_median}
        return rebuy_dict

    def joint_rebuy_index(self, df, field='category'):
        """
        计算连带复购（即复购的东西可以是非本品）人数，df应该是所有品类的订单数据
        field可选：category, sku_id, province, tier
        """
        df = self.compute_rebuy_interval(df)
        df = df.drop_duplicates([field, 'user_id'], keep='first')
        df = df[df['next_dt'].notnull()]
        df = df.groupby(field).agg(连带复购人数=('user_id', 'count')).reset_index()
        return df

    def rebuy_result(self, field='category'):
        """
        field可选：category, sku_id, province, tier
        """
        cols = ['订单总数','订单平均价值', '下单人数','下单人数占比', '复购人数', '复购率', '复购周期中位数']
        if field == 'sku_id':
            cols = ['sku_id', 'title', 'channel', 'category'] + cols
            field_range = self.df_sub[['sku_id', 'title', 'channel', 'category']].drop_duplicates().reset_index(drop=True).astype({'sku_id':'int64'})
        else:
            cols = [field] + cols
            field_range = pd.DataFrame(self.df_sub.loc[self.df_sub[field].notnull(), field].drop_duplicates().reset_index(drop=True))

        res_rebuy = pd.DataFrame(columns = cols)
        # 本品复购
        for _, row in field_range.iterrows():
            tmp = row[field]
            df_tmp = self.compute_rebuy_interval(self.df_sub_sorted.loc[self.df_sub_sorted[field] == tmp])
            rebuy_dict = self.rebuy_index(df_tmp)
            rebuy_dict[field] = tmp
            if field == 'sku_id':
                for e in ['title', 'channel', 'category']:
                    rebuy_dict[e] = row[e]
            res_rebuy = res_rebuy.append(rebuy_dict, ignore_index=True)
        # 连带复购
        if field in ['category', 'sku_id', 'channel']:
            res_joint_rebuy = self.joint_rebuy_index(self.df_sub_sorted, field=field)
            res_rebuy = res_rebuy.merge(res_joint_rebuy, on=field, how='left')
            res_rebuy['连带复购率'] = res_rebuy['连带复购人数'] / res_rebuy['下单人数']
            res_rebuy = res_rebuy.rename(columns={'复购率':'本品复购率', '复购人数':'本品复购人数', '复购周期中位数':'本品复购周期中位数'})
        res_rebuy = res_rebuy.sort_values('订单总数', ascending=False)
        return res_rebuy

    def cate_rebuy_interval_dist(self):
        res_num = pd.DataFrame()
        for cate in self.df['category'].unique():
            df_cate = self.compute_rebuy_interval(self.df_sub_sorted[self.df_sub_sorted['category'] == cate])
            df_cate['下单时间间隔区间'] \
                    = pd.cut(df_cate.loc[df_cate['rebuy_interval'].notnull(), 'rebuy_interval'], [-1, 0,7,15,30,60,90,180,365,600],
                             labels=[0, '1-7', '7-15', '15-30', '30-60', '60-90', '90-180', '180-365', '365-以上'])
            tmp = pd.DataFrame(df_cate['下单时间间隔区间'].value_counts()).reset_index()
            tmp.columns=['下单时间间隔区间', cate]
            tmp['下单时间间隔区间'] = tmp['下单时间间隔区间'].astype('str')
            tmp = tmp.set_index('下单时间间隔区间')
            res_num = res_num.append(tmp.T, sort=False)
        res_num['总订单数'] = res_num.sum(axis=1)
        # 计算百分比
        res_pct = res_num.div(res_num['总订单数'], axis=0)
        res_pct['总订单数'] = res_num['总订单数']
        return res_pct

    def promo_outbreak_coeff(self, n = 30):
        total_days = len(self.df['dt'].unique())
        if n > total_days:
            print("订单日期覆盖数过少，请增加订单或调小参数！")
            return

        def process_promo_info():
            promo_info = pd.read_excel(self.promo_file)
            promo_info['dt'] = promo_info[['起始日期', '终止日期']].apply(lambda x: pd.date_range(start=x[0], end=x[1], freq='D'), axis=1)
            promo_info['dt'] = promo_info['dt'].apply(lambda x: [datetime.strftime(e,'%Y-%m-%d') for e in x])
            promo_info['天数'] = promo_info['dt'].apply(len)
            promo_info = promo_info.explode('dt')
            promo_info = promo_info.rename(columns={
                '促销活动': 'promotion',
                '促销类型': 'promo_type'
            })[['dt', 'promotion', 'promo_type', '起始日期', '终止日期', '天数']].reset_index(drop=True)
            promo_info['dt'] = promo_info['dt'].astype('datetime64[ns]')
            return promo_info[['dt', 'promotion', 'promo_type', '起始日期', '天数']]

        def compute_outbreak_coeff(promo_info, df_sub, n_days=7, sku_id=None):
            """
            - df_sub: 订单明细
            - n_days: 往前推几天，默认7天
            - sku_id：如果sku_id=None，则计算全店的爆发系数
            """
            # 订单数据处理
            if sku_id:
                df = df_sub.loc[df_sub['sku_id'] == sku_id].drop_duplicates('order_id')
            else:
                df = df_sub.drop_duplicates('order_id')
            df = df.groupby(['dt', 'promotion', 'promo_type']).agg(订单数 = ('order_id', 'count')).reset_index()

            # 标记促销
            df = df.merge(promo_info, on=['dt', 'promotion', 'promo_type'], how='left')
            df['起始日期'].fillna(df['dt'], inplace=True)
            df.loc[df['promotion'] == '无', 'promotion'] = np.nan
            df.loc[df['promo_type'] == '平日', 'promo_type'] = np.nan

            # 计算每一天往后n天的日均订单，存到dict里
            df['dt'] = df['dt'].astype('str')
            daily_order_list = df[['dt', '订单数']].values.tolist()
            daily_order_mean_dict = {}
            sum_tmp = sum(daily_order_list[i][1] for i in range(n_days))
            daily_order_mean_dict[daily_order_list[0][0]] = sum_tmp / n_days
            for i in range(n_days, len(daily_order_list)):
                sum_tmp += daily_order_list[i][1]
                sum_tmp -= daily_order_list[i - n_days][1]
                start_dt = daily_order_list[i - n_days + 1][0]
                daily_order_mean_dict[start_dt] = sum_tmp / n_days

            # 往前推n天日均
            df['dt'] = df['dt'].astype('datetime64[ns]')
            df['往前推n天'] = df['起始日期'] - pd.to_timedelta(n_days, unit='d')
            df.loc[df['往前推n天'].isna(), '往前推n天'] = df['dt'] - pd.to_timedelta(n_days, unit='d')
            df['往前推n天'] = df['往前推n天'].apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))
            df['前n天日均'] = df['往前推n天'].map(lambda x: daily_order_mean_dict.get(x, np.nan))

            # 计算爆发系数
            promo_daily_order = df.groupby(['起始日期', 'promotion', 'promo_type']).agg(日均订单数 = ('订单数', 'mean')).reset_index()
            df = df.merge(promo_daily_order, on=['起始日期', 'promotion', 'promo_type'], how='left')
            df['日均订单数'].fillna(df['订单数'], inplace=True)
            df['爆发系数'] = df['日均订单数'] / df['前n天日均']
            return df

        # 挑选的sku
        hot_sku = {
            None: '全店',
            577749158548: '新奥*8包',
            564067342028: '番茄酱330g*4',
            566885211307: '黑胡椒酱230g*3',
            589690895211: '麻辣锅物(url)'
        }

        promo_info = process_promo_info()
        res_promotype = pd.DataFrame()
        for sku_id, sku_name in hot_sku.items():
            sku_daily = compute_outbreak_coeff(promo_info, self.df_sub, n_days=n, sku_id=sku_id)
            sku_daily = sku_daily.groupby('promo_type').agg(爆发系数=('爆发系数', 'mean'))
            sku_daily.columns = [sku_name]
            res_promotype = pd.concat([res_promotype, sku_daily], axis=1, join='outer', sort=False)

        res_promotion = pd.DataFrame()
        for sku_id, sku_name in hot_sku.items():
            sku_daily = compute_outbreak_coeff(promo_info, self.df_sub, n_days=n, sku_id=sku_id)
            sku_daily = sku_daily.groupby(['promotion','promo_type', '起始日期', '天数']).agg(爆发系数=('爆发系数', 'mean')).rename(columns={'爆发系数': sku_name})
            res_promotion = pd.concat([res_promotion, sku_daily], axis=1, join='outer', sort=False)
        res_promotion = res_promotion.reset_index().sort_values('起始日期')
        res_promotion['天数'] = res_promotion['天数'].astype('int64')

        return res_promotype, res_promotion
