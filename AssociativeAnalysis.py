
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

class AssociativeAnalysis:
    def __init__(self, df, df_sub):
        self.df = df
        self.df_sub = df_sub

    def run_association_rules(self, dataset, min_sup, min_conf, max_len=None):
        """
        parameters:
        - dataset: 嵌套列表形式的订单数据，每个内嵌列表为同一订单内的独立品类名称
        - min_sup: 支持度的最小阈值
        - min_conf: 置信度的最小阈值
        - max_len：频繁项集的最大长度，如果max_len=None，则不限制频繁项集的长度
        """
        te = TransactionEncoder()
        te_ary = te.fit(dataset).transform(dataset) # fit学习数据中的unique labels，transform将它们转化为独热编码
        df = pd.DataFrame(te_ary, columns=te.columns_)
        # 频繁项集
        frequent_itemsets = fpgrowth(df, min_support=min_sup, use_colnames=True, max_len=max_len)
        # 关联规则
        rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_conf)
        return frequent_itemsets,rules

    def cate_associate_res(self, basket='order', min_sup=0.02, min_conf=0.2, min_lift=1):
        """
        basket：以什么作为菜篮子。可选维度：'order'-订单；'user'-用户
        """

        # 将订单数据处理为嵌套列表
        if basket == 'order': # 订单维度，只采用品类数>1的订单数据
            dataset = self.df_sub.loc[self.df_sub['cate_num'] > 1, ['order_id', 'category']
                                     ].drop_duplicates().groupby('order_id')['category'].apply(list).to_list()
        elif basket == 'user': # 用户维度，只采用累计购买品类数>1的用户的数据
            user_cate = self.df_sub[['user_id', 'category']].drop_duplicates()
            user_cate_count = user_cate.groupby('user_id').agg(cate_count=('category', 'count'))
            user_cate_count = user_cate.merge(user_cate_count, on='user_id', how='left')
            user_cate_count = user_cate_count[user_cate_count['cate_count'] > 1]
            dataset = user_cate_count.groupby('user_id')['category'].apply(list).to_list()

        # 关联分析
        frequent_itemsets,rules_category = self.run_association_rules(dataset,min_sup, min_conf, max_len=2)
        rules_category = rules_category[rules_category['lift'] > min_lift]
        rules_category['antecedents'] = rules_category['antecedents'].apply(lambda x: ', '.join(list(x)))
        rules_category['consequents'] = rules_category['consequents'].apply(lambda x: ', '.join(list(x)))

        # 调整列顺序
        cols = ['antecedents', 'consequents', 'antecedent support',
       'consequent support', 'support', 'confidence', 'lift']
        rules_category = rules_category[cols].sort_values(['support', 'lift', 'confidence'], ascending=False)
        rules_category.columns = ['商品A', '商品B', '含有A的订单比例', '含有B的订单比例',
                                  f'支持度(>{min_sup})', f'置信度(>{min_conf})', f'提升度(>{min_lift})']
        return rules_category

    def sku_associate_res(self, basket='order', min_sup=0.01, min_conf=0.2, min_lift=1):
        """
        basket：以什么作为菜篮子。可选维度：'order'-订单；'user'-用户
        """

        # 将订单数据处理为嵌套列表
        if basket == 'order': # 订单维度，只采用品类数>1的订单数据
            dataset = self.df_sub.loc[self.df_sub['goods_type'] > 1, ['order_id', 'sku_id']
                                     ].drop_duplicates().groupby('order_id')['sku_id'].apply(list).to_list()
        elif basket == 'user': # 用户维度，只采用累计购买单品数>1的用户的数据
            user_sku = self.df_sub[['user_id', 'sku_id']].drop_duplicates()
            user_sku_count = user_sku.groupby('user_id').agg(sku_count=('sku_id', 'count'))
            user_sku_count = user_sku.merge(user_sku_count, on='user_id', how='left')
            user_sku_count = user_sku_count[user_sku_count['sku_count'] > 1]
            dataset = user_sku_count.groupby('user_id')['sku_id'].apply(list).to_list()

        frequent_itemsets,rules_sku = self.run_association_rules(dataset, min_sup, min_conf)
        rules_sku = rules_sku[rules_sku['lift'] > min_lift]

        # 增添 品类 和 简化标题
        sku_info = self.df_sub[['sku_id', 'title_refined', 'title', 'category']].drop_duplicates(['sku_id'])
        sku_info['title_refined'].fillna(sku_info['title'], inplace=True)
        sku_title_dict = sku_info[['sku_id', 'title_refined']].set_index('sku_id').to_dict()['title_refined']
        sku_cate_dict = sku_info[['sku_id', 'category']].set_index('sku_id').to_dict()['category']
        rules_sku['A品类'] = rules_sku['antecedents'].apply(lambda x: ', '.join([sku_cate_dict.get(i, i) for i in list(x)]))
        rules_sku['B品类'] = rules_sku['consequents'].apply(lambda x: ', '.join([sku_cate_dict.get(i, i) for i in list(x)]))
        rules_sku['antecedents'] = rules_sku['antecedents'] \
                                       .apply(lambda x: ', '.join([sku_title_dict.get(i, i) for i in list(x)]))
        rules_sku['consequents'] = rules_sku['consequents'] \
                                       .apply(lambda x: ', '.join([sku_title_dict.get(i, i) for i in list(x)]))

        # 调整列顺序
        cols = ['antecedents', 'A品类', 'consequents','B品类', 'antecedent support',
               'consequent support', 'support', 'confidence', 'lift']
        rules_sku = rules_sku[cols].sort_values(['support', 'lift', 'confidence'], ascending=False)
        rules_sku.columns = ['商品A','A品类', '商品B', 'B品类', '含有A的订单比例', '含有B的订单比例',
                                  f'支持度(>{min_sup})', f'置信度(>{min_conf})', f'提升度(>{min_lift})']
        return rules_sku
