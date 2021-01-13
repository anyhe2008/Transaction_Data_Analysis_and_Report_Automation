# scientific environment
import numpy as np
import pandas as pd
import matplotlib as plt
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

class PurchasePath:
    def __init__(self, df, df_sub):
        self.df = df
        self.df_sub = df_sub
        self.output_dir = '报表测试/购买路径图/'

    def draw_promo_purchase_path(self, df, max_level = 3, min_buy_rate = 0.02, min_buy_uv = 200, output_pic=True):
        """
        绘制复购节点的 大促/小促/直播/平日 占比。
        parameters：
        - df: 需要分析的订单数据
        - max_level: 看前面几次的下单情况。例如，若max_level=3，则只看前3次订单。
        - min_buy_rate: 最低复购率。
        - min_buy_uv: 最低复购人数。若复购率<min_buy_rate或复购人数<min_buy_uv，则路径终止。
        - output_pic: 是否保存图片
        """

        dot = Digraph()
        df = df.drop_duplicates()
        uv_ttl = df.loc[df['nth_order'] == 1, 'user_id'].unique()
        dot.node('0', '首单' + ' ' + str(len(uv_ttl)))

        q = deque([['0', '首单', uv_ttl]])

        level = 0
        while q and level < max_level:
            level += 1
            for i in range(len(q)):
                pre_node, pre_promo, pre_uv = q.popleft()
                df_curr = df.loc[(df['nth_order'] == level) & (df['user_id'].isin(pre_uv))]
                tmp = df_curr.groupby('promo_type').agg({'user_id':'count'}).reset_index()

                for j in range(tmp.shape[0]):
                    curr_promo = tmp.loc[j, 'promo_type']
                    curr_uv = df_curr.loc[(df_curr['promo_type'] == curr_promo), 'user_id'].unique()
                    curr_node = str(level) + str(i) + str(j)
                    buy_rate = len(curr_uv) / len(pre_uv)
                    if buy_rate > min_buy_rate and len(curr_uv) > min_buy_uv:
                        q.append([curr_node, curr_promo, curr_uv])
                        dot.node(curr_node, curr_promo + ' ' + str(len(curr_uv)))
                        if level == 1:
                            dot.edge(pre_node, curr_node, '{:.1%}'.format(buy_rate), arrowhead="none")
                        else:
                            dot.edge(pre_node, curr_node, '{:.1%}'.format(buy_rate))
        if output_pic:
            dot.render(self.output_dir + f"前{max_level}次下单促销类型占比", format='png', cleanup=True)
        return dot

    def draw_promo_sku_ratio(self, df_sub, nth_order=1, min_buy_rate = 0.05, min_buy_uv = 500, output_pic=True):
        """
        绘制第n单中各促销类型的入口单品及购买人数占比
        parameters：
        - df_sub: 需要分析的订单明细
        - nth_order: 第几单
        """

        dot = Digraph()

        df_sub = df_sub.loc[df_sub['nth_order'] == nth_order, ['user_id', 'promo_type', 'title_refined']].drop_duplicates(['user_id', 'title_refined'])
        uv_ttl = df_sub['user_id'].nunique()

        if nth_order == 1:
            root_label = '首单'
        else:
            root_label = f'第{nth_order}单'
        dot.node('0', root_label + ' ' + str(uv_ttl))

        promo_list = ['大促', '小促', '平日', '直播']
        for i in range(4):
            promo_type = promo_list[i]
            df_promo = df_sub[df_sub['promo_type'] == promo_type]
            promo_uv = df_promo['user_id'].nunique()
            # 画图
            promo_label = '1' + str(i)
            dot.node(promo_label, promo_type + '' + str(promo_uv))
            dot.edge('0', promo_label, '{:.1%}'.format(promo_uv / uv_ttl), arrowhead="none")
            # 计算各sku比例
            tmp = df_promo.groupby('title_refined').agg(人数=('user_id', 'count')).sort_values('人数', ascending=False).reset_index()
            for j in range(tmp.shape[0]):
                sku_label = promo_label + str(j)
                title = tmp.loc[j, 'title_refined']
                sku_uv = tmp.loc[j, '人数']
                buy_rate = sku_uv / promo_uv

                if buy_rate > min_buy_rate and sku_uv > min_buy_uv:
                    dot.node(sku_label, title + ' ' + str(sku_uv))
                    dot.edge(promo_label, sku_label, '{:.1%}'.format(buy_rate), arrowhead="none")
                else:
                    break

        if output_pic:
            dot.render(self.output_dir + f"第{nth_order}单各促销类型入口单品", format='png', cleanup=True)
        return dot

    def draw_cate_purchase_path(self, df_sub, category=None, max_level = 3, min_buy_rate = 0.02, min_buy_uv = 200, output_pic=True):
        """
        绘制各品类的复购路径图
        """
        dot = Digraph()
        # 根节点
        uv_ttl = df_sub.loc[df_sub['nth_order'] == 1, 'user_id'].unique()
        dot.node('0', '消费者总数' + ' ' + str(len(uv_ttl)))

        df_sub = df_sub[['user_id', 'nth_order', 'category']]

        q = deque([['0', '消费者总数', uv_ttl]])

        level = 0
        while q and level < max_level:
            level += 1
            for i in range(len(q)):
                pre_label, pre_cate, pre_uv = q.popleft()
                if level == 1 and category is not None:
                    df_curr = df_sub.loc[(df_sub['nth_order'] == level) & (df_sub['user_id'].isin(pre_uv)) & (df_sub['category'] == category)]
                else:
                    df_curr = df_sub.loc[(df_sub['nth_order'] == level) & (df_sub['user_id'].isin(pre_uv))]
                tmp = df_curr.groupby('category').agg(人数=('user_id','count')).sort_values('人数', ascending=False).reset_index()

                for j in range(tmp.shape[0]):
                    curr_label = str(level) + str(i) + str(j)
                    curr_cate = tmp.loc[j, 'category']
                    curr_uv = df_curr.loc[(df_curr['category'] == curr_cate), 'user_id'].unique()
                    buy_rate = len(curr_uv) / len(pre_uv)
                    if buy_rate > min_buy_rate and len(curr_uv) > min_buy_uv:
                        q.append([curr_label, curr_cate, curr_uv])
                        dot.node(curr_label, curr_cate + ' ' + str(len(curr_uv)))
                        if level == 1:
                            dot.edge(pre_label, curr_label, '{:.1%}'.format(buy_rate), arrowhead="none")
                        else:
                            dot.edge(pre_label, curr_label, '{:.1%}'.format(buy_rate))

        if output_pic:
            dot.render(self.output_dir + f"品类购买路径-{category}", format='png', cleanup=True)

        return dot

    def draw_cate_sku_purchase_path(self, df_sub, category='世界风味酱', max_level = 3, min_buy_rate_1 = 0.1, min_buy_rate_2 = 0.02, min_buy_uv = 200, output_pic=True):
        """
        绘制品类的复购路径，要看的品类细分至单品（在标题前用“$”做了标记）
        - min_buy_rate_1: 第一单的购买率阈值，即单品在所属品类的订单中的订单数占比（建议设高一点，不然图片会很宽）
        - min_buy_rate_2: 第2次及之后的复购率阈值
        """
        dot = Digraph()

        store_uv = df_sub.loc[(df_sub['nth_order'] == 1), 'user_id'].unique()
        uv_root = df_sub.loc[(df_sub['nth_order'] == 1) & (df_sub['category'] == category), 'user_id'].unique()
        df_sub = df_sub.loc[df_sub['user_id'].isin(uv_root), ['user_id', 'nth_order', 'category', 'title_refined', 'title']]

        # 画出根节点和第一层的节点
        dot.node('0', '消费者总数' + ' ' + str(len(store_uv)))
        dot.node('00', category + ' ' + str(len(uv_root)))
        buy_rate = len(uv_root) / len(store_uv)
        dot.edge('0', '00', '{:.1%}'.format(buy_rate), arrowhead="none")


        # 该品类的category用title_refined代替
        def cate_replace(cols, cate_ref):
            cate = cols[0]
            title_refined = cols[1] # 简化标题
            title = cols[2] # 统一标题

            if pd.isnull(title_refined):
                title_refined = title
            if cate == cate_ref:
                return '$' + title_refined
            return cate

        df_sub['category'] = df_sub[['category', 'title_refined', 'title']].apply(lambda x: cate_replace(x, category), axis=1)


        q = deque([['00', category, uv_root]])
        level = 0
        while q and level < max_level:
            level += 1
            for i in range(len(q)):
                pre_label, pre_cate, pre_uv = q.popleft()
                if level == 1: # 第一单只看该品类的单品
                    df_curr = df_sub.loc[(df_sub['nth_order'] == level) & (df_sub['category'].str.contains("\$"))]
                else: # 第二单及以后
                    df_curr = df_sub.loc[(df_sub['nth_order'] == level) & (df_sub['user_id'].isin(pre_uv))]
                tmp = df_curr.groupby('category').agg(人数=('user_id','count')).sort_values('人数', ascending=False).reset_index()

                for j in range(tmp.shape[0]):
                    curr_label = str(level) + str(i) + str(j)
                    curr_cate = tmp.loc[j, 'category']
                    curr_uv = df_curr.loc[(df_curr['category'] == curr_cate), 'user_id'].unique()
                    buy_rate = len(curr_uv) / len(pre_uv)
                    if level == 1:
                        min_buy_rate = min_buy_rate_1
                    else:
                        min_buy_rate = min_buy_rate_2
                    if buy_rate > min_buy_rate and len(curr_uv) > min_buy_uv:
                        q.append([curr_label, curr_cate, curr_uv])
                        dot.node(curr_label, curr_cate + ' ' + str(len(curr_uv)))
#                         dot.edges([pre_label+curr_label])
                        if level == 1:
                            dot.edge(pre_label, curr_label, label='{:.1%}'.format(buy_rate), arrowhead="none")
                        else:
                            dot.edge(pre_label, curr_label, label='{:.1%}'.format(buy_rate))
                    else:
                        break

        if output_pic:
            dot.render(self.output_dir + f"品类购买路径-{category}(区分单品)", format='png', cleanup=True)

        return dot

    def main(self):
        # 前三次下单的促销类型占比
        pic1 = self.draw_promo_purchase_path(self.df, max_level=3, min_buy_rate=0.02, min_buy_uv=200)

        # 首单各促销类型的入口单品
        pic2 = self.draw_promo_sku_ratio(self.df_sub, nth_order=1, min_buy_rate=0.05, min_buy_uv=500)

        # 品类的购买路径
        cates = ['新奥尔良料', '世界风味酱', '番茄酱',  '烤翅料', '锅物']
        for cate in cates:
            pic3_tmp = self.draw_cate_purchase_path(self.df_sub, category=cate, max_level=3, min_buy_rate=0.02, min_buy_uv=200)

        # 世界风味酱和锅物，具体到单品的复购路径图
        cates2 = ['世界风味酱', '锅物']
        for cate in cates2:
            pic4_tmp = self.draw_cate_sku_purchase_path(self.df_sub, category=cate, max_level=3, min_buy_rate_1=0.1, min_buy_rate_2=0.02, min_buy_uv=200)
