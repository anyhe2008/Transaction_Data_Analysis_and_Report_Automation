import numpy as np
import pandas as pd
import re
from datetime import datetime

class DataProcess:
    """
    对新增订单数据进行清洗，并与历史数据进行合并。流程包括：
    - read_data(): 读取新数据
    - sku_map(): 将原始订单按宝贝标题拆分至子订单，然后进行标题清洗和sku mapping。
      注意：匹配过程依赖于从历史订单中整理出来的匹配文件（self.sku_file)，如果本月宝贝标题发生大的变动，需要到文件里进行补充，或者到一面数据库里进行匹配。
    - add_fields(): 提取新字段，包括：订单创建日期/月份/星期、订单包含的品类数、用户地域信息（province/city/district/tier)、CRM标签、促销信息
      注意：已购CRM会员信息 和 促销活动信息 需要客户自己更新
    - mark_rebuy(): 标记这是该用户的第几次下单，以及该订单是否是复购订单。
    - union_data(): 合并历史数据和新数据，对历史数据进行备份，然后统一存为历史数据

    """
    def __init__(self, filename):
        # 新数据
        self.df_new = None
        self.df_sub_new = None
        self.new_order_file = filename  # 新订单
        self.pre_order_file = "数据存档/历史订单_test.csv"  # 历史订单数据
        self.pre_suborder_file = "数据存档/历史订单明细_test.csv"  # 历史订单明细
        self.sku_file = '数据/sku信息汇总（包括原始标题、清洗标题、统一标题）.xlsx'
        self.crm_file = '数据/已购CRM_test.xlsx'
        self.promo_file = '数据/活动日历.xlsx'
        self.city_tier_file = '数据/全国行政区域.xlsx'
        self.title_refined_file = '数据/sku标题简化.xlsx'
        # 历史数据
        self.df = None
        self.df_sub = None
        # 数字统计
        self.new_order_num = None
        self.new_valid_order_num = None
        self.new_valid_suborder_num = None
        self.new_usable_order_num = None
        self.new_usable_suborder_num = None

    def read_data(self):
        col_map = {
            '订单付款时间 ':'订单付款时间',
            '宝贝标题 ': '宝贝标题',
            '宝贝种类 ': '宝贝种类',
            '联系电话 ': '联系电话',
            '物流单号 ': '物流单号',
            '收货地址 ': '收货地址'
        }
        self.df_new = pd.read_excel(self.new_order_file).rename(columns=col_map)

    def sku_map(self):
        """
        sku mapping：标题清洗 → sku_id匹配 → channel、category匹配 → 标题简化
        """

        ### 1 - SKU MAPPING ###
        def title_clean(title):
            if title == ' 味好美黑椒酱230g*3包组合 黑胡椒酱汁牛排酱':
                title = '味好美黑椒酱230g*3包组合 黑胡椒酱汁牛排酱'
            # 清除带中括号的活动信息
            res = re.search(r'.+】(.+)', title)
            if res:
                title = res[1]
            # [9月10日秒杀]味好美新奥尔良烤鸡翅腌料*8包 烤炸鸡烧烤调料
            if "]" in title:
                title = title.split(']')[-1]
            # 1元预定  味好美培煎芝麻沙拉酱200g
            elif "1元预定" in title or '1元秒杀' in title:
                title = title.split()[-1]
            # 薇娅推荐 味好美新奥尔良烤鸡翅腌料 家用烤炸鸡烧烤调料烤肉腌料
            elif "薇娅推荐" in title:
                title = title[4:].strip()
            # 以“X月X日直播专享/秒杀”开头的标题
            elif re.search(r'直播.+?\s(.+)', title):
                m = re.search(r'直播.+?\s(.+)', title)
                title = m.group(1).strip()
                # 直播商品标题末尾可能含有“秒杀”字样
                if title[-2:] == '秒杀':
                    title = title[:-2]
            # “包邮 | ”开头的标题
            elif re.match(r'^包邮(.+)', title):
                title = title[5:]
            elif re.match(r'^新品(.+)', title):
                m = re.match(r'^新品(.+)', title)
                title = m.group(1).strip()
            elif re.match(r'^预售(.+)', title):
                m = re.match(r'^预售(.+)', title)
                title = m.group(1).strip()
            return title

        # 提取有效订单
        self.new_order_num = self.df_new.shape[0]
        cols = ['订单编号', '订单创建时间', '总金额', '宝贝标题', '宝贝种类',
                '宝贝总数量', '买家会员名', '买家支付宝账号', '收货人姓名', '收货地址', '联系手机']
        self.df_new = self.df_new.loc[self.df_new['订单状态'] != '交易关闭', cols]
        self.new_valid_order_num = self.df_new.shape[0]


        # 标题清洗 + 拆分子订单
        self.df_new['宝贝标题'] = self.df_new['宝贝标题'].apply(lambda x: x.replace("购，", '购'))  # 处理活动信息 【1元换购，单拍不发货】
        self.df_new['title_origin'] = self.df_new['宝贝标题'].apply(lambda x: x.split("，"))
        self.df_sub_new = self.df_new.explode('title_origin')
        self.df_sub_new['title_clean'] = self.df_sub_new['title_origin'].apply(title_clean)
        self.new_valid_suborder_num = self.df_sub_new.shape[0]

        # sku_id匹配，先用title_clean，再用title_origin
        sku_info = pd.read_excel(self.sku_file)
        title_clean_tmp = sku_info[['title_clean', 'sku_id', 'title', '类别', '品类']]
        title_origin_tmp = sku_info[['title_origin', 'sku_id', 'title', '类别', '品类']].drop_duplicates(
            "title_origin").rename(
            columns={'sku_id': 'sku_id2', 'title': 'title2', '类别': '类别2', '品类': '品类2'})
        self.df_sub_new = self.df_sub_new.merge(title_clean_tmp, how='left')
        self.df_sub_new = self.df_sub_new.merge(title_origin_tmp, how='left')
        self.df_sub_new['sku_id'].fillna(self.df_sub_new['sku_id2'], inplace=True)
        self.df_sub_new['title'].fillna(self.df_sub_new['title2'], inplace=True)
        self.df_sub_new['类别'].fillna(self.df_sub_new['类别2'], inplace=True)
        self.df_sub_new['品类'].fillna(self.df_sub_new['品类2'], inplace=True)
        self.df_sub_new.drop(['sku_id2', 'title2', '类别2', '品类2'], axis=1, inplace=True)

        # 字段重命名
        col_dict = {
            '订单编号': 'order_id',
            '订单创建时间': 'order_time',
            '订单创建日期': 'dt',
            '买家会员名': 'user_id',
            '总金额': 'order_value',
            '宝贝种类': 'goods_type',
            '宝贝总数量': 'goods_num',
            '品类数': 'cate_num',
            '收货地址': 'address',
            '类别': 'channel',
            '品类': 'category',
            '联系手机':'tel',
            '买家支付宝账号':'alipay_id',
            '收货人姓名':'user_name',
            '宝贝标题':'items'
        }
        self.df_sub_new = self.df_sub_new.loc[(self.df_sub_new['sku_id'].notnull()) & (self.df_sub_new['品类'].notnull())].rename(
            columns=col_dict)
        self.new_usable_suborder_num = self.df_sub_new.shape[0]
        print('宝贝ID匹配完成！ 匹配率:', '{:.1%}'.format(self.new_usable_suborder_num / self.new_valid_suborder_num))

        # 标题简化
        title_refine = pd.read_excel(self.title_refined_file)
        title_refine_dict = title_refine[['sku_id', 'title_refined']].set_index('sku_id').to_dict()['title_refined']
        self.df_sub_new['sku_id'] = self.df_sub_new['sku_id'].astype('int64')
        self.df_sub_new['title_refined'] = self.df_sub_new['sku_id'].map(lambda x: title_refine_dict.get(x, np.nan))

    def add_fields(self):
        """
        - 从订单数据中提取：订单创建日期/月份/星期、订单包含的品类数、用户地域信息（province/city/district/tier)
        - 注意：已购CRM会员信息 和 促销活动信息 需要客户自己更新
        """
        # 1. 标记订单创建日期
        self.df_sub_new['order_time'] = pd.to_datetime(self.df_sub_new['order_time'])
        self.df_sub_new['dt'] = self.df_sub_new['order_time'].map(lambda x: x.date())
        self.df_sub_new['dt'] = self.df_sub_new['dt'].apply(lambda x: str(x.strftime('%Y-%m-%d')))
        self.df_sub_new['month'] = self.df_sub_new['dt'].apply(lambda x: x[:8] + '01')
        self.df_sub_new['dt'] = pd.to_datetime(self.df_sub_new['dt'])
        self.df_sub_new['day_of_week'] = self.df_sub_new['dt'].dt.dayofweek + 1

        # 2. 统计订单包含的品类数和类别数
        cate_num = self.df_sub_new.groupby(['order_id']).agg(cate_num=('category', 'nunique'),
                                                        channel_num=('channel', 'nunique')).reset_index()
        self.df_sub_new = self.df_sub_new.merge(cate_num, on='order_id', how='left')

        # 3. 标注地域和城市级别
        city_tier = pd.read_excel("数据/全国行政区域.xlsx")
        self.df_sub_new['province'] = self.df_sub_new['address'].map(lambda s: s.split()[0])
        self.df_sub_new['city'] = self.df_sub_new['address'].map(lambda s: s.split()[1])
        self.df_sub_new['district'] = self.df_sub_new['address'].map(lambda s: s.split()[2])

        # 省、市信息清洗
        def province_clean(province):
            if province[-1] == '省':
                return province[:-1]
            if province == '广西壮族自治区':
                return '广西'
            if province == '内蒙古自治区':
                return '内蒙古'
            if province == '宁夏回族自治区':
                return '宁夏'
            return province

        def city_clean(city):
            if city[-1] == '市':
                return city[:-1]
            if city == '大理白族自治州':
                return '大理'
            if city == '延边朝鲜族自治州':
                return '延边'
            s = re.search(r'^(黔东南|黔西南|西双版纳|.{2}).*族自治州$', city)
            if s:
                return s.group(1)
            return city

        self.df_sub_new['province'] = self.df_sub_new['province'].apply(province_clean)
        self.df_sub_new['city'] = self.df_sub_new['city'].apply(city_clean)
        # district 信息清洗
        self.df_sub_new.loc[(self.df_sub_new['province'] == '广东') & (self.df_sub_new['city'] == '东莞'), 'district'] = '东莞市'
        self.df_sub_new.loc[(self.df_sub_new['province'] == '广东') & (self.df_sub_new['city'] == '中山'), 'district'] = '中山市'
        self.df_sub_new.loc[(self.df_sub_new['province'] == '江苏') & (self.df_sub_new['city'] == '苏州') &
                       (self.df_sub_new['district'].isin(['苏州工业园区', '园区'])), 'district'] = '吴中区'
        self.df_sub_new.loc[(self.df_sub_new['province'] == '湖北') & (self.df_sub_new['city'] == '潜江'), 'district'] = '潜江市'
        self.df_sub_new.loc[(self.df_sub_new['province'] == '湖北') & (self.df_sub_new['city'] == '仙桃'), 'district'] = '仙桃市'
        self.df_sub_new.loc[(self.df_sub_new['province'] == '湖北') & (self.df_sub_new['city'] == '天门'), 'district'] = '天门市'
        self.df_sub_new.loc[(self.df_sub_new['province'] == '安徽') & (self.df_sub_new['city'] == '芜湖') &
                       (self.df_sub_new['district'] == '无为县'), 'district'] = '无为市'
        self.df_sub_new.loc[(self.df_sub_new['province'] == '海南') & (~self.df_sub_new['city'].isin(['三亚', '海口'])),
                       'district'] = self.df_sub_new.loc[
            (self.df_sub_new['province'] == '海南') & (~self.df_sub_new['city'].isin(['三亚', '海口'])), 'city']
        self.df_sub_new.loc[(self.df_sub_new['province'] == '海南') & (~self.df_sub_new['city'].isin(['三亚', '海口'])), 'city'] = '海南'
        self.df_sub_new = self.df_sub_new.merge(city_tier, on=['province', 'city', 'district'], how='left')
        print('城市级别匹配完成！匹配率:',
              '{:.1%}'.format(self.df_sub_new[self.df_sub_new['tier'].notnull()].shape[0] / self.new_usable_suborder_num))

        # 4. 标记CRM会员
        if self.crm_file:
            crm_info = pd.read_excel(self.crm_file).rename(columns={"客户ID": "user_id"})
            crm_info['is_crm'] = 'CRM'
            self.df_sub_new = self.df_sub_new.merge(crm_info[['user_id', 'is_crm']], how='left')
            self.df_sub_new['is_crm'].fillna('非CRM', inplace=True)

        # 5. 标记促销
        if self.promo_file:
            promo_info = pd.read_excel(self.promo_file)
            promo_info['dt'] = promo_info[['起始日期', '终止日期']].apply(
                lambda x: pd.date_range(start=x[0], end=x[1], freq='D'), axis=1)
            promo_info['dt'] = promo_info['dt'].apply(lambda x: [datetime.strftime(e, '%Y-%m-%d') for e in x])
            promo_info = promo_info.explode('dt')
            promo_info = promo_info.rename(columns={
                '促销活动': 'promotion',
                '促销类型': 'promo_type'
            })[['dt', 'promotion', 'promo_type', 'sku_id']]
            promo_info['dt'] = promo_info['dt'].astype('datetime64[ns]')
            live_info = promo_info.loc[promo_info['promotion'].str.contains('直播')].rename(columns={'promotion':'live_name', 'promo_type':'live'})
            promo_info = promo_info.loc[~promo_info['promotion'].str.contains('直播'), ['dt', 'promotion', 'promo_type']]
            # 标记促销
            self.df_sub_new = self.df_sub_new.merge(promo_info, on=['dt'], how='left')
            # 标记直播
            self.df_sub_new = self.df_sub_new.merge(live_info, on=['dt', 'sku_id'], how='left')
            self.df_sub_new['promotion'].fillna(self.df_sub_new['live_name'], inplace=True)
            self.df_sub_new['promo_type'].fillna(self.df_sub_new['live'], inplace=True)
            self.df_sub_new = self.df_sub_new.drop(['live_name', 'live'], axis=1)
            self.df_sub_new['promotion'].fillna('无', inplace=True)
            self.df_sub_new['promo_type'].fillna('平日', inplace=True)

    def mark_rebuy(self):
        """
        读取历史订单数据，然后标记新订单里用户是第几次下单，以及该订单是否是复购订单
        """
        # 读取历史文件
        self.df = pd.read_csv(self.pre_order_file, dtype={'联系手机': 'str', 'live': 'str'})
        self.df['dt'] = self.df['dt'].astype('datetime64[ns]')
        self.df_sub = pd.read_csv(self.pre_suborder_file, dtype={'联系手机': 'str', 'live': 'str'})
        self.df_sub['dt'] = self.df_sub['dt'].astype('datetime64[ns]')
        # 标注下单序数
        #逻辑：先对新订单排序，再加上老订单次序
        order_sort = self.df_sub_new[['user_id', 'order_time']].drop_duplicates().sort_values(['user_id', 'order_time']).reset_index(drop=True)
        tmp = order_sort.groupby('user_id').agg(order_count = ('order_time', 'count')).reset_index()
        order_sort = order_sort.merge(tmp)
        user_order_num = self.df.groupby(['user_id']).agg({'nth_order':'max'}).reset_index()
        order_sort = order_sort.merge(user_order_num, on='user_id', how='left')
        order_sort['nth_order'].fillna(value=0, inplace=True)
        order_sort2 = order_sort[order_sort['order_count'] > 1][['user_id', 'order_time']]
        order_sort2 = pd.concat([order_sort2,
                                order_sort2.groupby('user_id').transform(lambda x: list(range(1, len(x) + 1)))
                                        ], axis=1)
        order_sort2.columns = ['user_id', 'order_time', 'nth_order_2']
        order_sort = order_sort.merge(order_sort2, how='left')
        order_sort['nth_order_2'].fillna(value=1, inplace=True)
        order_sort['nth_order'] += order_sort['nth_order_2']
        order_sort['nth_order'] = order_sort['nth_order'].astype('int64')
        order_sort = order_sort[['user_id', 'order_time', 'nth_order']]
        self.df_sub_new = self.df_sub_new.merge(order_sort, on=['user_id', 'order_time'], how='left')
        # 标注是否复购订单
        self.df_sub_new['is_rebuy'] = self.df_sub_new['nth_order'] > 1
        # 删除多余字段
        self.df_sub_new = self.df_sub_new.drop('title_clean', axis=1)
        self.df_new = self.df_sub_new.drop_duplicates(['order_id'])
        self.df_new = self.df_new.drop('title_refined', axis=1)
        self.new_usable_order_num = self.df_new.shape[0]
        print("复购订单标注完成！")

    def union_data(self):
        """
        合并订单数据并储存为历史数据
        """
        # 数据备份
#         now = datetime.today().strftime("%Y%m%d_%H:%M:%S")
#         self.df.to_csv(f"数据存档/历史订单bak_{now}.csv")
#         self.df_sub.to_csv(f"数据存档/历史订单明细bak_{now}.csv")
        # 数据合并
        self.df = pd.concat([self.df, self.df_new], axis=0, ignore_index=True)
        self.df_sub = pd.concat([self.df_sub, self.df_sub_new], axis=0, ignore_index=True)
        self.df.to_csv("数据存档/历史订单_test.csv", index=False)
        self.df_sub.to_csv("数据存档/历史订单明细_test.csv", index=False)

    def main(self):
        self.read_data()
        self.sku_map()
        self.add_fields()
        self.mark_rebuy()
        self.union_data()
        print("数据处理完成！")
        print("=========== 新增数据概况 ===========")
        print(f"订单数：{self.new_order_num}")
        print(f"有效订单数：{self.new_valid_order_num}, 有效子订单数:{self.new_valid_suborder_num}")
        print(f"可用订单数：{self.new_usable_order_num}, 可用子订单数:{self.new_usable_suborder_num}")
        print("===================================")
