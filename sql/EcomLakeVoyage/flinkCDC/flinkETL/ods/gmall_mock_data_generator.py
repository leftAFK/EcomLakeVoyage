#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gmall 实时数仓 - MySQL 全量表模拟数据生成器
=============================================
按用户提供的 gmall_base(维度库) + gmall_business(业务库) 完整 schema 生成高逼真数据。
覆盖 14 张表，表间外键关联、金额一致、订单状态流转、优惠券核销等业务逻辑完整。

依赖: pip install faker pymysql numpy --break-system-packages
用法:
  python3 gmall_mock_data_generator.py --dry-run                           # 预览不写库
  python3 gmall_mock_data_generator.py --host 10.0.0.1 -u root -p secret   # 写入 MySQL
  python3 gmall_mock_data_generator.py --dry-run --seed 42                 # 可复现
"""

import argparse, json, random, sys
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import pymysql
from faker import Faker

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False
    print("[WARN] numpy 未安装，使用 random 替代。建议: pip install numpy")

fake = Faker('zh_CN')

# ============================================================
#  一、真实基础数据常量
# ============================================================

# ---- 品牌 ----
BRANDS = [
    "华为", "苹果", "小米", "OPPO", "vivo", "三星", "荣耀",
    "联想", "戴尔", "惠普", "华硕",
    "海尔", "美的", "格力", "西门子",
    "耐克", "阿迪达斯", "李宁", "安踏", "优衣库", "ZARA",
    "雅诗兰黛", "兰蔻", "巴黎欧莱雅", "SK-II", "资生堂",
    "飞利浦", "索尼", "罗技",
    "伊利", "雀巢", "三只松鼠", "百草味",
    "茅台", "五粮液",
    "宜家", "得力", "晨光",
]

# ---- 品牌 -> 主营三级分类 ----
BRAND_CATEGORIES = {
    "华为": ["智能手机", "智能手表", "平板电脑", "笔记本", "蓝牙耳机", "智能音箱"],
    "苹果": ["智能手机", "平板电脑", "笔记本", "蓝牙耳机", "智能手表"],
    "小米": ["智能手机", "智能手表", "平板电脑", "蓝牙耳机", "智能音箱", "电饭煲"],
    "OPPO": ["智能手机", "蓝牙耳机", "智能手表"],
    "vivo": ["智能手机", "蓝牙耳机", "智能手表"],
    "三星": ["智能手机", "平板电脑", "蓝牙耳机", "电视"],
    "荣耀": ["智能手机", "笔记本", "平板电脑", "智能手表"],
    "联想": ["笔记本", "台式机", "平板电脑", "显示器"],
    "戴尔": ["笔记本", "台式机", "显示器"],
    "惠普": ["笔记本", "台式机", "打印机"],
    "华硕": ["笔记本", "显示器"],
    "海尔": ["冰箱", "洗衣机", "空调", "电视"],
    "美的": ["冰箱", "洗衣机", "空调", "电饭煲", "微波炉"],
    "格力": ["空调", "电风扇"],
    "西门子": ["冰箱", "洗衣机"],
    "耐克": ["运动鞋", "运动服", "户外装备"],
    "阿迪达斯": ["运动鞋", "运动服"],
    "李宁": ["运动鞋", "运动服"],
    "安踏": ["运动鞋", "运动服"],
    "优衣库": ["T恤", "卫衣", "裤装", "外套"],
    "ZARA": ["连衣裙", "衬衫", "外套", "裤装"],
    "雅诗兰黛": ["精华", "面霜", "面膜"],
    "兰蔻": ["精华", "面霜", "口红"],
    "巴黎欧莱雅": ["面霜", "防晒霜", "粉底液"],
    "SK-II": ["精华", "面膜"],
    "资生堂": ["面霜", "精华", "防晒霜"],
    "飞利浦": ["电动牙刷", "剃须刀", "吹风机"],
    "索尼": ["蓝牙耳机", "音箱"],
    "罗技": ["鼠标", "键盘"],
    "伊利": ["牛奶", "奶粉"],
    "雀巢": ["咖啡", "奶粉"],
    "三只松鼠": ["坚果", "饼干", "巧克力"],
    "百草味": ["坚果", "肉脯", "饼干"],
    "茅台": ["白酒"],
    "五粮液": ["白酒"],
    "宜家": ["床", "沙发", "四件套"],
    "得力": ["办公文具", "碎纸机"],
    "晨光": ["办公文具"],
}
# 白酒/食品等映射到三级分类
BRAND_CATEGORIES["茅台"] = ["白酒"]
BRAND_CATEGORIES["五粮液"] = ["白酒"]

# ---- 三级分类树 (一级, [(二级, [三级,...]),...]) ----
CATEGORY_TREE = [
    ("手机数码", [
        ("手机通讯", ["智能手机", "老人机", "对讲机"]),
        ("手机配件", ["手机壳", "充电器", "移动电源"]),
        ("智能设备", ["智能手表", "智能手环", "智能音箱"]),
        ("数码配件", ["蓝牙耳机", "音箱", "U盘"]),
    ]),
    ("电脑办公", [
        ("电脑整机", ["笔记本", "台式机", "平板电脑", "一体机"]),
        ("电脑配件", ["鼠标", "键盘", "显示器"]),
        ("办公用品", ["打印机", "办公文具", "碎纸机"]),
    ]),
    ("家用电器", [
        ("大家电", ["空调", "冰箱", "洗衣机", "电视"]),
        ("厨房电器", ["电饭煲", "微波炉", "榨汁机", "电烤箱"]),
        ("生活电器", ["电风扇", "吹风机", "剃须刀", "电熨斗"]),
    ]),
    ("服饰鞋包", [
        ("男装", ["T恤", "衬衫", "裤装", "外套", "卫衣"]),
        ("女装", ["连衣裙", "半身裙", "毛衣", "羽绒服"]),
        ("运动户外", ["运动鞋", "运动服", "户外装备", "健身器材"]),
        ("箱包", ["双肩包", "手提包", "钱包", "旅行箱"]),
    ]),
    ("美妆个护", [
        ("面部护肤", ["面霜", "精华", "面膜", "防晒霜"]),
        ("彩妆", ["口红", "粉底液", "眼影", "睫毛膏"]),
        ("个人护理", ["洗发水", "沐浴露", "牙膏", "电动牙刷"]),
    ]),
    ("食品生鲜", [
        ("零食", ["坚果", "饼干", "巧克力", "肉脯"]),
        ("饮品", ["咖啡", "茶叶", "牛奶", "果汁"]),
        ("粮油", ["大米", "食用油", "奶粉"]),
    ]),
    ("运动健康", [
        ("运动装备", ["跑步机", "瑜伽垫", "哑铃", "跳绳"]),
        ("营养保健", ["维生素", "蛋白粉", "鱼油"]),
    ]),
    ("家居家装", [
        ("家具", ["床", "沙发", "衣柜", "书桌"]),
        ("家纺", ["四件套", "枕头", "被子"]),
        ("灯具", ["台灯", "吊灯", "LED灯泡"]),
    ]),
    ("母婴玩具", [
        ("婴儿用品", ["纸尿裤", "婴儿车"]),
        ("玩具", ["积木", "遥控车", "毛绒玩具"]),
    ]),
    ("酒水", [
        ("白酒", ["白酒"]),
    ]),
]

# ---- 分类 -> 价格区间 ----
CATEGORY_PRICES = {
    "智能手机": (899, 12999), "平板电脑": (1299, 9999), "笔记本": (2999, 29999),
    "台式机": (2999, 19999), "一体机": (3999, 14999), "智能手表": (299, 5999),
    "智能手环": (99, 999), "智能音箱": (99, 1999), "蓝牙耳机": (99, 3999),
    "音箱": (99, 4999), "电视": (1299, 19999), "空调": (1599, 12999),
    "冰箱": (1299, 19999), "洗衣机": (899, 9999), "电饭煲": (99, 1999),
    "微波炉": (299, 1999), "榨汁机": (99, 999), "电烤箱": (199, 3999),
    "电风扇": (89, 999), "吹风机": (79, 2999), "剃须刀": (89, 1999),
    "电熨斗": (59, 999), "运动鞋": (159, 2999), "运动服": (99, 1599),
    "户外装备": (49, 999), "健身器材": (199, 9999), "T恤": (39, 599),
    "衬衫": (99, 1299), "裤装": (79, 999), "外套": (159, 3999),
    "卫衣": (99, 899), "连衣裙": (129, 2999), "半身裙": (99, 999),
    "毛衣": (159, 1999), "羽绒服": (299, 5999), "双肩包": (59, 999),
    "手提包": (129, 2999), "钱包": (39, 599), "旅行箱": (159, 2999),
    "面霜": (89, 2999), "精华": (159, 3999), "面膜": (39, 599),
    "防晒霜": (69, 599), "口红": (49, 999), "粉底液": (89, 899),
    "眼影": (39, 599), "睫毛膏": (29, 399), "洗发水": (29, 299),
    "沐浴露": (19, 159), "牙膏": (9, 99), "电动牙刷": (99, 1999),
    "坚果": (19, 199), "饼干": (9, 99), "巧克力": (19, 299),
    "肉脯": (29, 159), "咖啡": (29, 399), "茶叶": (39, 999),
    "牛奶": (39, 199), "果汁": (19, 99), "大米": (29, 299),
    "食用油": (39, 299), "奶粉": (89, 599), "跑步机": (999, 9999),
    "瑜伽垫": (39, 299), "哑铃": (49, 599), "跳绳": (19, 99),
    "维生素": (69, 599), "蛋白粉": (99, 899), "鱼油": (59, 399),
    "床": (499, 9999), "沙发": (999, 19999), "衣柜": (599, 5999),
    "书桌": (299, 2999), "四件套": (99, 999), "枕头": (39, 399),
    "被子": (99, 999), "台灯": (49, 999), "吊灯": (99, 2999),
    "LED灯泡": (9, 99), "纸尿裤": (59, 399), "婴儿车": (299, 3999),
    "积木": (29, 299), "遥控车": (49, 599), "毛绒玩具": (19, 299),
    "白酒": (299, 39999), "鼠标": (39, 999), "键盘": (79, 1599),
    "显示器": (699, 6999), "打印机": (299, 3999), "办公文具": (5, 99),
    "碎纸机": (199, 1999), "手机壳": (9, 199), "充电器": (19, 299),
    "移动电源": (49, 399), "老人机": (99, 899), "对讲机": (99, 999),
    "U盘": (19, 399),
}
DEFAULT_PRICE = (29, 999)

# ---- 分类 -> SKU 规格选项 ----
CATEGORY_SPECS = {
    "智能手机": [("颜色", ["曜石黑", "冰川白", "深海蓝", "晨曦紫", "流光金", "远峰蓝"]),
                 ("存储", ["8GB+128GB", "8GB+256GB", "12GB+256GB", "12GB+512GB", "16GB+1TB"])],
    "笔记本": [("颜色", ["深空灰", "银色", "午夜色"]),
                 ("配置", ["i5/16G/512G", "i7/16G/512G", "i7/32G/1T", "R7/16G/512G"])],
    "平板电脑": [("颜色", ["深空灰", "银色", "玫瑰金"]),
                  ("存储", ["64GB", "128GB", "256GB", "512GB"])],
    "蓝牙耳机": [("颜色", ["白色", "黑色", "蓝色"]), ("款式", ["标准版", "Pro版"])],
    "智能手表": [("颜色", ["黑色", "银色", "金色"]), ("表带", ["运动版", "商务版"])],
    "运动鞋": [("颜色", ["黑色", "白色", "红色", "蓝色"]), ("尺码", ["39", "40", "41", "42", "43", "44"])],
    "T恤": [("颜色", ["白色", "黑色", "藏青", "灰色", "卡其"]), ("尺码", ["S", "M", "L", "XL", "XXL"])],
    "连衣裙": [("颜色", ["黑色", "红色", "蓝色", "绿色"]), ("尺码", ["S", "M", "L", "XL"])],
    "白酒": [("规格", ["500ml", "1000ml", "500ml礼盒装", "250ml"])],
}
DEFAULT_SPECS = [("颜色", ["黑色", "白色", "蓝色", "灰色"]), ("规格", ["标准版", "升级版"])]

# ---- 省/市/区 + 大区 ----
REGIONS_DATA = [
    ("华东", "上海市", [("上海市", ["黄浦区", "徐汇区", "长宁区", "静安区", "普陀区", "浦东新区", "闵行区", "宝山区"])]),
    ("华东", "江苏省", [("南京市", ["玄武区", "秦淮区", "鼓楼区", "建邺区", "栖霞区"]), ("苏州市", ["姑苏区", "吴中区", "相城区", "工业园区"]), ("无锡市", ["梁溪区", "滨湖区", "新吴区"])]),
    ("华东", "浙江省", [("杭州市", ["上城区", "拱墅区", "西湖区", "滨江区", "余杭区"]), ("宁波市", ["海曙区", "江北区", "鄞州区"]), ("温州市", ["鹿城区", "龙湾区"])]),
    ("华东", "安徽省", [("合肥市", ["蜀山区", "庐阳区", "包河区"]), ("芜湖市", ["镜湖区", "弋江区"])]),
    ("华东", "福建省", [("福州市", ["鼓楼区", "台江区", "仓山区"]), ("厦门市", ["思明区", "湖里区", "集美区"])]),
    ("华东", "江西省", [("南昌市", ["东湖区", "西湖区", "青山湖区"])]),
    ("华东", "山东省", [("济南市", ["历下区", "市中区", "天桥区"]), ("青岛市", ["市南区", "市北区", "李沧区"]), ("烟台市", ["芝罘区", "莱山区"])]),
    ("华北", "北京市", [("北京市", ["东城区", "西城区", "朝阳区", "海淀区", "丰台区", "石景山区", "通州区", "昌平区"])]),
    ("华北", "天津市", [("天津市", ["和平区", "河东区", "河西区", "南开区", "河北区"])]),
    ("华北", "河北省", [("石家庄市", ["长安区", "桥西区", "新华区"]), ("唐山市", ["路南区", "路北区"])]),
    ("华北", "山西省", [("太原市", ["小店区", "迎泽区", "杏花岭区"])]),
    ("华北", "内蒙古自治区", [("呼和浩特市", ["新城区", "回民区", "赛罕区"]), ("包头市", ["昆都仑区", "青山区"])]),
    ("华南", "广东省", [("广州市", ["天河区", "海珠区", "越秀区", "白云区", "番禺区"]), ("深圳市", ["福田区", "南山区", "罗湖区", "宝安区", "龙岗区"]), ("东莞市", ["南城街道", "莞城街道"]), ("佛山市", ["禅城区", "南海区"])]),
    ("华南", "广西壮族自治区", [("南宁市", ["青秀区", "兴宁区", "江南区"]), ("柳州市", ["城中区", "鱼峰区"])]),
    ("华南", "海南省", [("海口市", ["秀英区", "龙华区", "琼山区"]), ("三亚市", ["吉阳区", "天涯区"])]),
    ("华中", "河南省", [("郑州市", ["金水区", "二七区", "管城回族区", "中原区"]), ("洛阳市", ["老城区", "洛龙区"])]),
    ("华中", "湖北省", [("武汉市", ["江岸区", "江汉区", "硚口区", "汉阳区", "武昌区", "洪山区"]), ("宜昌市", ["西陵区", "伍家岗区"])]),
    ("华中", "湖南省", [("长沙市", ["芙蓉区", "天心区", "岳麓区", "开福区", "雨花区"]), ("株洲市", ["天元区", "荷塘区"])]),
    ("西南", "重庆市", [("重庆市", ["渝中区", "江北区", "南岸区", "九龙坡区", "沙坪坝区", "渝北区", "巴南区"])]),
    ("西南", "四川省", [("成都市", ["锦江区", "青羊区", "金牛区", "武侯区", "成华区", "龙泉驿区"]), ("绵阳市", ["涪城区", "游仙区"])]),
    ("西南", "贵州省", [("贵阳市", ["南明区", "云岩区", "花溪区"])]),
    ("西南", "云南省", [("昆明市", ["五华区", "盘龙区", "官渡区", "西山区"])]),
    ("西南", "西藏自治区", [("拉萨市", ["城关区"])]),
    ("西北", "陕西省", [("西安市", ["新城区", "碑林区", "莲湖区", "雁塔区", "未央区", "长安区"]), ("宝鸡市", ["渭滨区", "金台区"])]),
    ("西北", "甘肃省", [("兰州市", ["城关区", "七里河区", "安宁区"])]),
    ("西北", "青海省", [("西宁市", ["城中区", "城东区", "城西区"])]),
    ("西北", "宁夏回族自治区", [("银川市", ["兴庆区", "金凤区", "西夏区"])]),
    ("西北", "新疆维吾尔自治区", [("乌鲁木齐市", ["天山区", "沙依巴克区", "新市区", "水磨沟区"])]),
    ("东北", "辽宁省", [("沈阳市", ["和平区", "沈河区", "皇姑区", "铁西区"]), ("大连市", ["中山区", "西岗区", "沙河口区"])]),
    ("东北", "吉林省", [("长春市", ["南关区", "宽城区", "朝阳区"])]),
    ("东北", "黑龙江省", [("哈尔滨市", ["道里区", "南岗区", "道外区", "松北区"])]),
]

# ---- 省份权重(人口/经济) ----
PROVINCE_WEIGHTS = {
    "广东省": 120, "江苏省": 85, "浙江省": 80, "山东省": 100, "河南省": 95,
    "四川省": 90, "湖北省": 60, "湖南省": 70, "河北省": 75, "安徽省": 65,
    "福建省": 45, "北京市": 50, "上海市": 50, "重庆市": 35, "陕西省": 40,
    "江西省": 45, "辽宁省": 42, "黑龙江省": 32, "吉林省": 25, "山西省": 35,
    "云南省": 48, "贵州省": 38, "广西壮族自治区": 50, "甘肃省": 25, "海南省": 15,
    "内蒙古自治区": 24, "新疆维吾尔自治区": 26, "宁夏回族自治区": 8, "青海省": 8, "西藏自治区": 4, "天津市": 30,
}

# ---- 优惠券模板 ----
COUPON_TEMPLATES = [
    (1, 99, 20, "新人满99减20券", "满99元可用,限新用户"),
    (1, 199, 30, "满199减30券", "满199元可用"),
    (1, 299, 50, "满299减50券", "满299元可用"),
    (1, 499, 80, "满499减80券", "满499元可用"),
    (1, 999, 150, "满999减150券", "满999元可用"),
    (1, 1999, 300, "满1999减300券", "满1999元可用"),
    (1, 2999, 500, "大额满2999减500券", "满2999元可用,数码家电专用"),
    (2, None, 10, "无门槛10元券", "无门槛,全场通用"),
    (2, None, 15, "无门槛15元新人券", "无门槛,限新用户"),
    (2, None, 20, "无门槛20元券", "无门槛,部分商品可用"),
    (1, 599, 100, "美妆满599减100券", "满599元可用,美妆个护专用"),
    (1, 159, 25, "服饰满159减25券", "满159元可用,服饰鞋包专用"),
    (1, 89, 15, "食品满89减15券", "满89元可用,食品生鲜专用"),
    (2, None, 30, "生日无门槛30元券", "无门槛,生日月专用"),
    (1, 399, 60, "满399减60券", "满399元可用"),
    (1, 899, 120, "满899减120券", "满899元可用"),
    (2, None, 50, "VIP无门槛50元券", "无门槛,黄金以上会员专享"),
    (1, 1299, 200, "满1299减200券", "满1299元可用"),
    (1, 699, 100, "家电满699减100券", "满699元可用,家用电器专用"),
    (2, None, 8, "无门槛8元券", "无门槛,全场通用"),
]

# ---- 数据字典 (base_dic) ----
# (dic_type, code, name, sort) 与用户提供的 INSERT 完全一致
BASE_DIC_DATA = [
    # BUSINESS 业务域(交易)
    ('BUSINESS_order_status', '1001', '未支付', 1),
    ('BUSINESS_order_status', '1002', '支付中', 2),
    ('BUSINESS_order_status', '1003', '已支付', 3),
    ('BUSINESS_order_status', '1004', '已取消', 4),
    ('BUSINESS_order_status', '1005', '已完成', 5),
    ('BUSINESS_order_status', '1006', '退款中', 6),
    ('BUSINESS_order_status', '1007', '已退款', 7),
    ('BUSINESS_payment_way', '1', '微信', 1),
    ('BUSINESS_payment_way', '2', '支付宝', 2),
    ('BUSINESS_payment_way', '3', '银联', 3),
    ('BUSINESS_payment_way', '4', '货到付款', 4),
    ('BUSINESS_source_type', '2401', '用户下单', 1),
    ('BUSINESS_source_type', '2402', '促销活动', 2),
    ('BUSINESS_source_type', '2403', '购物车', 3),
    ('BUSINESS_payment_status', '1001', '未支付', 1),
    ('BUSINESS_payment_status', '1002', '已支付', 2),
    ('BUSINESS_payment_status', '1003', '已取消', 3),
    ('BUSINESS_refund_status', '0701', '申请退款', 1),
    ('BUSINESS_refund_status', '0702', '退款中', 2),
    ('BUSINESS_refund_status', '0703', '已退款', 3),
    ('BUSINESS_refund_status', '0704', '退款拒绝', 4),
    ('BUSINESS_refund_type', '1', '仅退款', 1),
    ('BUSINESS_refund_type', '2', '退货退款', 2),
    ('BUSINESS_refund_type', '3', '换货', 3),
    ('BUSINESS_refund_reason_type', '1', '质量问题', 1),
    ('BUSINESS_refund_reason_type', '2', '与描述不符', 2),
    ('BUSINESS_refund_reason_type', '3', '七天无理由', 3),
    ('BUSINESS_refund_reason_type', '4', '未收到货', 4),
    ('BUSINESS_refund_reason_type', '5', '其他', 5),
    ('BUSINESS_coupon_status', '1401', '未使用', 1),
    ('BUSINESS_coupon_status', '1404', '已锁定', 2),
    ('BUSINESS_coupon_status', '1402', '已核销', 3),
    ('BUSINESS_coupon_status', '1403', '已过期', 4),
    # BASE 基础域(用户/商品/通用)
    ('BASE_user_level', '1', '普通会员', 1),
    ('BASE_user_level', '2', '青铜', 2),
    ('BASE_user_level', '3', '白银', 3),
    ('BASE_user_level', '4', '黄金', 4),
    ('BASE_user_level', '5', '铂金', 5),
    ('BASE_user_level', '6', '钻石', 6),
    ('BASE_user_status', '1001', '正常', 1),
    ('BASE_user_status', '1002', '冻结', 2),
    ('BASE_user_status', '1003', '注销', 3),
    ('BASE_gender', '0', '未知', 0),
    ('BASE_gender', '1', '男', 1),
    ('BASE_gender', '2', '女', 2),
    ('BASE_coupon_type', '1', '满减券', 1),
    ('BASE_coupon_type', '2', '无门槛券', 2),
    # LOG 日志域(埋点)
    ('LOG_event_type', '1001', '启动', 1),
    ('LOG_event_type', '1002', '页面浏览', 2),
    ('LOG_event_type', '1003', '动作', 3),
    ('LOG_event_type', '1004', '曝光', 4),
    ('LOG_event_type', '1005', '错误', 5),
]

# ---- 业务常量 ----
ORDER_STATUS_NAMES = {1001: "未支付", 1002: "支付中", 1003: "已支付", 1004: "已取消", 1005: "已完成", 1006: "退款中", 1007: "已退款"}
PAYMENT_WAYS = [1, 2, 3, 4]
PAYMENT_WAY_WEIGHTS = [40, 35, 15, 10]
PAYMENT_WAY_NAMES = {1: "微信", 2: "支付宝", 3: "银联", 4: "货到付款"}
USER_LEVELS = [1, 2, 3, 4, 5, 6]
USER_LEVEL_WEIGHTS = [30, 25, 20, 15, 7, 3]
# 退款原因 -> 原因类型映射 (与 base_dic.BUSINESS_refund_reason_type 对齐: 1质量问题/2与描述不符/3七天无理由/4未收到货/5其他)
REFUND_REASONS = [
    ("商品质量问题", 1),
    ("收到商品损坏", 1),
    ("商品与描述不符", 2),
    ("尺寸不合适", 2),
    ("7天无理由退款", 3),
    ("不想要了", 3),
    ("未收到货", 4),
    ("物流太慢", 5),
    ("发错货了", 5),
    ("效果不好", 5),
]
REFUND_TYPES = [1, 2, 3]
REFUND_TYPE_WEIGHTS = [50, 40, 10]
SOURCE_TYPES = [2401, 2402, 2403]
SOURCE_TYPE_WEIGHTS = [50, 15, 35]
USER_STATUS = [1001, 1002, 1003]
USER_STATUS_WEIGHTS = [95, 3, 2]
COUPON_STATUSES = [1401, 1402, 1403, 1404]  # 未使用/已核销/已过期/已锁定


# ============================================================
#  二、工具函数
# ============================================================

def wchoice(options, weights):
    return random.choices(options, weights=weights, k=1)[0]

def random_province():
    provinces = list(PROVINCE_WEIGHTS.keys())
    weights = list(PROVINCE_WEIGHTS.values())
    return wchoice(provinces, weights)

def random_price(category):
    low, high = CATEGORY_PRICES.get(category, DEFAULT_PRICE)
    if HAS_NUMPY:
        base = np.random.beta(2, 5)
    else:
        base = random.random() ** 1.8
    return round(Decimal(str(low + (high - low) * base)), 2)

def random_stock():
    if HAS_NUMPY:
        stock = int(np.random.gamma(shape=2.0, scale=200))
    else:
        stock = int(random.gauss(400, 250))
    return max(0, min(stock, 5000))

def random_business_time(days_back=180):
    """生成符合业务时段特征的时间戳: 晚间高峰、工作日多"""
    now = datetime.now()
    start = now - timedelta(days=days_back)
    random_days = random.randint(0, days_back - 1)
    date = start + timedelta(days=random_days)
    hour_weights = [1,1,1,1,1,1,1,2, 3,5,6,6, 4,7,8,8, 7,8,9,10, 9,6,3,2]
    hour = random.choices(range(24), weights=hour_weights, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return date.replace(hour=hour, minute=minute, second=second)

def generate_spu_name(brand, category3_name):
    """生成真实感 SPU 名称"""
    if category3_name == "智能手机":
        models = {
            "华为": ["Mate 60 Pro", "Mate 60", "P60 Pro", "Nova 12 Ultra", "畅享 70 Pro"],
            "苹果": ["iPhone 15 Pro Max", "iPhone 15 Pro", "iPhone 15", "iPhone 14 Plus"],
            "小米": ["小米14 Pro", "小米14", "Redmi K70 Pro", "Redmi Note 13 Pro"],
            "OPPO": ["Find X7 Ultra", "Reno 11 Pro", "A3 Pro"],
            "vivo": ["X100 Pro", "S18 Pro", "Y100", "iQOO 12"],
            "三星": ["Galaxy S24 Ultra", "Galaxy S24+", "Galaxy A55"],
            "荣耀": ["Magic 6 Pro", "100 Pro", "X50"],
        }
        model = random.choice(models.get(brand, [f"{brand} {random.randint(10, 99)}"]))
        return f"{brand} {model}"
    elif category3_name == "笔记本":
        return f"{brand} {random.choice(['Pro', 'Air', 'Plus', 'Book'])} {random.randint(13,16)}"
    elif category3_name == "平板电脑":
        return f"{brand} {random.choice(['Pad Pro', 'Pad', 'Tab S', 'MatePad'])} {random.randint(10, 12)}"
    elif category3_name == "白酒":
        if brand == "茅台":
            return random.choice(["飞天茅台 53度", "茅台王子酒", "茅台迎宾酒"])
        elif brand == "五粮液":
            return random.choice(["五粮液 普五第八代", "五粮液 1618", "五粮液 尖庄"])
        return f"{brand} 53度酱香型"
    elif category3_name in ("运动鞋",):
        return f"{brand} {random.choice(['Air Max', 'Revolution', 'Pegasus', 'Ultraboost', 'Cloud'])} {random.randint(1, 99)}"
    elif category3_name in ("T恤", "衬衫", "卫衣"):
        return f"{brand} {category3_name} {random.choice(['纯棉', '冰丝凉感', '宽松圆领', '印花短袖'])}"
    elif category3_name in ("连衣裙",):
        return f"{brand} {random.choice(['法式', '碎花', '修身', 'A字'])}连衣裙"
    elif category3_name in ("面霜", "精华", "面膜"):
        suffixes = {"面霜": "保湿修护面霜", "精华": "赋活精华液", "面膜": "补水面膜贴"}
        return f"{brand} {suffixes.get(category3_name, category3_name)}"
    elif category3_name in ("空调", "冰箱", "洗衣机", "电视"):
        if category3_name == "空调":
            return f"{brand} {category3_name} {random.choice(['1匹', '1.5匹', '2匹', '3匹'])}变频 {random.choice(['一级能效', '新一级能效'])}"
        elif category3_name in ("冰箱", "洗衣机", "电视"):
            return f"{brand} {category3_name} {random.choice(['Pro', 'Max', '智享版', '旗舰版'])} {random.randint(100, 999)}L"
    else:
        return f"{brand} {category3_name} {random.choice(['经典款', '升级版', '尊享版', '旗舰版', '标准版'])}"

def generate_sku_name(spu_name, specs_dict):
    """生成 SKU 名称 = SPU名 + 规格组合"""
    spec_parts = list(specs_dict.values())
    return f"{spu_name} {' '.join(spec_parts)}"

def generate_sku_attr(specs_dict):
    """生成 SKU 规格属性 JSON"""
    return json.dumps(specs_dict, ensure_ascii=False)

def random_age():
    if HAS_NUMPY:
        age = int(np.clip(np.random.normal(30, 9), 18, 65))
    else:
        age = max(18, min(65, int(random.gauss(30, 9))))
    return age

def age_range_str(age):
    if age <= 25: return "18-25"
    elif age <= 35: return "26-35"
    elif age <= 45: return "36-45"
    else: return "46+"

def random_register_time():
    days_ago = int(random.expovariate(1 / 365))
    days_ago = min(days_ago, 1095)
    return datetime.now() - timedelta(days=days_ago)


# ============================================================
#  三、维度表数据生成
# ============================================================

def gen_base_dic_data():
    """生成数据字典表数据 (固定数据,与用户提供的 INSERT 完全一致)"""
    records = []
    for i, (dic_type, code, name, sort) in enumerate(BASE_DIC_DATA, 1):
        records.append({
            'id': i, 'dic_type': dic_type, 'code': code,
            'name': name, 'sort': sort,
            'create_time': datetime(2024, 1, 1),
        })
    return records

def gen_region_data():
    """生成省/市/区三级地区数据"""
    regions = []
    rid = 1
    for big_region, province, cities in REGIONS_DATA:
        # 省
        provinces_entry = {
            'id': rid, 'region_code': f'{rid:06d}', 'region_name': province,
            'level': 1, 'parent_id': None, 'big_region': big_region,
            'create_time': datetime(2024, 1, 1),
        }
        province_id = rid
        regions.append(provinces_entry)
        rid += 1
        for city, districts in cities:
            # 市
            city_entry = {
                'id': rid, 'region_code': f'{rid:06d}', 'region_name': city,
                'level': 2, 'parent_id': province_id, 'big_region': None,
                'create_time': datetime(2024, 1, 1),
            }
            city_id = rid
            regions.append(city_entry)
            rid += 1
            for district in districts:
                # 区
                regions.append({
                    'id': rid, 'region_code': f'{rid:06d}', 'region_name': district,
                    'level': 3, 'parent_id': city_id, 'big_region': None,
                    'create_time': datetime(2024, 1, 1),
                })
                rid += 1
    return regions

def gen_brand_data():
    """生成品牌数据"""
    brands = []
    for i, name in enumerate(BRANDS, 1):
        brands.append({
            'id': i, 'brand_name': name,
            'logo_url': f'https://img.gmall.com/brand/logo_{i:03d}.png',
            'create_time': datetime(2024, 1, 1),
        })
    return brands

def gen_category_data():
    """生成三级分类数据"""
    categories = []
    cid = 1
    for cat1_name, sub_cats in CATEGORY_TREE:
        # 一级
        cat1_id = cid
        categories.append({
            'id': cid, 'category_name': cat1_name, 'level': 1, 'parent_id': None,
            'create_time': datetime(2024, 1, 1),
        })
        cid += 1
        for cat2_name, cat3_list in sub_cats:
            # 二级
            cat2_id = cid
            categories.append({
                'id': cid, 'category_name': cat2_name, 'level': 2, 'parent_id': cat1_id,
                'create_time': datetime(2024, 1, 1),
            })
            cid += 1
            for cat3_name in cat3_list:
                # 三级
                categories.append({
                    'id': cid, 'category_name': cat3_name, 'level': 3, 'parent_id': cat2_id,
                    'create_time': datetime(2024, 1, 1),
                })
                cid += 1
    return categories

def gen_coupon_info_data():
    """生成优惠券模板数据"""
    coupons = []
    for i, (ctype, full, reduce, name, condition) in enumerate(COUPON_TEMPLATES, 1):
        coupons.append({
            'id': i, 'coupon_type': ctype, 'full_amount': Decimal(str(full)) if full else None,
            'reduce_amount': Decimal(str(reduce)), 'coupon_name': name,
            'use_condition': condition,
            'create_time': random_register_time(),
        })
    return coupons

def gen_user_info_data(count, region_data):
    """生成用户数据"""
    provinces = [r for r in region_data if r['level'] == 1]
    users = []
    for i in range(1, count + 1):
        gender = random.choice([0, 1, 2])
        if gender == 1:
            name = fake.name_male()
        elif gender == 2:
            name = fake.name_female()
        else:
            name = fake.name()
        age = random_age()
        province = random_province()
        province_entry = next((p for p in provinces if p['region_name'] == province), provinces[0])
        # 找该省下的市和区
        cities = [r for r in region_data if r['parent_id'] == province_entry['id']]
        if cities:
            city = random.choice(cities)
            districts = [r for r in region_data if r['parent_id'] == city['id']]
            district_name = random.choice(districts)['region_name'] if districts else city['region_name']
            city_name = city['region_name']
        else:
            city_name = province
            district_name = ''
        addr_detail = fake.street_address()
        birthday = datetime.now() - timedelta(days=age * 365 + random.randint(0, 364))
        users.append({
            'id': i,
            'login_name': fake.user_name() + str(i),
            'nick_name': fake.user_name(),
            'name': name,
            'phone_num': fake.phone_number(),
            'email': fake.email(),
            'user_level': wchoice(USER_LEVELS, USER_LEVEL_WEIGHTS),
            'birthday': birthday.date(),
            'gender': gender,
            'age_range': age_range_str(age),
            'status': wchoice(USER_STATUS, USER_STATUS_WEIGHTS),
            'create_time': random_register_time(),
        })
    return users

def gen_spu_sku_data(brand_data, category_data, spu_count):
    """生成 SPU 和 SKU 数据"""
    cat3_list = [c for c in category_data if c['level'] == 3]
    spus = []
    skus = []
    spu_id = 1
    sku_id = 1
    for _ in range(spu_count):
        brand = random.choice(brand_data)
        cats = BRAND_CATEGORIES.get(brand['brand_name'], [])
        if not cats:
            cat3 = random.choice(cat3_list)
            cat3_name = cat3['category_name']
            cat3_id = cat3['id']
        else:
            cat3_name = random.choice(cats)
            cat3_entry = next((c for c in cat3_list if c['category_name'] == cat3_name), None)
            if cat3_entry:
                cat3_id = cat3_entry['id']
            else:
                cat3_id = random.choice(cat3_list)['id']
        spu_name = generate_spu_name(brand['brand_name'], cat3_name)
        spus.append({
            'id': spu_id, 'spu_name': spu_name,
            'description': f'{brand["brand_name"]}正品,{cat3_name},品质保证',
            'category3_id': cat3_id, 'brand_id': brand['id'],
            'create_time': random_register_time(),
        })
        # 为每个 SPU 生成 2-5 个 SKU
        specs_def = CATEGORY_SPECS.get(cat3_name, DEFAULT_SPECS)
        num_skus = random.randint(2, min(5, len(specs_def[0][1]) * (len(specs_def[1][1]) if len(specs_def) > 1 else 1)))
        # 生成规格组合
        spec_combos = []
        if len(specs_def) == 1:
            spec_combos = [{specs_def[0][0]: v} for v in specs_def[0][1]]
        else:
            for v1 in specs_def[0][1]:
                for v2 in specs_def[1][1]:
                    spec_combos.append({specs_def[0][0]: v1, specs_def[1][0]: v2})
        random.shuffle(spec_combos)
        for spec_dict in spec_combos[:num_skus]:
            price = random_price(cat3_name)
            sku_name = generate_sku_name(spu_name, spec_dict)
            skus.append({
                'id': sku_id, 'sku_name': sku_name, 'spu_id': spu_id,
                'category3_id': cat3_id, 'brand_id': brand['id'],
                'price': price,
                'weight': Decimal(str(round(random.uniform(0.1, 20.0), 2))),
                'img_url': f'https://img.gmall.com/sku/{sku_id:06d}.jpg',
                'is_sale': random.choice([1, 1, 1, 0]),
                'sku_attr': generate_sku_attr(spec_dict),
                'create_time': random_register_time(),
            })
            sku_id += 1
        spu_id += 1
    return spus, skus


# ============================================================
#  四、业务表数据生成 (订单链路)
# ============================================================

def gen_order_chain(order_id, user_ids, sku_list, province_ids, coupon_list):
    """
    生成一个完整的订单链路:
    订单主表 + 明细 + 状态履历 + 支付 + 退款(可选) + 优惠券核销(可选) + 购物车回填(可选)
    返回 dict: {order, details, status_logs, payment, refund, coupon_use_update, cart_update}
    """
    user_id = random.choice(user_ids)
    province_id = random.choice(province_ids)

    # 收货信息快照
    consignee_name = fake.name()
    consignee_tel = fake.phone_number()
    delivery_addr = f"{fake.province()}{fake.city_name()}{fake.district()}{fake.street_address()}"

    create_time = random_business_time(days_back=180)
    out_trade_no = f"OTN{create_time.strftime('%Y%m%d%H%M%S')}{order_id:08d}"

    # 决定订单状态路径
    path = wchoice(
        ['cancelled', 'completed', 'refunded', 'refunding', 'pending'],
        weights=[8, 72, 10, 4, 6]
    )

    # 生成 1-3 条明细
    num_details = wchoice([1, 2, 3], weights=[60, 30, 10])
    selected_skus = random.sample(sku_list, min(num_details, len(sku_list)))
    source_type = wchoice(SOURCE_TYPES, SOURCE_TYPE_WEIGHTS)
    source_id = random.randint(1, 100000) if source_type == 2403 else None

    details_raw = []
    original_total = Decimal('0.00')
    for idx, sku in enumerate(selected_skus):
        qty = wchoice([1, 2, 3, 4], weights=[55, 25, 12, 8])
        line_total = Decimal(str(sku['price'])) * qty
        original_total += line_total
        details_raw.append({
            'sku_id': sku['id'], 'sku_name': sku['sku_name'],
            'img_url': sku['img_url'], 'order_price': sku['price'],
            'sku_num': qty, 'order_line_no': idx + 1,
            'source_type': source_type, 'source_id': source_id,
            'line_total': line_total,
        })

    # 优惠券处理
    coupon = None
    coupon_reduce = Decimal('0.00')
    used_coupon = False
    if random.random() < 0.25 and coupon_list:
        coupon = random.choice(coupon_list)
        if coupon['coupon_type'] == 1:  # 满减券
            if original_total >= (coupon['full_amount'] or Decimal('999999')):
                coupon_reduce = coupon['reduce_amount']
                used_coupon = True
        else:  # 无门槛券
            coupon_reduce = coupon['reduce_amount']
            used_coupon = True

    total_amount = original_total - coupon_reduce

    # 优惠分摊到各明细
    for d in details_raw:
        if used_coupon and coupon_reduce > 0:
            ratio = d['line_total'] / original_total
            d['split_coupon_amount'] = (coupon_reduce * ratio).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
        else:
            d['split_coupon_amount'] = Decimal('0.0000')
        d['split_activity_amount'] = Decimal('0.0000')
        d['split_total_amount'] = (d['line_total'] - d['split_coupon_amount']).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)

    # 修正最后一行确保总和精确
    if used_coupon and coupon_reduce > 0:
        total_split_coupon = sum(d['split_coupon_amount'] for d in details_raw)
        diff = coupon_reduce - total_split_coupon
        details_raw[-1]['split_coupon_amount'] = (details_raw[-1]['split_coupon_amount'] + diff).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
        details_raw[-1]['split_total_amount'] = (details_raw[-1]['line_total'] - details_raw[-1]['split_coupon_amount']).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)

    # 状态流转
    status_logs = []
    current_status = 1001
    operate_time = create_time
    receive_time = None
    expire_time = None
    payment = None
    refund = None
    coupon_use_update = None
    cart_update = None

    status_logs.append({'order_status': 1001, 'create_time': create_time})

    if path == 'cancelled':
        # 1001 -> 1004 (超时取消)
        cancel_time = create_time + timedelta(minutes=random.randint(15, 120))
        current_status = 1004
        status_logs.append({'order_status': 1004, 'create_time': cancel_time})
        operate_time = cancel_time
        expire_time = cancel_time
        if used_coupon:
            coupon_use_update = {'coupon_status': 1401}  # 退回未使用

    elif path == 'pending':
        # 1001 -> 1002 (支付中) 或 1001 -> 1003 (刚支付)
        if random.random() < 0.4:
            pay_start = create_time + timedelta(seconds=random.randint(5, 300))
            current_status = 1002
            status_logs.append({'order_status': 1002, 'create_time': pay_start})
            operate_time = pay_start
        else:
            pay_start = create_time + timedelta(seconds=random.randint(5, 300))
            current_status = 1002
            status_logs.append({'order_status': 1002, 'create_time': pay_start})
            pay_success = pay_start + timedelta(seconds=random.randint(3, 120))
            current_status = 1003
            status_logs.append({'order_status': 1003, 'create_time': pay_success})
            operate_time = pay_success
            payment = _make_payment(order_id, out_trade_no, user_id, create_time, pay_start, pay_success, total_amount, wchoice(PAYMENT_WAYS, PAYMENT_WAY_WEIGHTS), 1002)
            if used_coupon:
                coupon_use_update = {'coupon_status': 1404}  # 已锁定

    elif path == 'completed':
        # 1001 -> 1002 -> 1003 -> 1005
        pay_start = create_time + timedelta(seconds=random.randint(5, 600))
        status_logs.append({'order_status': 1002, 'create_time': pay_start})
        pay_success = pay_start + timedelta(seconds=random.randint(3, 300))
        status_logs.append({'order_status': 1003, 'create_time': pay_success})
        complete_time = pay_success + timedelta(hours=random.randint(24, 168))
        current_status = 1005
        status_logs.append({'order_status': 1005, 'create_time': complete_time})
        operate_time = complete_time
        receive_time = complete_time
        pay_way = wchoice(PAYMENT_WAYS, PAYMENT_WAY_WEIGHTS)
        payment = _make_payment(order_id, out_trade_no, user_id, create_time, pay_start, pay_success, total_amount, pay_way, 1002)
        if used_coupon:
            coupon_use_update = {'coupon_status': 1402, 'used_time': complete_time}  # 已核销
        # 购物车回填
        if source_type == 2403:
            cart_update = {'cart_order_id': order_id}

    elif path == 'refunded':
        # 1001 -> 1002 -> 1003 -> 1005 -> 1006 -> 1007
        pay_start = create_time + timedelta(seconds=random.randint(5, 600))
        status_logs.append({'order_status': 1002, 'create_time': pay_start})
        pay_success = pay_start + timedelta(seconds=random.randint(3, 300))
        status_logs.append({'order_status': 1003, 'create_time': pay_success})
        complete_time = pay_success + timedelta(hours=random.randint(24, 168))
        status_logs.append({'order_status': 1005, 'create_time': complete_time})
        refund_start = complete_time + timedelta(hours=random.randint(1, 72))
        status_logs.append({'order_status': 1006, 'create_time': refund_start})
        refund_done = refund_start + timedelta(hours=random.randint(1, 48))
        current_status = 1007
        status_logs.append({'order_status': 1007, 'create_time': refund_done})
        operate_time = refund_done
        receive_time = complete_time
        pay_way = wchoice(PAYMENT_WAYS, PAYMENT_WAY_WEIGHTS)
        payment = _make_payment(order_id, out_trade_no, user_id, create_time, pay_start, pay_success, total_amount, pay_way, 1003)  # 支付已取消(退款)
        if used_coupon:
            coupon_use_update = {'coupon_status': 1402, 'used_time': complete_time}  # 已核销
        # 生成退款记录
        refund_detail = random.choice(details_raw)
        _reason, _reason_type = random.choice(REFUND_REASONS)
        refund = {
            'user_id': user_id, 'order_id': order_id,
            'sku_name': refund_detail['sku_name'],
            'refund_amount': refund_detail['split_total_amount'].quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
            'refund_num': refund_detail['sku_num'],
            'refund_status': 703,  # 已退款
            'refund_type': wchoice(REFUND_TYPES, REFUND_TYPE_WEIGHTS),
            'refund_reason': _reason,
            'refund_reason_type': _reason_type,
            'create_time': refund_start,
            'refund_time': refund_done,
            'operate_time': refund_done,
        }
        if source_type == 2403:
            cart_update = {'cart_order_id': order_id}

    elif path == 'refunding':
        # 1001 -> 1002 -> 1003 -> 1006 (退款中)
        pay_start = create_time + timedelta(seconds=random.randint(5, 600))
        status_logs.append({'order_status': 1002, 'create_time': pay_start})
        pay_success = pay_start + timedelta(seconds=random.randint(3, 300))
        status_logs.append({'order_status': 1003, 'create_time': pay_success})
        refund_start = pay_success + timedelta(hours=random.randint(1, 168))
        current_status = 1006
        status_logs.append({'order_status': 1006, 'create_time': refund_start})
        operate_time = refund_start
        pay_way = wchoice(PAYMENT_WAYS, PAYMENT_WAY_WEIGHTS)
        payment = _make_payment(order_id, out_trade_no, user_id, create_time, pay_start, pay_success, total_amount, pay_way, 1003)
        if used_coupon:
            coupon_use_update = {'coupon_status': 1404}  # 已锁定(退款中不核销)
        refund_detail = random.choice(details_raw)
        _reason, _reason_type = random.choice(REFUND_REASONS)
        refund = {
            'user_id': user_id, 'order_id': order_id,
            'sku_name': refund_detail['sku_name'],
            'refund_amount': refund_detail['split_total_amount'].quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
            'refund_num': refund_detail['sku_num'],
            'refund_status': 702,  # 退款中
            'refund_type': wchoice(REFUND_TYPES, REFUND_TYPE_WEIGHTS),
            'refund_reason': _reason,
            'refund_reason_type': _reason_type,
            'create_time': refund_start,
            'refund_time': None,
            'operate_time': refund_start,
        }

    # 构建订单主表记录
    pay_way = payment['payment_type'] if payment else None
    order = {
        'id': order_id,
        'consignee': consignee_name,
        'consignee_tel': consignee_tel,
        'total_amount': total_amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
        'order_status': current_status,
        'user_id': user_id,
        'payment_way': pay_way,
        'delivery_address': delivery_addr,
        'order_comment': random.choice([None, None, None, '尽快发货', '工作日送达', '不要冰袋', '送礼用,请包装']),
        'out_trade_no': out_trade_no,
        'trade_body': f"{len(details_raw)}件商品",
        'create_time': create_time,
        'operate_time': operate_time,
        'receive_time': receive_time,
        'expire_time': expire_time,
        'province_id': province_id,
        'coupon_reduce_amount': coupon_reduce.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
        'original_total_amount': original_total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
    }

    # 优惠券领用记录(如果用了券)
    coupon_use = None
    if used_coupon and coupon:
        get_time = create_time - timedelta(days=random.randint(1, 30))
        lock_time = create_time if current_status >= 1002 else None
        using_time = create_time if current_status >= 1003 else None
        used_time = coupon_use_update.get('used_time') if coupon_use_update else None
        cu_status = coupon_use_update.get('coupon_status', 1401) if coupon_use_update else 1401
        coupon_use = {
            'coupon_id': coupon['id'], 'coupon_type': coupon['coupon_type'],
            'user_id': user_id, 'order_id': order_id,
            'coupon_status': cu_status,
            'coupon_reduce_amount': coupon_reduce.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
            'get_time': get_time, 'lock_time': lock_time,
            'using_time': using_time, 'used_time': used_time,
            'expire_time': get_time + timedelta(days=30),
        }

    return {
        'order': order,
        'details': details_raw,
        'status_logs': status_logs,
        'payment': payment,
        'refund': refund,
        'coupon_use': coupon_use,
        'cart_update': cart_update,
    }


def _make_payment(order_id, out_trade_no, user_id, order_create, pay_start, pay_success, amount, pay_type, pay_status):
    """生成支付记录"""
    trade_no = f"TN{pay_success.strftime('%Y%m%d%H%M%S')}{random.randint(100000, 999999)}"
    callback = {
        "trade_status": "TRADE_SUCCESS",
        "total_amount": str(amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
        "buyer_id": f"2088{random.randint(10**11, 10**12 - 1)}",
        "trade_no": trade_no,
        "notify_time": pay_success.strftime('%Y-%m-%d %H:%M:%S'),
    }
    return {
        'out_trade_no': out_trade_no, 'order_id': order_id, 'user_id': user_id,
        'payment_type': pay_type, 'trade_no': trade_no,
        'total_amount': amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
        'payment_status': pay_status,
        'create_time': pay_start, 'callback_time': pay_success,
        'callback_content': json.dumps(callback, ensure_ascii=False),
    }


def gen_coupon_use_data(coupon_list, user_ids, count):
    """生成独立的优惠券领用记录(未使用的)"""
    records = []
    for i in range(count):
        coupon = random.choice(coupon_list)
        user_id = random.choice(user_ids)
        get_time = random_register_time()
        expire_time = get_time + timedelta(days=random.choice([7, 15, 30]))
        if expire_time > datetime.now():
            status = 1401  # 未使用
        else:
            status = 1403  # 已过期
        records.append({
            'coupon_id': coupon['id'], 'coupon_type': coupon['coupon_type'],
            'user_id': user_id, 'order_id': None,
            'coupon_status': status,
            'coupon_reduce_amount': None,
            'get_time': get_time, 'lock_time': None,
            'using_time': None, 'used_time': None,
            'expire_time': expire_time,
        })
    return records


def gen_cart_info_data(user_ids, sku_list, count, order_ids=None):
    """生成购物车数据(order_ids: 已下单的订单ID,用于回填)"""
    records = []
    for i in range(1, count + 1):
        user_id = random.choice(user_ids)
        sku = random.choice(sku_list)
        qty = wchoice([1, 2, 3, 4, 5], weights=[40, 25, 15, 10, 10])
        is_checked = random.choice([1, 1, 0])
        create_time = random_business_time(days_back=90)
        # 30%概率已下单
        order_id = random.choice(order_ids) if (order_ids and random.random() < 0.3) else None
        records.append({
            'user_id': user_id, 'sku_id': sku['id'],
            'sku_name': sku['sku_name'], 'category_id': sku['category3_id'],
            'cart_price': sku['price'], 'sku_num': qty,
            'img_url': sku['img_url'], 'sku_attr': sku['sku_attr'],
            'order_id': order_id, 'is_checked': is_checked,
            'create_time': create_time,
            'operate_time': create_time + timedelta(minutes=random.randint(1, 1440)) if random.random() < 0.5 else None,
        })
    return records


# ============================================================
#  五、DDL (用户提供)
# ============================================================

DDL_GMALL_BASE = """
CREATE DATABASE IF NOT EXISTS `gmall_base` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
"""

DDL_GMALL_BUSINESS = """
CREATE DATABASE IF NOT EXISTS `gmall_business` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
"""

DDL_TABLES = [
    ("gmall_base", """
    CREATE TABLE IF NOT EXISTS `gmall_base`.`coupon_info` (
      `id`              BIGINT        NOT NULL AUTO_INCREMENT COMMENT '优惠券ID(券模板)',
      `coupon_type`     TINYINT       NOT NULL                COMMENT '券类型:1满减券/2无门槛券',
      `full_amount`     DECIMAL(18,2) DEFAULT NULL            COMMENT '满减券门槛金额;无门槛为NULL',
      `reduce_amount`   DECIMAL(18,2) DEFAULT NULL            COMMENT '立减金额',
      `coupon_name`     VARCHAR(100)  DEFAULT NULL            COMMENT '券名称',
      `use_condition`   VARCHAR(500)  DEFAULT NULL            COMMENT '使用条件说明',
      `create_time`     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      `update_time`     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券模板表'
    """),
    ("gmall_base", """
    CREATE TABLE IF NOT EXISTS `gmall_base`.`base_brand` (
      `id`          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '品牌ID',
      `brand_name`  VARCHAR(100) NOT NULL                COMMENT '品牌名称',
      `logo_url`    VARCHAR(200) DEFAULT NULL            COMMENT '品牌LOGO',
      `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      `update_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='品牌表'
    """),
    ("gmall_base", """
    CREATE TABLE IF NOT EXISTS `gmall_base`.`base_category` (
      `id`            BIGINT      NOT NULL AUTO_INCREMENT COMMENT '分类ID',
      `category_name` VARCHAR(100) NOT NULL               COMMENT '分类名称',
      `level`         TINYINT     NOT NULL                COMMENT '层级:1/2/3',
      `parent_id`     BIGINT      DEFAULT NULL            COMMENT '父分类ID',
      `create_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      `update_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
      PRIMARY KEY (`id`),
      KEY `idx_parent_id` (`parent_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分类表'
    """),
    ("gmall_base", """
    CREATE TABLE IF NOT EXISTS `gmall_base`.`base_region` (
      `id`          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '地区ID',
      `region_code` VARCHAR(20)  DEFAULT NULL            COMMENT '行政区划代码',
      `region_name` VARCHAR(50)  NOT NULL                COMMENT '地区名称',
      `level`       TINYINT      NOT NULL                COMMENT '层级:1省/2市/3区',
      `parent_id`   BIGINT       DEFAULT NULL            COMMENT '父地区ID',
      `big_region`  VARCHAR(20)  DEFAULT NULL            COMMENT '大区',
      `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      PRIMARY KEY (`id`),
      KEY `idx_parent_id` (`parent_id`),
      KEY `idx_big_region` (`big_region`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='地区表'
    """),
    ("gmall_base", """
    CREATE TABLE IF NOT EXISTS `gmall_base`.`spu_info` (
      `id`           BIGINT       NOT NULL AUTO_INCREMENT COMMENT 'SPU_ID',
      `spu_name`     VARCHAR(200) NOT NULL                COMMENT 'SPU名称',
      `description`  VARCHAR(500) DEFAULT NULL            COMMENT '商品描述',
      `category3_id` BIGINT       NOT NULL                COMMENT '三级分类ID',
      `brand_id`     BIGINT       NOT NULL                COMMENT '品牌ID',
      `create_time`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      `update_time`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
      PRIMARY KEY (`id`),
      KEY `idx_category3_id` (`category3_id`),
      KEY `idx_brand_id` (`brand_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SPU表'
    """),
    ("gmall_base", """
    CREATE TABLE IF NOT EXISTS `gmall_base`.`sku_info` (
      `id`            BIGINT       NOT NULL AUTO_INCREMENT COMMENT 'SKU_ID',
      `sku_name`      VARCHAR(200) NOT NULL                COMMENT 'SKU名称',
      `spu_id`        BIGINT       NOT NULL                COMMENT 'SPU_ID',
      `category3_id`  BIGINT       NOT NULL                COMMENT '三级分类ID',
      `brand_id`      BIGINT       NOT NULL                COMMENT '品牌ID',
      `price`         DECIMAL(18,2) DEFAULT NULL           COMMENT '商品价格',
      `weight`        DECIMAL(10,2) DEFAULT NULL           COMMENT '重量(kg)',
      `img_url`       VARCHAR(200)  DEFAULT NULL           COMMENT '图片URL',
      `is_sale`       TINYINT(1)    DEFAULT 0              COMMENT '是否上架:0/1',
      `sku_attr`      VARCHAR(500)  DEFAULT NULL           COMMENT 'SKU规格属性JSON',
      `create_time`   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      `update_time`   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
      PRIMARY KEY (`id`),
      KEY `idx_spu_id` (`spu_id`),
      KEY `idx_category3_id` (`category3_id`),
      KEY `idx_brand_id` (`brand_id`),
      KEY `idx_is_sale` (`is_sale`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SKU表'
    """),
    ("gmall_base", """
    CREATE TABLE IF NOT EXISTS `gmall_base`.`user_info` (
      `id`           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '用户ID',
      `login_name`   VARCHAR(50)  NOT NULL                COMMENT '登录账号',
      `nick_name`    VARCHAR(50)  DEFAULT NULL            COMMENT '昵称',
      `name`         VARCHAR(50)  DEFAULT NULL            COMMENT '真实姓名',
      `phone_num`    VARCHAR(20)  DEFAULT NULL            COMMENT '手机号',
      `email`        VARCHAR(50)  DEFAULT NULL            COMMENT '邮箱',
      `user_level`   TINYINT      DEFAULT NULL            COMMENT '用户等级:1-6',
      `birthday`     DATE         DEFAULT NULL            COMMENT '出生日期',
      `gender`       TINYINT      DEFAULT NULL            COMMENT '性别:0未知/1男/2女',
      `age_range`    VARCHAR(20)  DEFAULT NULL            COMMENT '年龄段',
      `status`       SMALLINT     DEFAULT NULL            COMMENT '状态:1001正常/1002冻结/1003注销',
      `create_time`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '注册时间',
      `update_time`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
      PRIMARY KEY (`id`),
      UNIQUE KEY `uk_login_name` (`login_name`),
      KEY `idx_phone_num` (`phone_num`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表'
    """),
    ("gmall_business", """
    CREATE TABLE IF NOT EXISTS `gmall_business`.`order_info` (
      `id`                BIGINT         NOT NULL AUTO_INCREMENT COMMENT '订单编号',
      `consignee`         VARCHAR(100)   DEFAULT NULL            COMMENT '收货人姓名',
      `consignee_tel`     VARCHAR(20)    DEFAULT NULL            COMMENT '收货人手机号',
      `total_amount`      DECIMAL(18,2)  DEFAULT NULL            COMMENT '订单总金额',
      `order_status`      SMALLINT       DEFAULT NULL            COMMENT '订单状态',
      `user_id`           BIGINT         DEFAULT NULL            COMMENT '用户ID',
      `payment_way`       TINYINT        DEFAULT NULL            COMMENT '支付方式',
      `delivery_address`  VARCHAR(200)  DEFAULT NULL            COMMENT '收货地址',
      `order_comment`     VARCHAR(200)   DEFAULT NULL            COMMENT '订单备注',
      `out_trade_no`      VARCHAR(50)    DEFAULT NULL            COMMENT '对外交易编号',
      `trade_body`        VARCHAR(200)   DEFAULT NULL            COMMENT '交易主体',
      `create_time`       DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '下单时间',
      `operate_time`      DATETIME(3)    DEFAULT NULL            COMMENT '状态变更时间',
      `receive_time`      DATETIME(3)    DEFAULT NULL            COMMENT '确认收货时间',
      `expire_time`       DATETIME(3)    DEFAULT NULL            COMMENT '订单过期时间',
      `province_id`       INT            DEFAULT NULL            COMMENT '省份ID',
      `coupon_reduce_amount`   DECIMAL(18,2) DEFAULT 0.00        COMMENT '优惠券抵扣金额',
      `original_total_amount`  DECIMAL(18,2) DEFAULT NULL        COMMENT '订单原始总金额',
      `update_time`       DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间',
      PRIMARY KEY (`id`),
      KEY `idx_user_id` (`user_id`),
      KEY `idx_create_time` (`create_time`),
      KEY `idx_operate_time` (`operate_time`),
      UNIQUE KEY `uk_out_trade_no` (`out_trade_no`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单主表'
    """),
    ("gmall_business", """
    CREATE TABLE IF NOT EXISTS `gmall_business`.`order_status_log` (
      `id`           BIGINT    NOT NULL AUTO_INCREMENT COMMENT '履历主键',
      `order_id`     BIGINT    NOT NULL                COMMENT '订单ID',
      `order_status` SMALLINT  NOT NULL                COMMENT '订单状态',
      `create_time`  DATETIME(3) NOT NULL              COMMENT '进入该状态的时间',
      PRIMARY KEY (`id`),
      KEY `idx_order_status_log` (`order_id`, `order_status`),
      KEY `idx_create_time` (`create_time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单状态履历表'
    """),
    ("gmall_business", """
    CREATE TABLE IF NOT EXISTS `gmall_business`.`order_detail` (
      `id`                   BIGINT       NOT NULL AUTO_INCREMENT COMMENT '明细编号',
      `order_id`             BIGINT       NOT NULL                COMMENT '订单ID',
      `order_line_no`        INT          NOT NULL                COMMENT '明细行号',
      `sku_id`               BIGINT       NOT NULL                COMMENT 'SKU_ID',
      `sku_name`             VARCHAR(200) DEFAULT NULL            COMMENT 'SKU名称',
      `img_url`              VARCHAR(200) DEFAULT NULL            COMMENT '图片URL',
      `order_price`          DECIMAL(18,2) DEFAULT NULL           COMMENT '商品单价',
      `sku_num`              INT          DEFAULT NULL            COMMENT '购买数量',
      `create_time`          DATETIME(3)  NOT NULL                COMMENT '下单时间',
      `source_type`          SMALLINT     DEFAULT NULL            COMMENT '来源类型',
      `source_id`            BIGINT       DEFAULT NULL            COMMENT '来源ID',
      `split_activity_amount` DECIMAL(18,4) DEFAULT 0.00          COMMENT '分摊活动优惠金额',
      `coupon_id`            BIGINT       DEFAULT NULL            COMMENT '优惠券ID',
      `split_coupon_amount`   DECIMAL(18,4) DEFAULT 0.00          COMMENT '分摊优惠券金额',
      `split_total_amount`    DECIMAL(18,4) DEFAULT NULL          COMMENT '分摊后实际金额',
      `update_time`       DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间',
      PRIMARY KEY (`id`),
      UNIQUE KEY `uk_order_line` (`order_id`, `order_line_no`),
      KEY `idx_sku_id` (`sku_id`),
      KEY `idx_create_time` (`create_time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细表'
    """),
    ("gmall_business", """
    CREATE TABLE IF NOT EXISTS `gmall_business`.`cart_info` (
      `id`           BIGINT        NOT NULL AUTO_INCREMENT COMMENT '购物车主键',
      `user_id`      BIGINT        NOT NULL                COMMENT '用户ID',
      `sku_id`       BIGINT        NOT NULL                COMMENT 'SKU_ID',
      `sku_name`     VARCHAR(200)  DEFAULT NULL            COMMENT 'SKU名称',
      `category_id`  BIGINT        DEFAULT NULL            COMMENT '三级分类ID',
      `cart_price`   DECIMAL(18,2) DEFAULT NULL            COMMENT '加购时单价',
      `sku_num`      INT           DEFAULT 1               COMMENT '数量',
      `img_url`      VARCHAR(200)  DEFAULT NULL            COMMENT '图片URL',
      `sku_attr`     VARCHAR(500)  DEFAULT NULL            COMMENT '规格JSON',
      `order_id`     BIGINT        DEFAULT NULL            COMMENT '关联订单ID',
      `is_checked`   TINYINT(1)    DEFAULT 1               COMMENT '是否勾选',
      `create_time`  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '加购时间',
      `operate_time` DATETIME(3)   DEFAULT NULL            COMMENT '操作时间',
      `update_time`  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间',
      PRIMARY KEY (`id`),
      KEY `idx_user_id` (`user_id`),
      KEY `idx_sku_id` (`sku_id`),
      KEY `idx_order_id` (`order_id`),
      KEY `idx_create_time` (`create_time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='购物车表'
    """),
    ("gmall_business", """
    CREATE TABLE IF NOT EXISTS `gmall_business`.`payment_info` (
      `id`               BIGINT        NOT NULL AUTO_INCREMENT COMMENT '支付主键',
      `out_trade_no`     VARCHAR(50)   NOT NULL                COMMENT '对外交易编号',
      `order_id`         BIGINT        NOT NULL                COMMENT '订单ID',
      `user_id`          BIGINT        DEFAULT NULL            COMMENT '用户ID',
      `payment_type`     TINYINT       DEFAULT NULL            COMMENT '支付类型',
      `trade_no`         VARCHAR(50)   DEFAULT NULL            COMMENT '第三方流水号',
      `total_amount`     DECIMAL(18,2) DEFAULT NULL            COMMENT '支付金额',
      `payment_status`   SMALLINT      DEFAULT NULL            COMMENT '支付状态',
      `create_time`      DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '发起支付时间',
      `callback_time`    DATETIME(3)   DEFAULT NULL            COMMENT '回调时间',
      `callback_content` TEXT          DEFAULT NULL            COMMENT '回调内容JSON',
      `update_time`      DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间',
      PRIMARY KEY (`id`),
      UNIQUE KEY `uk_order_id` (`order_id`),
      UNIQUE KEY `uk_out_trade_no` (`out_trade_no`),
      KEY `idx_user_id` (`user_id`),
      KEY `idx_create_time` (`create_time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付表'
    """),
    ("gmall_business", """
    CREATE TABLE IF NOT EXISTS `gmall_business`.`refund_info` (
      `id`                 BIGINT        NOT NULL AUTO_INCREMENT COMMENT '退款主键',
      `user_id`            BIGINT        NOT NULL                COMMENT '用户ID',
      `order_id`           BIGINT        NOT NULL                COMMENT '订单ID',
      `order_detail_id`    BIGINT        NOT NULL                COMMENT '订单明细ID',
      `sku_name`           VARCHAR(200)  DEFAULT NULL            COMMENT 'SKU名称',
      `refund_amount`      DECIMAL(18,2) NOT NULL                COMMENT '退款金额',
      `refund_num`         INT           DEFAULT 1               COMMENT '退款数量',
      `refund_status`      SMALLINT      DEFAULT NULL            COMMENT '退款状态',
      `refund_type`        TINYINT       DEFAULT NULL            COMMENT '退款类型',
      `refund_reason`      VARCHAR(200)  DEFAULT NULL            COMMENT '退款原因',
      `refund_reason_type` TINYINT       DEFAULT NULL            COMMENT '退款原因类型',
      `create_time`        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '申请退款时间',
      `refund_time`        DATETIME(3)   DEFAULT NULL            COMMENT '退款到账时间',
      `operate_time`       DATETIME(3)   DEFAULT NULL            COMMENT '状态变更时间',
      `update_time`        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间',
      PRIMARY KEY (`id`),
      UNIQUE KEY `uk_order_detail` (`order_detail_id`),
      KEY `idx_order_id` (`order_id`),
      KEY `idx_user_id` (`user_id`),
      KEY `idx_create_time` (`create_time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='退款表'
    """),
    ("gmall_business", """
    CREATE TABLE IF NOT EXISTS `gmall_business`.`coupon_use` (
      `id`              BIGINT        NOT NULL AUTO_INCREMENT COMMENT '领用编号',
      `coupon_id`       BIGINT        NOT NULL                COMMENT '优惠券模板ID',
      `coupon_type`     TINYINT       DEFAULT NULL            COMMENT '券类型',
      `user_id`         BIGINT        NOT NULL                COMMENT '用户ID',
      `order_id`        BIGINT        DEFAULT NULL            COMMENT '订单ID',
      `coupon_status`   SMALLINT      DEFAULT NULL            COMMENT '券状态',
      `coupon_reduce_amount` DECIMAL(18,2) DEFAULT NULL       COMMENT '抵扣金额',
      `get_time`        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '领取时间',
      `lock_time`       DATETIME(3)   DEFAULT NULL            COMMENT '锁定时间',
      `using_time`      DATETIME(3)   DEFAULT NULL            COMMENT '下单用券时间',
      `used_time`       DATETIME(3)   DEFAULT NULL            COMMENT '核销时间',
      `expire_time`     DATETIME(3)   DEFAULT NULL            COMMENT '过期时间',
      `update_time`     DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间',
      PRIMARY KEY (`id`),
      KEY `idx_coupon_id` (`coupon_id`),
      KEY `idx_user_id` (`user_id`),
      KEY `idx_order_id` (`order_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券领用核销表'
    """),
]


# ============================================================
#  六、数据库操作
# ============================================================

def connect_mysql(host, port, user, password):
    try:
        conn = pymysql.connect(
            host=host, port=port, user=user, password=password,
            charset='utf8mb4',
            connect_timeout=30,
            read_timeout=300,
            write_timeout=300,
        )
        # 设置会话超时参数 (容错: 某些参数可能因权限/版本失败, 忽略错误)
        cur = conn.cursor()
        for sql in [
            "SET SESSION wait_timeout=28800",
            "SET SESSION net_read_timeout=300",
            "SET SESSION net_write_timeout=300",
            "SET SESSION interactive_timeout=28800",
        ]:
            try:
                cur.execute(sql)
            except pymysql.Error:
                pass
        conn.commit()
        cur.close()
        print(f"[OK] 已连接 MySQL: {host}:{port}")
        return conn
    except pymysql.Error as e:
        print(f"[ERROR] MySQL 连接失败: {e}")
        sys.exit(1)

def execute_ddl(conn):
    cur = conn.cursor()
    cur.execute(DDL_GMALL_BASE)
    cur.execute(DDL_GMALL_BUSINESS)
    print("[DDL] 数据库 gmall_base / gmall_business 已创建")
    for db, ddl in DDL_TABLES:
        table_name = ddl.split('`')[3] if '`' in ddl else 'unknown'
        cur.execute(ddl)
        print(f"  [DDL] {db}.{table_name} 就绪")
    conn.commit()
    cur.close()

def batch_insert(conn, full_table, columns, rows, batch_size=500):
    """批量插入 full_table 格式: gmall_base.table_name"""
    if not rows:
        return 0
    cur = conn.cursor()
    placeholders = ','.join(['%s'] * len(columns))
    col_str = ','.join(f'`{c}`' for c in columns)
    sql = f"INSERT INTO {full_table} ({col_str}) VALUES ({placeholders})"
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        batch_data = [tuple(row.get(c) for c in columns) for row in batch]
        cur.executemany(sql, batch_data)
        conn.commit()
        total += len(batch)
        print(f"  [INSERT] {full_table}: {total}/{len(rows)}", end='\r')
    print(f"  [INSERT] {full_table}: 完成 {total} 行{'':30}")
    cur.close()
    return total


# ============================================================
#  七、预览
# ============================================================

def preview(regions, brands, categories, coupons, users, spus, skus,
            orders, order_details, status_logs, payments, refunds, coupon_uses, carts):
    print("\n" + "=" * 100)
    print("数据预览")
    print("=" * 100)

    print("\n--- gmall_base.base_region (地区表) ---")
    for r in regions[:5]:
        print(f"  ID={r['id']}  {r['region_name']}(L{r['level']})  大区={r['big_region']}  parent={r['parent_id']}")

    print("\n--- gmall_base.base_brand (品牌表) ---")
    for b in brands[:5]:
        print(f"  ID={b['id']}  {b['brand_name']}")

    print("\n--- gmall_base.base_category (分类表, 三级) ---")
    cat3 = [c for c in categories if c['level'] == 3]
    for c in cat3[:5]:
        print(f"  ID={c['id']}  {c['category_name']}(L{c['level']})  parent={c['parent_id']}")

    print("\n--- gmall_base.coupon_info (优惠券模板) ---")
    for c in coupons[:5]:
        print(f"  ID={c['id']}  {c['coupon_name']}  类型={c['coupon_type']}  满{c['full_amount']}减{c['reduce_amount']}")

    print("\n--- gmall_base.user_info (用户表) ---")
    for u in users[:5]:
        print(f"  ID={u['id']}  {u['name']}({u['gender']},{u['age_range']})  手机={u['phone_num']}  Lv{u['user_level']}  状态={u['status']}")

    print("\n--- gmall_base.spu_info / sku_info (商品) ---")
    for s in spus[:5]:
        print(f"  SPU ID={s['id']}  {s['spu_name']}  品牌ID={s['brand_id']}  分类ID={s['category3_id']}")
    for s in skus[:5]:
        print(f"  SKU ID={s['id']}  {s['sku_name']}  价格={s['price']}  上架={s['is_sale']}  规格={s['sku_attr']}")

    print("\n--- gmall_business.order_info (订单主表) ---")
    for o in orders[:5]:
        print(f"  ID={o['id']}  {o['out_trade_no']}  用户={o['user_id']}  金额={o['total_amount']}(原{'' if not o.get('original_total_amount') else o['original_total_amount']})  "
              f"状态={ORDER_STATUS_NAMES.get(o['order_status'], o['order_status'])}  省={o['province_id']}  优惠={o['coupon_reduce_amount']}")

    print("\n--- gmall_business.order_detail (订单明细) ---")
    for d in order_details[:5]:
        print(f"  订单={d['order_id']}  行{d['order_line_no']}  SKU={d['sku_id']}  {d.get('sku_name','')[:30]}  "
              f"单价={d['order_price']}  数量={d['sku_num']}  分摊后={d.get('split_total_amount')}")

    print("\n--- gmall_business.order_status_log (状态履历) ---")
    for s in status_logs[:8]:
        print(f"  订单={s['order_id']}  状态={ORDER_STATUS_NAMES.get(s['order_status'], s['order_status'])}  时间={s['create_time']}")

    print("\n--- gmall_business.payment_info (支付表) ---")
    for p in payments[:5]:
        print(f"  订单={p['order_id']}  流水={p['trade_no']}  金额={p['total_amount']}  "
              f"方式={PAYMENT_WAY_NAMES.get(p['payment_type'], p['payment_type'])}  状态={p['payment_status']}")

    print("\n--- gmall_business.refund_info (退款表) ---")
    for r in refunds[:5]:
        print(f"  订单={r['order_id']}  金额={r['refund_amount']}  状态={r['refund_status']}  原因={r['refund_reason']}")

    print("\n--- gmall_business.coupon_use (优惠券领用) ---")
    for c in coupon_uses[:5]:
        print(f"  券ID={c['coupon_id']}  用户={c['user_id']}  订单={c.get('order_id')}  状态={c['coupon_status']}  抵扣={c.get('coupon_reduce_amount')}")

    print("\n--- gmall_business.cart_info (购物车) ---")
    for c in carts[:5]:
        print(f"  用户={c['user_id']}  SKU={c['sku_id']}  数量={c['sku_num']}  已下单={c.get('order_id')}  勾选={c['is_checked']}")

    print("\n" + "=" * 100)
    print(f"数据汇总: 地区{len(regions)} 品牌{len(brands)} 分类{len(categories)} 优惠券模板{len(coupons)} "
          f"用户{len(users)} SPU{len(spus)} SKU{len(skus)} 订单{len(orders)} 明细{len(order_details)} "
          f"状态履历{len(status_logs)} 支付{len(payments)} 退款{len(refunds)} 优惠券领用{len(coupon_uses)} 购物车{len(carts)}")
    print("=" * 100)


# ============================================================
#  八、主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='gmall 实时数仓 MySQL 全量表模拟数据生成器')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('-P', '--port', type=int, default=3306)
    parser.add_argument('-u', '--user', default='root')
    parser.add_argument('-p', '--password', default='')
    parser.add_argument('--users', type=int, default=3000, help='用户数量')
    parser.add_argument('--spus', type=int, default=200, help='SPU数量')
    parser.add_argument('--orders', type=int, default=15000, help='订单数量')
    parser.add_argument('--coupons', type=int, default=10000, help='独立优惠券领用数量')
    parser.add_argument('--carts', type=int, default=8000, help='购物车数量')
    parser.add_argument('--seed', type=int, default=None)
    parser.add_argument('--dry-run', action='store_true', help='仅预览不写库')
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)
        if HAS_NUMPY:
            np.random.seed(args.seed)
        print(f"[INFO] 随机种子={args.seed}, 数据可复现\n")

    print(f"[INFO] 开始生成数据: 用户{args.users} SPU{args.spus} 订单{args.orders} 优惠券领用{args.coupons} 购物车{args.carts}")
    if args.dry_run:
        print("[INFO] dry-run 模式: 仅预览\n")

    # 1. 维度表 (固定数据)
    print("[1/8] 生成维度表: base_region ...")
    regions = gen_region_data()
    print(f"  完成: {len(regions)} 条 (省{sum(1 for r in regions if r['level']==1)} 市{sum(1 for r in regions if r['level']==2)} 区{sum(1 for r in regions if r['level']==3)})")

    print("[2/8] 生成维度表: base_brand ...")
    brands = gen_brand_data()
    print(f"  完成: {len(brands)} 个品牌")

    print("[3/8] 生成维度表: base_category ...")
    categories = gen_category_data()
    print(f"  完成: {len(categories)} 条 (一级{sum(1 for c in categories if c['level']==1)} 二级{sum(1 for c in categories if c['level']==2)} 三级{sum(1 for c in categories if c['level']==3)})")

    print("[4/8] 生成维度表: coupon_info ...")
    coupons = gen_coupon_info_data()
    print(f"  完成: {len(coupons)} 个优惠券模板")

    # 2. 用户
    print("[5/8] 生成维度表: user_info ...")
    users = gen_user_info_data(args.users, regions)
    print(f"  完成: {len(users)} 个用户")

    # 3. 商品 SPU/SKU
    print("[6/8] 生成维度表: spu_info / sku_info ...")
    spus, skus = gen_spu_sku_data(brands, categories, args.spus)
    print(f"  完成: {len(spus)} 个SPU, {len(skus)} 个SKU")

    # 4. 订单链路
    print("[7/8] 生成业务表: 订单链路 (order+detail+status_log+payment+refund+coupon_use) ...")
    user_ids = [u['id'] for u in users]
    province_ids = [r['id'] for r in regions if r['level'] == 1]
    orders, order_details, status_logs, payments, refunds, coupon_uses_from_orders = [], [], [], [], [], []
    detail_id_counter = 1
    status_log_id_counter = 1
    for oid in range(1, args.orders + 1):
        chain = gen_order_chain(oid, user_ids, skus, province_ids, coupons)
        order = chain['order']
        orders.append(order)
        # 明细
        for d in chain['details']:
            d['id'] = detail_id_counter
            d['order_id'] = oid
            d['create_time'] = order['create_time']
            d['coupon_id'] = chain['coupon_use']['coupon_id'] if chain['coupon_use'] else None
            order_details.append(d)
            detail_id_counter += 1
        # 状态履历
        for sl in chain['status_logs']:
            sl['id'] = status_log_id_counter
            sl['order_id'] = oid
            status_logs.append(sl)
            status_log_id_counter += 1
        # 支付
        if chain['payment']:
            chain['payment']['id'] = len(payments) + 1
            payments.append(chain['payment'])
        # 退款
        if chain['refund']:
            # 找对应的明细ID
            refund_sku_name = chain['refund']['sku_name']
            matching_detail = next((d for d in chain['details'] if d['sku_name'] == refund_sku_name), None)
            if matching_detail:
                chain['refund']['order_detail_id'] = matching_detail['id']
            else:
                chain['refund']['order_detail_id'] = chain['details'][0]['id']
            chain['refund']['id'] = len(refunds) + 1
            refunds.append(chain['refund'])
        # 优惠券领用(来自订单)
        if chain['coupon_use']:
            chain['coupon_use']['id'] = len(coupon_uses_from_orders) + 1
            coupon_uses_from_orders.append(chain['coupon_use'])
    print(f"  完成: 订单{len(orders)} 明细{len(order_details)} 状态履历{len(status_logs)} 支付{len(payments)} 退款{len(refunds)} 订单优惠券{len(coupon_uses_from_orders)}")

    # 5. 独立优惠券领用 + 购物车
    print("[8/8] 生成业务表: coupon_use(独立领用) + cart_info ...")
    coupon_uses_standalone = gen_coupon_use_data(coupons, user_ids, args.coupons)
    all_coupon_uses = coupon_uses_from_orders + coupon_uses_standalone
    order_ids = [o['id'] for o in orders]
    carts = gen_cart_info_data(user_ids, skus, args.carts, order_ids)
    print(f"  完成: 优惠券领用{len(all_coupon_uses)}(订单{len(coupon_uses_from_orders)}+独立{len(coupon_uses_standalone)}) 购物车{len(carts)}")

    # 预览
    preview(regions, brands, categories, coupons, users, spus, skus,
            orders, order_details, status_logs, payments, refunds, all_coupon_uses, carts)

    if args.dry_run:
        print("\n[INFO] dry-run 结束, 未写库。去掉 --dry-run 即可写入 MySQL。")
        return

    # 写入 MySQL
    print("\n[写入 MySQL] ...")
    conn = connect_mysql(args.host, args.port, args.user, args.password)
    execute_ddl(conn)

    # 维度表
    batch_insert(conn, '`gmall_base`.`base_region`',
                 ['id', 'region_code', 'region_name', 'level', 'parent_id', 'big_region', 'create_time'], regions)
    batch_insert(conn, '`gmall_base`.`base_brand`',
                 ['id', 'brand_name', 'logo_url', 'create_time'], brands)
    batch_insert(conn, '`gmall_base`.`base_category`',
                 ['id', 'category_name', 'level', 'parent_id', 'create_time'], categories)
    batch_insert(conn, '`gmall_base`.`coupon_info`',
                 ['id', 'coupon_type', 'full_amount', 'reduce_amount', 'coupon_name', 'use_condition', 'create_time'], coupons)
    batch_insert(conn, '`gmall_base`.`user_info`',
                 ['id', 'login_name', 'nick_name', 'name', 'phone_num', 'email', 'user_level', 'birthday', 'gender', 'age_range', 'status', 'create_time'], users)
    batch_insert(conn, '`gmall_base`.`spu_info`',
                 ['id', 'spu_name', 'description', 'category3_id', 'brand_id', 'create_time'], spus)
    batch_insert(conn, '`gmall_base`.`sku_info`',
                 ['id', 'sku_name', 'spu_id', 'category3_id', 'brand_id', 'price', 'weight', 'img_url', 'is_sale', 'sku_attr', 'create_time'], skus)

    # 业务表
    batch_insert(conn, '`gmall_business`.`order_info`',
                 ['id', 'consignee', 'consignee_tel', 'total_amount', 'order_status', 'user_id', 'payment_way',
                  'delivery_address', 'order_comment', 'out_trade_no', 'trade_body', 'create_time', 'operate_time',
                  'receive_time', 'expire_time', 'province_id', 'coupon_reduce_amount', 'original_total_amount'], orders)
    batch_insert(conn, '`gmall_business`.`order_detail`',
                 ['id', 'order_id', 'order_line_no', 'sku_id', 'sku_name', 'img_url', 'order_price', 'sku_num',
                  'create_time', 'source_type', 'source_id', 'split_activity_amount', 'coupon_id',
                  'split_coupon_amount', 'split_total_amount'], order_details)
    batch_insert(conn, '`gmall_business`.`order_status_log`',
                 ['id', 'order_id', 'order_status', 'create_time'], status_logs)
    batch_insert(conn, '`gmall_business`.`payment_info`',
                 ['id', 'out_trade_no', 'order_id', 'user_id', 'payment_type', 'trade_no', 'total_amount',
                  'payment_status', 'create_time', 'callback_time', 'callback_content'], payments)
    batch_insert(conn, '`gmall_business`.`refund_info`',
                 ['id', 'user_id', 'order_id', 'order_detail_id', 'sku_name', 'refund_amount', 'refund_num',
                  'refund_status', 'refund_type', 'refund_reason', 'refund_reason_type', 'create_time',
                  'refund_time', 'operate_time'], refunds)
    batch_insert(conn, '`gmall_business`.`coupon_use`',
                 ['id', 'coupon_id', 'coupon_type', 'user_id', 'order_id', 'coupon_status', 'coupon_reduce_amount',
                  'get_time', 'lock_time', 'using_time', 'used_time', 'expire_time'], all_coupon_uses)
    batch_insert(conn, '`gmall_business`.`cart_info`',
                 ['id', 'user_id', 'sku_id', 'sku_name', 'category_id', 'cart_price', 'sku_num', 'img_url',
                  'sku_attr', 'order_id', 'is_checked', 'create_time', 'operate_time'], carts)

    conn.close()
    total_rows = len(regions) + len(brands) + len(categories) + len(coupons) + len(users) + len(spus) + len(skus) + \
                 len(orders) + len(order_details) + len(status_logs) + len(payments) + len(refunds) + len(all_coupon_uses) + len(carts)
    print(f"\n[DONE] 全部完成! 共写入 {total_rows} 行数据到 gmall_base + gmall_business")


if __name__ == '__main__':
    main()
