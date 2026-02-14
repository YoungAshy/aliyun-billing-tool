import time
from alibabacloud_bssopenapi20171214.client import Client as BssOpenApiClient
from alibabacloud_bssopenapi20171214 import models as bss_models
from alibabacloud_tea_openapi import models as open_api_models
import json
import re
import pyodbc
import sys

#获取客户端
def create_client(access_key_id, access_key_secret):
    """创建阿里云客户端"""
    config = open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        endpoint='business.aliyuncs.com'
    )
    return BssOpenApiClient(config)

#获取财务子账号
def get_financial_member_accounts(client, master_account_id):
    """
    获取主账号下所有财务成员账号（MEMBER）
    
    Args:
        bss_client: BSS OpenAPI 客户端
        master_account_id: 主账号 UID (如 "1219436465239380")
    
    Returns:
        List[str]: 成员账号 AccountId 列表
    """
    member_accounts = []
    page_num = 1
    page_size = 20
    
    while True:
        request = bss_models.QueryRelationListRequest(
            user_id=int(master_account_id),  # 注意：这里是 integer 类型！
            page_num=page_num,
            page_size=page_size,
            status_list=["RELATED"]  # 只查询已生效的关系
        )
        
        try:
            response = client.query_relation_list(request)
            
            # 检查响应
            if (not hasattr(response.body, 'data') or 
                response.body.data is None or
                not hasattr(response.body.data, 'financial_relation_info_list')):
                break
            
            relations = response.body.data.financial_relation_info_list
            if not relations:
                break
            
            # 提取 MEMBER 账号的 AccountId
            for relation in relations:
                if (hasattr(relation, 'account_type') and 
                    relation.account_type == "MEMBER" and
                    hasattr(relation, 'account_id')):
                    member_accounts.append(str(relation.account_id))
            
            # 检查是否还有更多页面
            total_count = getattr(response.body.data, 'total_count', 0)
            if page_num * page_size >= total_count:
                break
                
            page_num += 1
            
        except Exception as e:
            print(f"❌ 获取财务关系失败 (页 {page_num}): {e}")
            break
    member_accounts=[str(master_account_id)] + member_accounts
    print(f"✅ 获取到 {len(member_accounts)} 个财务成员账号")
    return member_accounts

#调用api获取账单
def get_all_bill_data_by_DescribeInstanceBill(client, billing_cycle,bill_owner_id=None,product_code=None,max_results=100):
    all_items = []
    page_count = 0
    next_token=None
    count=0
    for member in bill_owner_id:
        count+=1
        print(f"📥 正在获取第 {count} 个子账号的数据...")
        while True:
            page_count += 1
            print(f"📥 正在获取第 {page_count} 页数据...")
            try:
                # 构建请求
                request = bss_models.DescribeInstanceBillRequest(
                    billing_cycle=billing_cycle,
                    product_code=product_code,
                    max_results=max_results,
                    bill_owner_id=member,
                    is_billing_item=True,
                    
                )
                
                if next_token:
                    request.next_token = next_token
                
                # 发送请求
                response = client.describe_instance_bill(request)
                # print(response)
                # 检查响应是否成功
                if (not hasattr(response.body, 'data') or 
                    response.body.data is None or
                    not hasattr(response.body.data, 'items') or
                    response.body.data.items is None):
                    print("⚠️ 响应数据为空")
                    break
                
                # 提取当前页的数据
                current_items = extract_items_with_to_map_by_DescribeInstanceBill_N(response)
                all_items.extend(current_items)

                print(f"   ✅ 获取 {len(current_items)} 条记录，累计 {len(all_items)} 条")

                # 获取下一个 token
                next_token = None
                if (hasattr(response.body, 'data') and 
                    response.body.data is not None and
                    hasattr(response.body.data, 'next_token')):
                    next_token = response.body.data.next_token
                
                # 如果没有 next_token，说明已经到最后一页
                if not next_token or str(next_token).strip() == '':
                    print("✅ 所有数据获取完成！")
                    break
                    
                # 避免 API 限流
                time.sleep(0.05)
                
            except Exception as e:
                print(f"❌ 第 {page_count} 页获取失败: {e}")
                break
    
    return all_items

#在账单中转换字符串并清理空格保存数量为0的金额信息            
def extract_items_with_to_map_by_DescribeInstanceBill_N(response):
    """使用 to_map() 方法转换 item，并为每条记录添加账户信息"""
    items_list = []
    #清理数据中的空格并转化为字符类型
    def clean_string(value):
        """清除字符串中的所有空白字符（包括空格、制表符、换行、全角空格等）"""
        if value is None:
            return ''
        if isinstance(value, str):
            # 使用正则表达式移除所有空白字符
            return re.sub(r'\s+', '', value)
        # 对非字符串类型：先转为字符串，再清除空白（虽然通常不会有空白）
        return re.sub(r'\s+', '', str(value))

    # 金额信息（不需要清理空格，但要安全转换）
    def safe_float(value):
        if value is None or str(value).strip() == '':
            return 0.0
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return 0.0



    # ✅ 直接处理 response.body.data（它是一个对象，不是列表）
    data = response.body.data
    if data is None:
        return items_list
    
    # ✅ 转换为字典
    data_dict = data.to_map()
    
    # ✅ 获取公共信息（来自 Data 层）
    account_period = data_dict.get('BillingCycle', '')
    account_id = data_dict.get('AccountID', '')
    
    # ✅ 遍历所有账单项
    for item_dict in data_dict.get('Items', []):
        # ✅ 为每条记录创建一个新的字典
        record = {}
        
    # ✅ 提取并清理字段
        record['AccountPeriod'] = account_period
        record['AccountID'] = account_id  # 或 AccountID
        record['Account'] = clean_string(item_dict.get('BillAccountName', ''))
        
        # 产品信息
        record['ProductCode'] = clean_string(item_dict.get('PipCode', ''))
        record['Product'] = clean_string(item_dict.get('ProductName', ''))
        record['ProductDetailCode'] = clean_string(item_dict.get('CommodityCode', ''))
        record['ProductDetail'] = clean_string(item_dict.get('ProductDetail', ''))
        record['ProductType'] = clean_string(item_dict.get('ProductType', ''))
        
        
        # 实例信息
        record['InstanceId'] = clean_string(item_dict.get('InstanceID', ''))
        record['InstanceName'] = clean_string(item_dict.get('NickName', ''))
        record['Region'] = clean_string(item_dict.get('Region', ''))
        record['Zone'] = clean_string(item_dict.get('Zone', ''))
        record['BillingItem'] = clean_string(item_dict.get('BillingItem', ''))
        
        record['Amount'] = safe_float(item_dict.get('PretaxAmount', 0))
        record['AfterDiscountAmount'] = safe_float(item_dict.get('AfterDiscountAmount', 0))
        record['InvoiceDiscount'] = safe_float(item_dict.get('InvoiceDiscount', 0))
        record['DeductedByCoupons'] = safe_float(item_dict.get('DeductedByCoupons', 0))
      
        # 其他信息
        record['SubscriptionType'] = clean_string(item_dict.get('SubscriptionType', ''))
        record['Usage'] = clean_string(item_dict.get('Usage', ''))
        record['UsageUnit'] = clean_string(item_dict.get('UsageUnit', ''))
        #将资源组保存在tag键为财务单元的值中
        tag_str = item_dict.get('Tag', {})
        # 使用正则匹配所有 key:value 对
        pattern = r'key:([^;]+?)\s+value:([^;]*?)(?=;\s*key:|$)'
        matches = re.findall(pattern, tag_str)
        # 转为字典
        tag_dict = {}
        for k, v in matches:
            tag_dict[k.strip()] = v.strip()
        record['Tag'] = clean_string(tag_dict.get('财务单元', ''))    
        # 添加到结果列表
        items_list.append(record)

    
    return items_list

def save_to_sql_server(items_list, db_config,table_name):
    """
    将账单数据保存到 SQL Server
    
    Args:
        items_list: List[dict] 账单条目列表
        db_config: dict 数据库连接配置
        billing_cycle: str 账单周期 "YYYY-MM"
    """
    #安全转换字符串
    def safe_decimal(value):
        """安全地将值转为 float（用于 DECIMAL 字段）"""
        if value is None or value == '' or str(value).lower() in ('null', 'none'):
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    if not items_list:
        print("⚠️ 无数据需要保存")
        return
    
    # 表名
    table_name = table_name#"N_Testtconsume"
    
    # ✅ 修正：字段数量与你的 record 结构完全一致
    columns = [
        'AccountPeriod', 'AccountID', 'Account',
        'ProductCode', 'Product','ProductDetailCode', 'ProductDetail',
        'ProductType',
        'InstanceId', 'InstanceName',
        'Region', 'Zone', 'BillingItem',
        'Amount', 'AfterDiscountAmount', 'InvoiceDiscount', 'DeductedByCoupons',
        'SubscriptionType', 'Usage', 'UsageUnit', 'Tag'
    ]
    
    # ✅ 构建 INSERT SQL（字段数量与 values 一一对应）
    placeholders = ', '.join(['?'] * len(columns))
    print(placeholders)
    sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
    """
    print(sql)
    
    # ✅ 准备数据元组列表（字段顺序必须与 columns 一致）
    data_to_insert = []
    for item in items_list:
        row = (
            item.get('AccountPeriod', ''),          # 1
            item.get('AccountID', ''),              # 2
            item.get('Account', ''),                # 3
            item.get('ProductCode', ''),            # 4
            item.get('Product', ''),                # 5
            item.get('ProductDetailCode', ''),      # 6
            item.get('ProductDetail', ''),          # 7
            item.get('ProductType', ''),            # 8
            item.get('InstanceId', ''),             # 9
            item.get('InstanceName', ''),           # 10
            item.get('Region', ''),                 # 11
            item.get('Zone', ''),                   # 12
            item.get('BillingItem', ''),            # 13
            safe_decimal(item.get('Amount')),       # 14
            safe_decimal(item.get('AfterDiscountAmount')),  # 15
            safe_decimal(item.get('InvoiceDiscount')),      # 16
            safe_decimal(item.get('DeductedByCoupons')),    # 17
            item.get('SubscriptionType', ''),       # 18
            item.get('Usage', ''),                  # 19
            item.get('UsageUnit', ''),              # 20
            item.get('Tag', '')                     # 21
        )
        data_to_insert.append(row)
    
    # 连接数据库并插入
    conn = None
    try:
        # 建立连接
        conn_str = (
            f"DRIVER={{{db_config['driver']}}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"✅ 连接成功! SQL Server 版本: {version[:50]}...")
        
        # # ✅ 创建表（如果不存在）- 字段数量与你的 record 完全一致
        # cursor.execute(f"""
        #     IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        #     CREATE TABLE {table_name} (
        #         id INT IDENTITY(1,1) PRIMARY KEY,
        #         AccountPeriod VARCHAR(10),
        #         AccountID VARCHAR(32),
        #         Account NVARCHAR(100),
        #         ProductCode NVARCHAR(50),
        #         ProductDetailCode NVARCHAR(50),
        #         ProductDetail NVARCHAR(100),
        #         ProductType NVARCHAR(100),
        #         ProductName NVARCHAR(100),
        #         InstanceId NVARCHAR(100),
        #         InstanceName NVARCHAR(255),
        #         Region NVARCHAR(100),
        #         Zone NVARCHAR(100),
        #         BillingItem NVARCHAR(100),
        #         Amount DECIMAL(18,6),
        #         AfterDiscountAmount DECIMAL(18,6),
        #         InvoiceDiscount DECIMAL(18,6),
        #         DeductedByCoupons DECIMAL(18,6),
        #         SubscriptionType NVARCHAR(20),
        #         Usage NVARCHAR(50),
        #         UsageUnit NVARCHAR(20),
        #         Tag NVARCHAR(MAX),
        #         created_at DATETIME2 DEFAULT GETDATE()
        #     )
        # """)
        
        # ✅ 批量插入（字段数量必须完全匹配！）
        cursor.executemany(sql, data_to_insert)
        conn.commit()
        
        print(f"✅ 成功保存 {len(data_to_insert)} 条记录到表 {table_name}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ 保存失败: {e}")
        print(f"    SQL: {sql}")
        print(f"    第一条数据: {data_to_insert[0] if data_to_insert else '无数据'}")
        raise
    finally:
        if conn:
            conn.close()

#从 config.json 读取所有阿里云账号
def get_all_aliyun_accounts():
    
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    accounts = []
    
    # 遍历所有 aliyun* 键
    for key, value in config.items():
        accounts.append({
            'account_name': key,  # aliyun, aliyun2, aliyun3
            'uid':value.get('uid',''),
            'ak_id': value.get('access_key_id', ''),
            'ak_secret': value.get('access_key_secret', '')
        })

    return accounts
#判断数据库表格中是否已经有该月份的数据
def check_billing_period_exists(db_config, billing_cycle,table_name):
    """
    检查指定账期是否已存在于数据库中
    :param db_config: 数据库配置字典
    :param billing_cycle: 账期，格式如 '2025-12'
    :return: True 如果已存在，False 如果不存在
    """
    try:
        # 构建连接字符串
        conn_str = (
            f"DRIVER={{{db_config['driver']}}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"Trusted_Connection=yes;"#windows身份验证
        )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # 表名（根据你的命名规则）
        table_name = table_name
        
        # 检查表是否存在
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ?
        """, table_name)
        
        if cursor.fetchone()[0] == 0:
            print(f"ℹ️ 表 {table_name} 不存在，请先创建表格")
            conn.close()
            return False
        
        # 检查表中是否有相同 AccountPeriod 的记录 AccountPeriod=billing_cycle
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM [{table_name}] 
            WHERE AccountPeriod = ?
        """, billing_cycle)
        
        count = cursor.fetchone()[0]
        conn.close()
        
        if count > 0:
            print(f"❌ 账期 {billing_cycle} 的当前月账单在数据库中已存在（共 {count} 条记录），程序终止！")
            return False
        else:
            print(f"✅ 账期 {billing_cycle} 的当前月账单在数据库中不存在，继续执行...")
            return True
            
    except Exception as e:
        print(f"⚠️ 数据库检查出错: {e}")
        # 出错时选择继续还是终止？这里建议继续（保守策略）
        return False
#输入账单月份，并判断获取的月份格式是否正确
def check_billing_cycle(billing_cycle):

    if not re.fullmatch(r'\d{4}-\d{2}', billing_cycle):
        print("格式有误")
        return False

    try:
        year, month = map(int, billing_cycle.split('-'))
        if not (1 <= month <= 12):
            print("月份有误")
            return False
        if not (1900 <= year <= 2100):
            print("年份有误")
            return False
    except ValueError:
        print("其他异常")
        return False
    print("输入月份合法")
    return True

def main():
    while True:
        billing_cycle=input("请输入所要查询的月份如（2026-01）：").strip()
        #判断程序输入月份是否合规，否则直接结束程序
        if check_billing_cycle(billing_cycle):
            break
    print("账期为："+billing_cycle)
            
    table_name="N_Testtconsume"
    db_config = {
                'driver': 'ODBC Driver 17 for SQL Server',   # 或 'ODBC Driver 18 for SQL Server'
                'server': '172.16.18.37',                    # SQL Server 实例地址
                'database': 'aliyun20241128'                 # 你要连接的数据库名
            }
    accounts=get_all_aliyun_accounts()
    #判断原数据库中是否有对应月份的账单
    if check_billing_period_exists(db_config,billing_cycle,table_name):
        #调用函数
        
        for account in accounts:
            print(F"正在调用账号{account['account_name']}")
            client = create_client(account['ak_id'], account['ak_secret'])
            member_accounts=get_financial_member_accounts(client, account['uid'])
            # member_accounts=["1038214233557653"]
            items_dict_list=get_all_bill_data_by_DescribeInstanceBill(client, billing_cycle,member_accounts)

            #print(items_dict_list[0:10])
            #保存到sqlserver
            save_to_sql_server(items_dict_list,db_config,table_name)
    else:
            print(f'程序已结束')
            return True
            
if __name__ == "__main__":
    main()