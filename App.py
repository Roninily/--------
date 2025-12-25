



from urllib.parse import quote
import json
import os
from transCoordinateSystem import gcj02_to_wgs84, gcj02_to_bd09
import area_boundary as  area_boundary
import city_grid as city_grid
import time
import collections
import pandas as pd
from requests.adapters import HTTPAdapter
import requests

#from shp import trans_point_to_shp




#################################################需要修改###########################################################

## TODO 1.划分的网格距离，0.02-0.05最佳，建议如果是数量比较多的用0.01或0.02，如餐厅，企业。数据量少的用0.05或者更大，如大学
pology_split_distance = 0.2

## TODO 2. 城市编码，参见高德城市编码表，注意需要用adcode列的编码
# city_code = '440607'
city_code = '610124'

## TODO 3. POI类型编码，类型名或者编码都行，具体参见《高德地图POI分类编码表.xlsx》
typs = ['141202']
#['购物服务','餐饮服务','公司企业','金融保险服务','公司企业','金融保险服务' ,'交通设施服务','购物服务'
#typs = ['购物服务', '生活服务', '餐饮服务', '公司企业', '地名地址信息']

# ,'科教文化服务','交通设施服务','商务住宅', '政府机构及社会团体', '通行设施', '医疗保健服务',  '住宿服务'

# , '风景名胜', '公共设施', '金融保险服务',   '汽车服务', '汽车维修', '室内设施', '体育休闲服务'

# '汽车销售', '摩托车服务','道路附属设施',  '事件活动',

## TODO 4. 高德开放平台密钥
gaode_key = ['9aefde292eb2bfb60fa9cae621c7b3c9']



# TODO 5.输出数据坐标系,1为高德GCJ20坐标系，2WGS84坐标系，3百度BD09坐标系
coord = 2

############################################以下不需要动#######################################################################


poi_pology_search_url = 'https://restapi.amap.com/v3/place/polygon'

buffer_keys = collections.deque(maxlen=len(gaode_key))


# 定义需要切换密钥的错误码列表
error_codes_to_switch = [
    '10001',  # 密钥无效或过期
    '10003',  # 日访问量超限
    '10007',  # 数字签名错误
    '10010',  # IP访问超限
    '10026',  # 账号被封禁
    '10029',  # Key QPS超限
    '10044',  # 账号日调用量超限
    '10019',  # 服务总QPS超限
    '10020',  # 接口QPS超限
    '10021'   # 账号接口QPS超限
]

def init_queen():
    buffer_keys.clear()
    for key in gaode_key:
        buffer_keys.append(key)
    print('当前可供使用的高德密钥：', buffer_keys)

# 根据城市名称和分类关键字获取poi数据
def getpois(grids, keywords):
    if not buffer_keys:
        print('密钥已经用尽，程序退出！')
        return []
    poilist = []
    page = 1
    while True:
        time.sleep(0.4)
        if not buffer_keys:
            print('所有密钥均已耗尽，无法继续请求。')
            break
        current_key = buffer_keys[0]
        print(f'当前使用密钥：{current_key}，正在获取第{page}页')
        try:
            result = getpoi_page(grids, keywords, page, current_key)
            data = json.loads(result)
            
            # 处理API返回的错误码
            if data['infocode'] == '10000':
                if data.get('count', '0') == '0':
                    break
                hand(poilist, data)
                page += 1  # 成功获取后翻页
            else:
                if data['infocode'] in error_codes_to_switch:
                    print(f'密钥 {current_key} 失效，错误码：{data["infocode"]}，信息：{data.get("info", "")}')
                    buffer_keys.remove(current_key)
                    print(f'剩余可用密钥：{buffer_keys}')
                    # 保持page不变，继续重试当前页
                else:
                    print(f'遇到不可处理错误，终止请求。错误码：{data["infocode"]}，信息：{data.get("info", "")}')
                    break
        except requests.exceptions.RequestException as e:
            print(f'网络请求异常：{e}，5秒后重试...')
            time.sleep(5)
        except Exception as e:
            print(f'发生未知异常：{e}')
            break
    return poilist
# 数据写入csv文件中
def write_to_csv(poilist, citycode, classfield, coord):
    data_csv = {}
    lons, lats, names, addresss, pnames, citynames,adnames, business_areas, tels, types, typecodes, ids, type_1s, type_2s, type_3s, type_4s = [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []

    if len(poilist) == 0:
        print("处理完成，当前citycode:" + str(citycode), ", classfield为：", str(classfield) + "，数据为空，，，结束.......")
        return None, None

    for i in range(len(poilist)):
       
        location = poilist[i].get('location')
        name = poilist[i].get('name')
        address = poilist[i].get('address')
        pname = poilist[i].get('pname')
        cityname = poilist[i].get('cityname')
        adname = poilist[i].get('adname')
        business_area = poilist[i].get('business_area')
        tel = poilist[i].get('tel')
        type = poilist[i].get('type')
        typecode = poilist[i].get('typecode')
        lng = str(location).split(",")[0]
        lat = str(location).split(",")[1]
        id = poilist[i].get('id')

        if (coord == 2):
            result = gcj02_to_wgs84(float(lng), float(lat))
            lng = result[0]
            lat = result[1]
        if (coord == 3):
            result = gcj02_to_bd09(float(lng), float(lat))
            lng = result[0]
            lat = result[1]
        type_1, type_2, type_3, type_4 = '','','',''
        if str(type) != None and str(type) != '':
            type_strs = type.split(';')
            for i in range(len(type_strs)):
                ty = type_strs[i]
                if i == 0:
                    type_1 = ty
                elif i == 1:
                    type_2 = ty
                elif i == 2:
                    type_3 = ty
                elif i == 3:
                    type_4 = ty

        lons.append(lng)
        lats.append(lat)
        names.append(name)
        addresss.append(address)
        pnames.append(pname)
        citynames.append(cityname)
        adnames.append(adname)
        if business_area == []:
            business_area = ''
        business_areas.append(business_area)
        tels.append(tel)
        types.append(type)
        typecodes.append(typecode)
        ids.append(id)
        type_1s.append(type_1)
        type_2s.append(type_2)
        type_3s.append(type_3)
        type_4s.append(type_4)
    data_csv['lon'], data_csv['lat'], data_csv['name'], data_csv['address'], data_csv['pname'], \
    data_csv['cityname'], data_csv['adname'], data_csv['business_area'], data_csv['tel'], data_csv['type'], data_csv['typecode'], data_csv['id'], data_csv[
        'type1'], data_csv['type2'], data_csv['type3'], data_csv['type4'] = \
        lons, lats, names, addresss, pnames, citynames, adnames, business_areas, tels, types, typecodes, ids, type_1s, type_2s, type_3s, type_4s
    df = pd.DataFrame(data_csv)

    file_name = 'POI_' + citycode + "-" + classfield + ".csv"
    df.to_csv(r'data' + 'POI_' + citycode + "_" + classfield + ".csv" , index=False, encoding='utf_8_sig')

    # folder_name = 'poi-' + citycode + "-" + classfield
    # folder_name_full = 'data' + os.sep + folder_name + os.sep
    # if os.path.exists(folder_name_full) is False:
    #    os.makedirs(folder_name_full)
    # file_name = 'poi-' + citycode + "-" + classfield + ".csv"
    # file_path = folder_name_full + file_name
    # df.to_csv(file_path, index=False, encoding='utf_8_sig')
    # print('写入成功')
    # return folder_name_full, file_name


# 将返回的poi数据装入集合返回
def hand(poilist, result):
    # result = json.loads(result)  # 将字符串转换为json
    pois = result['pois']
    for i in range(len(pois)):
        poilist.append(pois[i])


# 单页获取pois
def getpoi_page(grids, types, page, key):
    polygon = f"{grids[0]},{grids[1]}|{grids[2]},{grids[3]}"
    url = f"{poi_pology_search_url}?key={key}&extensions=all&types={quote(types)}&polygon={polygon}&offset=25&page={page}&output=json"
    
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=3))
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f'请求失败：{str(e)}')
        raise  # 重新抛出异常供上层处理
    
def get_drids(min_lng, max_lat, max_lng, min_lat, keyword, key, pology_split_distance, all_grids):
    grids_lib = city_grid.generate_grids(min_lng, max_lat, max_lng, min_lat, pology_split_distance)

    print('划分后的网格数：', len(grids_lib))
    print(grids_lib)

    # 3. 根据生成的网格爬取数据，验证网格大小是否合适，如果不合适的话，需要继续切分网格
    for grid in grids_lib:
        one_pology_data = getpoi_page(grid, keyword, 1, key)
        data = json.loads(one_pology_data)
        print(data)

        while int(data['count']) > 890:
            get_drids(grid[0], grid[1], grid[2], grid[3], keyword, key, pology_split_distance / 2, all_grids)


        all_grids.append(grid)
    return all_grids


def get_data(city, keyword, coord):
    # 1. 获取城市边界的最大、最小经纬度
    amap_key = buffer_keys[0]  # 总是获取队列中的第一个密钥
    max_lng, min_lng, max_lat, min_lat = area_boundary.getlnglat(city, amap_key)

    print('当前城市：', city, "max_lng, min_lng, max_lat, min_lat：", max_lng, min_lng, max_lat, min_lat)

    # 2. 生成网格切片格式：

    grids_lib = city_grid.generate_grids(min_lng, max_lat, max_lng, min_lat, pology_split_distance)

    print('划分后的网格数：', len(grids_lib))
    print(grids_lib)

    all_data = []
    begin_time = time.time()

    print('============正式开始爬取啦！！！==============')

    for grid in grids_lib:
        # grid格式：[112.23, 23.23, 112.24, 23.22]
        try:
          one_pology_data = getpois(grid, keyword)
        except:
          print("============QPS限额, 即将返回CSV文件=============")
        print('============当前矩形范围：', grid, '总共：',
              str(len(one_pology_data)) + "条数据...............")

        all_data.extend(one_pology_data)

    end_time = time.time()
    print('全部：', str(len(grids_lib)) + '个矩形范围', '总的', str(len(all_data)), '条数据, 耗时：', str(end_time - begin_time),
          '正在写入CSV文件中')
    write_to_csv(all_data, city, keyword, coord)
    #file_folder, file_name = write_to_csv(all_data, city, keyword, coord)
    # 写入shp
    #if file_folder is not None:
        #trans_point_to_shp(file_folder, file_name, 0, 1, pology_split_distance, keyword)


if __name__ == '__main__':
    # 初始化密钥队列
    buffer_keys = collections.deque(maxlen=len(gaode_key))
    init_queen()

    for typ in typs:
        print(f'====== 开始爬取【{typ}】类型数据 ======')
        get_data(city_code, typ, coord)